//! BGP neighbor-state subscription over gRPC (`zebra.bgp.v1`).
//!
//! `WatchNeighbors` streams the default instance's neighbors: a snapshot,
//! then an event as soon as a neighbor changes. Machine consumers (a CNI
//! agent reporting session health, monitoring) otherwise have to poll
//! `show bgp neighbor`, which delays what they report by the poll interval.
//!
//! The handler mints a per-subscriber channel and hands its sender to the
//! config manager ([`Message::BgpNeighborWatch`]), which forwards it to the
//! BGP task (or parks it until BGP starts). The BGP task owns the
//! subscribers in [`NeighborWatch`]: at the top of every event-loop
//! iteration it compares each neighbor with what it last delivered and
//! sends the differences. That single comparison point catches every way
//! a session changes — the FSM, accepted connections, collision handling,
//! timers and configuration — without hooking each one. It only runs while
//! someone is subscribed, and compares small `Copy` fields keyed by
//! [`PeerKey`], so it allocates nothing until a neighbor actually changes.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;

use super::peer::{Peer, PeerDownReason, State};
use super::peer_key::PeerKey;
use super::peer_map::PeerMap;
use crate::config::Message;

pub mod pb {
    tonic::include_proto!("zebra.bgp.v1");
}

pub use pb::bgp_service_server::BgpServiceServer;

/// Per-subscriber buffer of undelivered events. A session change is one
/// event, so this only fills when a client stops reading — the subscriber
/// is then dropped rather than buffered toward without bound.
pub const EVENT_BUFFER: usize = 64;

/// What a subscriber has been told about one neighbor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    state: State,
    remote_as: u32,
    ibgp: bool,
    last_reset: Option<PeerDownReason>,
}

impl Seen {
    fn of(peer: &Peer) -> Self {
        Seen {
            state: peer.state,
            remote_as: peer.remote_as,
            ibgp: peer.peer_type.is_ibgp(),
            last_reset: peer.last_reset.map(|(reason, _)| reason),
        }
    }
}

fn neighbor(peer: &Peer) -> pb::Neighbor {
    pb::Neighbor {
        address: peer.address.to_string(),
        interface: peer.ifname.clone().unwrap_or_default(),
        remote_as: peer.remote_as,
        peer_type: peer.peer_type.to_str().to_string(),
        state: peer.state.to_str().to_string(),
        last_reset: peer
            .last_reset
            .map(|(reason, _)| reason.as_str().to_string())
            .unwrap_or_default(),
    }
}

/// The BGP task's neighbor-state subscribers and what they were last sent.
#[derive(Debug, Default)]
pub struct NeighborWatch {
    subscribers: Vec<mpsc::Sender<pb::NeighborEvent>>,
    seen: HashMap<PeerKey, (Seen, pb::Neighbor)>,
}

impl NeighborWatch {
    /// Deliver changes since the last call to every subscriber.
    pub fn upkeep(&mut self, peers: &PeerMap) {
        if self.subscribers.is_empty() {
            self.seen.clear();
            return;
        }
        let event = self.diff(peers);
        if !event.neighbors.is_empty() || !event.removed.is_empty() {
            self.subscribers
                .retain(|tx| tx.try_send(event.clone()).is_ok());
        }
    }

    /// Register a subscriber: bring the others up to date, then send it a
    /// snapshot of the same state.
    pub fn add(&mut self, peers: &PeerMap, tx: mpsc::Sender<pb::NeighborEvent>) {
        self.upkeep(peers);
        if self.subscribers.is_empty() {
            self.diff(peers); // nobody was watching: seen is empty
        }
        let mut neighbors: Vec<_> = self.seen.values().map(|(_, n)| n.clone()).collect();
        neighbors.sort_by(|a, b| (&a.address, &a.interface).cmp(&(&b.address, &b.interface)));
        let snapshot = pb::NeighborEvent {
            snapshot: true,
            neighbors,
            removed: Vec::new(),
        };
        // A fresh channel only refuses the snapshot if the client is gone.
        if tx.try_send(snapshot).is_ok() {
            self.subscribers.push(tx);
        }
    }

    /// Update `seen` from the current neighbors and return what changed.
    fn diff(&mut self, peers: &PeerMap) -> pb::NeighborEvent {
        let mut event = pb::NeighborEvent::default();
        let mut present = 0;
        for (key, peer) in peers.iter_all() {
            present += 1;
            let now = Seen::of(peer);
            match self.seen.get(key) {
                Some((seen, _)) if *seen == now => {}
                _ => {
                    let n = neighbor(peer);
                    event.neighbors.push(n.clone());
                    self.seen.insert(*key, (now, n));
                }
            }
        }
        if present != self.seen.len() {
            let current: std::collections::HashSet<PeerKey> =
                peers.iter_all().map(|(k, _)| *k).collect();
            self.seen.retain(|key, (_, n)| {
                let keep = current.contains(key);
                if !keep {
                    event.removed.push(pb::Neighbor {
                        address: n.address.clone(),
                        interface: n.interface.clone(),
                        ..Default::default()
                    });
                }
                keep
            });
        }
        event
    }
}

#[derive(Debug)]
pub struct BgpWatchService {
    pub tx: mpsc::Sender<Message>,
}

#[tonic::async_trait]
impl pb::bgp_service_server::BgpService for BgpWatchService {
    type WatchNeighborsStream = ReceiverStream<Result<pb::NeighborEvent, tonic::Status>>;

    async fn watch_neighbors(
        &self,
        _request: tonic::Request<pb::WatchNeighborsRequest>,
    ) -> Result<Response<Self::WatchNeighborsStream>, tonic::Status> {
        let (bus_tx, mut bus_rx) = mpsc::channel::<pb::NeighborEvent>(EVENT_BUFFER);
        self.tx
            .send(Message::BgpNeighborWatch(bus_tx))
            .await
            .map_err(|_| tonic::Status::unavailable("config manager unavailable"))?;
        // Forward deliveries to the response stream; `closed()` drops the
        // bus as soon as the client disconnects, so the BGP task prunes
        // the subscriber on its next delivery.
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = bus_rx.recv() => match event {
                        Some(event) => {
                            if tx.send(Ok(event)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                    _ = tx.closed() => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_peer(addr: IpAddr) -> Peer {
        let (tx, _rx) = mpsc::channel(1);
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
        Box::leak(Box::new(inbound_rx));
        let rib = crate::rib::client::RibClient::new(
            inbound_tx,
            crate::rib::client::ProtoId::from_raw(0),
        );
        let ctx = crate::context::ProtoContext::default_table(rib);
        Peer::new(
            0,
            65000,
            Ipv4Addr::new(1, 1, 1, 1),
            65001,
            addr,
            None,
            tx,
            ctx,
        )
    }

    fn addr(last: u8) -> IpAddr {
        Ipv4Addr::new(192, 0, 2, last).into()
    }

    fn drain(rx: &mut mpsc::Receiver<pb::NeighborEvent>) -> Vec<pb::NeighborEvent> {
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    #[test]
    fn snapshot_then_only_changes() {
        let mut peers = PeerMap::new();
        peers.insert(addr(1), make_peer(addr(1)));
        peers.insert(addr(2), make_peer(addr(2)));
        let mut watch = NeighborWatch::default();
        let (tx, mut rx) = mpsc::channel(EVENT_BUFFER);
        watch.add(&peers, tx);
        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].snapshot);
        let snap: Vec<_> = events[0]
            .neighbors
            .iter()
            .map(|n| (n.address.as_str(), n.state.as_str()))
            .collect();
        assert_eq!(snap, [("192.0.2.1", "Idle"), ("192.0.2.2", "Idle")]);

        // Nothing changed: nothing is sent.
        watch.upkeep(&peers);
        assert!(drain(&mut rx).is_empty());

        // One session comes up, then goes down with a reason.
        peers.get_mut(&addr(1)).unwrap().state = State::Established;
        watch.upkeep(&peers);
        let p = peers.get_mut(&addr(1)).unwrap();
        p.state = State::Active;
        p.last_reset = Some((PeerDownReason::HoldTimerExpired, std::time::Instant::now()));
        watch.upkeep(&peers);
        let events = drain(&mut rx);
        let got: Vec<_> = events
            .iter()
            .map(|e| {
                assert!(!e.snapshot && e.removed.is_empty() && e.neighbors.len() == 1);
                let n = &e.neighbors[0];
                (
                    n.address.clone(),
                    n.state.clone(),
                    n.last_reset.clone(),
                    n.remote_as,
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                ("192.0.2.1".into(), "Established".into(), "".into(), 65001),
                (
                    "192.0.2.1".into(),
                    "Active".into(),
                    "Hold timer expired".into(),
                    65001
                ),
            ]
        );

        // Added and removed neighbors.
        peers.remove(&addr(2));
        peers.insert(addr(3), make_peer(addr(3)));
        watch.upkeep(&peers);
        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]
                .neighbors
                .iter()
                .map(|n| n.address.as_str())
                .collect::<Vec<_>>(),
            ["192.0.2.3"]
        );
        assert_eq!(
            events[0]
                .removed
                .iter()
                .map(|n| n.address.as_str())
                .collect::<Vec<_>>(),
            ["192.0.2.2"]
        );
    }

    #[test]
    fn late_subscriber_gets_current_state_and_gone_subscribers_are_dropped() {
        let mut peers = PeerMap::new();
        peers.insert(addr(1), make_peer(addr(1)));
        let mut watch = NeighborWatch::default();
        let (tx1, mut rx1) = mpsc::channel(EVENT_BUFFER);
        watch.add(&peers, tx1);
        drain(&mut rx1);
        peers.get_mut(&addr(1)).unwrap().state = State::Established;
        // The second subscriber's registration delivers the change to the
        // first, then a snapshot with the same state to the second.
        let (tx2, mut rx2) = mpsc::channel(EVENT_BUFFER);
        watch.add(&peers, tx2);
        assert_eq!(drain(&mut rx1)[0].neighbors[0].state, "Established");
        let snap = drain(&mut rx2);
        assert!(snap[0].snapshot && snap[0].neighbors[0].state == "Established");

        drop(rx1);
        peers.get_mut(&addr(1)).unwrap().state = State::Idle;
        watch.upkeep(&peers);
        assert_eq!(watch.subscribers.len(), 1);
        drop(rx2);
        peers.get_mut(&addr(1)).unwrap().state = State::Connect;
        watch.upkeep(&peers);
        assert!(watch.subscribers.is_empty());
        // With nobody watching, nothing is tracked.
        watch.upkeep(&peers);
        assert!(watch.seen.is_empty());
    }
}
