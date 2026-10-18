use anyhow::Result;
use std::io::Write;
use std::process::exit;
use tonic::Request;

use pb::WatchNeighborsRequest;
use pb::bgp_service_client::BgpServiceClient;

pub mod pb {
    tonic::include_proto!("zebra.bgp.v1");
}

/// One line per neighbor: `<kind> <address>[%<interface>] <state>
/// [AS <remote-as> <peer-type>] [last reset: <reason>]`, where kind is
/// `snapshot` (a new full set follows), `neighbor` or `removed`.
fn format_event(event: &pb::NeighborEvent) -> String {
    let mut out = String::new();
    if event.snapshot {
        out.push_str(&format!("snapshot {} neighbors\n", event.neighbors.len()));
    }
    let name = |n: &pb::Neighbor| {
        if n.interface.is_empty() {
            n.address.clone()
        } else {
            format!("{}%{}", n.address, n.interface)
        }
    };
    for n in &event.neighbors {
        out.push_str(&format!(
            "neighbor {} {} AS {} {}",
            name(n),
            n.state,
            n.remote_as,
            n.peer_type
        ));
        if !n.last_reset.is_empty() {
            out.push_str(&format!(" (last reset: {})", n.last_reset));
        }
        out.push('\n');
    }
    for n in &event.removed {
        out.push_str(&format!("removed {}\n", name(n)));
    }
    out
}

pub async fn watch_bgp_neighbors(host: &str) -> Result<()> {
    let uri = crate::endpoint::host_uri(host);
    let channel = match crate::endpoint::connect(&uri).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Can't connect to {uri}: {e}");
            exit(3);
        }
    };
    let mut client = BgpServiceClient::new(channel);
    let mut stream = client
        .watch_neighbors(Request::new(WatchNeighborsRequest {}))
        .await?
        .into_inner();
    while let Some(event) = stream.message().await? {
        print!("{}", format_event(&event));
        std::io::stdout().flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_event_renders_snapshot_changes_and_removals() {
        let event = pb::NeighborEvent {
            snapshot: true,
            neighbors: vec![pb::Neighbor {
                address: "192.0.2.1".into(),
                remote_as: 65001,
                peer_type: "external".into(),
                state: "Active".into(),
                last_reset: "Hold timer expired".into(),
                ..Default::default()
            }],
            removed: vec![pb::Neighbor {
                address: "fe80::1".into(),
                interface: "eth0".into(),
                ..Default::default()
            }],
        };
        assert_eq!(
            format_event(&event),
            "snapshot 1 neighbors\n\
             neighbor 192.0.2.1 Active AS 65001 external (last reset: Hold timer expired)\n\
             removed fe80::1%eth0\n"
        );
    }
}
