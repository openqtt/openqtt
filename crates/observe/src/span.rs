//! The spans every role opens, and the names of the fields they carry, so the log lines of any
//! role can be searched and joined on the same names.
//!
//! A log line in JSON carries the spans it was written in, with their fields: a line written
//! while serving a client names its [`CLIENT_ID`], one written while replicating a partition
//! names its [`PARTITION`], and one about another node names it in [`NODE`]. The node that
//! wrote the line is not a field: the binary states it once, as the process's identity.
//!
//! Spans are made by the functions here rather than by `tracing::info_span!` at each site, so a
//! field's name is written once.

use tracing::Span;

/// The MQTT client identifier a span serves, as the session knows it: the one the client sent,
/// the one OpenQTT assigned, or the certificate's CN on a listener with `identity_from_cn`.
pub const CLIENT_ID: &str = "client_id";

/// The log partition a span works on, a number below `log.partitions` (R3).
pub const PARTITION: &str = "partition";

/// The other node a span talks to, by its `cluster.node_name`.
pub const NODE: &str = "node";

/// The span around everything an edge does for one client.
pub fn client(client_id: &str) -> Span {
    tracing::info_span!("client", client_id)
}

/// The span around work on one log partition.
pub fn partition(partition: u32) -> Span {
    tracing::info_span!("partition", partition)
}

/// The span around a request to, or from, another node.
pub fn node(node: &str) -> Span {
    tracing::info_span!("node", node)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_span_carries_its_field_under_the_name_the_constant_gives() {
        tracing::subscriber::with_default(tracing_subscriber::registry(), || {
            for (span, name, field) in [
                (client("device-1"), "client", CLIENT_ID),
                (partition(17), "partition", PARTITION),
                (node("openqtt-log-0"), "node", NODE),
            ] {
                let metadata = span.metadata().expect("an enabled span has metadata");
                assert_eq!(metadata.name(), name);
                let fields: Vec<&str> =
                    metadata.fields().iter().map(|field| field.name()).collect();
                assert_eq!(fields, [field]);
            }
        });
    }
}
