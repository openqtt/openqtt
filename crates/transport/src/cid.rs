//! Connection IDs that name the node and the endpoint that issued them, so that a load balancer or
//! a forwarder can route a client's packets by connection ID alone (report R7, docs/spec/
//! mqtt-over-quic.md section 5): once a client's address changes, its address no longer leads to
//! the edge holding its connection, and the connection ID still does.
//!
//! # Layout
//!
//! 18 bytes, in the format of QUIC-LB (draft-ietf-quic-load-balancers-21, section 3) used without
//! a key:
//!
//! | Bytes | Field | Value |
//! | --- | --- | --- |
//! | 0 | first octet | config rotation 0 in the top three bits, and 17, the length of what follows, in the low five: `0x11` |
//! | 1 to 8 | server ID | the node's [`NodeId`], big-endian |
//! | 9 | server ID | the index of the endpoint on the node, one per core (R7, D7) |
//! | 10 to 17 | nonce | 8 random bytes |
//!
//! A QUIC-LB load balancer configured with a 9-byte server ID routes to the endpoint. One
//! configured with an 8-byte server ID and a 9-byte nonce routes to the node, which can steer by
//! byte 9 to the endpoint's socket, for instance with a reuseport BPF program. QUIC-LB keeps
//! config rotation 0b111 for connection IDs that cannot be routed; these never use it.
//!
//! The server ID is in the clear. An observer learns which endpoint a connection belongs to, and
//! can tell that two connection IDs belong to the same endpoint, though not to the same
//! connection among the others there. QUIC-LB advises IDs without a key only for the server's
//! first one; encrypting the server ID is for report R7 to decide once S3 has measured routing.
//!
//! An endpoint not given a node, as in tests, uses quinn's own connection IDs: 8 bytes, random
//! but for a keyed check value.

use std::time::Duration;

use openqtt_core::NodeId;
use quinn::{ConnectionId, ConnectionIdGenerator};
use quinn_proto::InvalidCid;
use rustls::crypto::SecureRandom;

/// The length of a connection ID in this layout.
pub const CID_LEN: usize = 18;

/// The first octet: config rotation 0, and the length of the rest.
const FIRST_OCTET: u8 = 0x11;

/// The first octet and the server ID: what every ID an endpoint issues begins with.
const PREFIX_LEN: usize = 10;

/// The node and endpoint a connection ID in this layout names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct CidRoute {
    /// The node that issued it.
    pub node: NodeId,
    /// The index of the endpoint on that node.
    pub endpoint: u8,
}

impl CidRoute {
    /// The route in `cid`, or `None` for a connection ID not in this layout.
    pub fn of(cid: &[u8]) -> Option<Self> {
        if cid.len() != CID_LEN || cid.first() != Some(&FIRST_OCTET) {
            return None;
        }
        let node: [u8; 8] = cid.get(1..9)?.try_into().ok()?;
        Some(Self {
            node: NodeId::new(u64::from_be_bytes(node)),
            endpoint: *cid.get(9)?,
        })
    }

    /// The first octet and server ID every connection ID of this route begins with.
    fn prefix(self) -> [u8; PREFIX_LEN] {
        let mut prefix = [0; PREFIX_LEN];
        prefix[0] = FIRST_OCTET;
        prefix[1..9].copy_from_slice(&self.node.get().to_be_bytes());
        prefix[9] = self.endpoint;
        prefix
    }
}

/// Issues the connection IDs of one endpoint, in the layout above.
pub(crate) struct NodeConnectionIds {
    prefix: [u8; PREFIX_LEN],
    random: &'static dyn SecureRandom,
}

impl NodeConnectionIds {
    /// The generator of endpoint `endpoint` on `node`.
    pub(crate) fn new(node: NodeId, endpoint: u8) -> Self {
        Self {
            prefix: CidRoute { node, endpoint }.prefix(),
            random: rustls::crypto::aws_lc_rs::default_provider().secure_random,
        }
    }
}

impl ConnectionIdGenerator for NodeConnectionIds {
    fn generate_cid(&mut self) -> ConnectionId {
        let mut cid = [0; CID_LEN];
        cid[..PREFIX_LEN].copy_from_slice(&self.prefix);
        // aws-lc's generator does not fail once the process has drawn from it, and quinn has no
        // way to hear of an ID that could not be made. Carrying on with a nonce left at zero
        // would hand out IDs an off-path attacker can guess.
        self.random
            .fill(&mut cid[PREFIX_LEN..])
            .expect("the system random generator works");
        ConnectionId::new(&cid)
    }

    /// Refuses an ID this endpoint did not issue, so that a packet meant for another node or
    /// another endpoint of this one is dropped without the stateless reset quinn would
    /// otherwise answer it with.
    fn validate(&self, cid: &ConnectionId) -> Result<(), InvalidCid> {
        if cid.len() == CID_LEN && cid.starts_with(&self.prefix) {
            Ok(())
        } else {
            Err(InvalidCid)
        }
    }

    fn cid_len(&self) -> usize {
        CID_LEN
    }

    fn cid_lifetime(&self) -> Option<Duration> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_carries_the_first_octet_the_node_and_the_endpoint() {
        let node = NodeId::new(0x0102_0304_0506_0708);
        let mut ids = NodeConnectionIds::new(node, 3);
        let cid = ids.generate_cid();
        assert_eq!(cid.len(), CID_LEN);
        assert_eq!(ids.cid_len(), CID_LEN);
        assert_eq!(
            cid[..PREFIX_LEN],
            [0x11, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x03]
        );
        // QUIC-LB: config rotation in the top three bits, the length of the rest in the low five.
        assert_eq!(cid[0] >> 5, 0);
        assert_eq!(usize::from(cid[0] & 0x1F), CID_LEN - 1);
        assert_eq!(CidRoute::of(&cid), Some(CidRoute { node, endpoint: 3 }));
        assert_eq!(ids.cid_lifetime(), None);
    }

    #[test]
    fn the_nonce_is_drawn_for_every_id() {
        let mut ids = NodeConnectionIds::new(NodeId::new(9), 0);
        let first = ids.generate_cid();
        let second = ids.generate_cid();
        assert_ne!(first, second);
        assert_eq!(first[..PREFIX_LEN], second[..PREFIX_LEN]);
    }

    #[test]
    fn only_the_ids_of_this_endpoint_are_valid() {
        let mut ids = NodeConnectionIds::new(NodeId::new(9), 1);
        let mine = ids.generate_cid();
        assert!(ids.validate(&mine).is_ok());
        let sibling = NodeConnectionIds::new(NodeId::new(9), 2).generate_cid();
        assert!(ids.validate(&sibling).is_err());
        let other_node = NodeConnectionIds::new(NodeId::new(10), 1).generate_cid();
        assert!(ids.validate(&other_node).is_err());
        assert!(ids.validate(&ConnectionId::new(&mine[..8])).is_err());
        assert!(ids.validate(&ConnectionId::new(&[0; CID_LEN])).is_err());
    }

    #[test]
    fn other_layouts_have_no_route() {
        assert_eq!(CidRoute::of(&[0x11; 17]), None);
        assert_eq!(CidRoute::of(&[0x12; CID_LEN]), None);
        assert_eq!(CidRoute::of(&[]), None);
        let mut cid = [0; CID_LEN];
        cid[0] = 0x11;
        cid[9] = 254;
        assert_eq!(
            CidRoute::of(&cid),
            Some(CidRoute {
                node: NodeId::new(0),
                endpoint: 254
            })
        );
    }
}
