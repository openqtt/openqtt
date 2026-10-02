//! A pod's identity in the cluster.

use std::fmt;

/// A pod of the cluster, as the meta group numbers it when the pod registers (report R3,
/// section Membership and placement). A 64-bit number, the type openraft gives a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    /// The node numbered `id`.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A pod's incarnation. The meta group gives a pod a higher epoch every time it registers,
/// which it does on every start. Every request to a log partition carries `(NodeId, Epoch)`,
/// and one with a stale epoch is refused, so a pod that lost its lease and came back cannot
/// act on what it owned before (R3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch before any registration.
    pub const ZERO: Self = Self(0);

    /// The epoch `value`.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The epoch after this one, or `None` when there is none.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_only_go_up() {
        let first = Epoch::ZERO.next().unwrap();
        assert_eq!(first, Epoch::new(1));
        assert!(first > Epoch::ZERO);
        assert_eq!(Epoch::new(u64::MAX).next(), None);
        assert_eq!(first.get(), 1);
        assert_eq!(first.to_string(), "1");
    }

    #[test]
    fn node_ids_are_numbers() {
        let node = NodeId::new(7);
        assert_eq!(node.get(), 7);
        assert_eq!(node.to_string(), "7");
        assert!(NodeId::new(3) < node);
    }
}
