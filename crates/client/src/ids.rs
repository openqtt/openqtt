//! Packet Identifiers for the packets the client starts: PUBLISH at QoS 1 and 2, SUBSCRIBE and
//! UNSUBSCRIBE (section 2.2.1).

use openqtt_codec::PacketId;

/// The number of 64-bit words that hold one bit per 16-bit identifier.
const WORDS: usize = 1 << 10;

/// Hands out Packet Identifiers that are not in use ([MQTT-2.2.1-3]).
///
/// Identifiers are scoped to the session, not to a connection: one still waiting for its
/// acknowledgement stays taken across a reconnect. Allocation takes the next identifier after
/// the last one handed out that is free, so an identifier is not reused while any other is
/// free, and one still in flight is skipped rather than reused.
#[derive(Debug, Clone)]
pub(crate) struct PacketIds {
    /// One bit per identifier; bit 0 of word 0, identifier 0, is never set.
    in_use: Box<[u64; WORDS]>,
    /// Where the search for a free identifier starts.
    next: u16,
    /// How many identifiers are taken.
    taken: u16,
}

impl Default for PacketIds {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketIds {
    /// No identifier in use.
    pub(crate) fn new() -> Self {
        Self {
            in_use: Box::new([0; WORDS]),
            next: 1,
            taken: 0,
        }
    }

    /// Takes the next free identifier, or `None` when all 65,535 are in use.
    pub(crate) fn allocate(&mut self) -> Option<PacketId> {
        if self.taken == u16::MAX {
            return None;
        }
        let mut candidate = self.next;
        loop {
            // Identifier 0 is never used ([MQTT-2.2.1-3]).
            if let Some(id) = PacketId::new(candidate)
                && !self.contains(id)
            {
                self.take(id);
                self.next = candidate.wrapping_add(1);
                return Some(id);
            }
            candidate = candidate.wrapping_add(1);
        }
    }

    /// Marks an identifier taken, as one a resumed session still has in flight.
    pub(crate) fn take(&mut self, id: PacketId) {
        let (word, bit) = Self::slot(id);
        if self.in_use[word] & bit == 0 {
            self.in_use[word] |= bit;
            self.taken += 1;
        }
    }

    /// Frees an identifier once its exchange is complete.
    pub(crate) fn release(&mut self, id: PacketId) {
        let (word, bit) = Self::slot(id);
        if self.in_use[word] & bit != 0 {
            self.in_use[word] &= !bit;
            self.taken -= 1;
        }
    }

    /// Whether the identifier is taken.
    pub(crate) fn contains(&self, id: PacketId) -> bool {
        let (word, bit) = Self::slot(id);
        self.in_use[word] & bit != 0
    }

    /// The word and bit that hold an identifier.
    fn slot(id: PacketId) -> (usize, u64) {
        let value = id.get();
        (usize::from(value / 64), 1 << u32::from(value % 64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u16) -> PacketId {
        PacketId::new(value).unwrap()
    }

    #[test]
    fn mqtt_2_2_1_3_identifiers_are_nonzero_and_not_in_use() {
        let mut ids = PacketIds::new();
        assert_eq!(ids.allocate(), Some(id(1)));
        assert_eq!(ids.allocate(), Some(id(2)));
        assert_eq!(ids.allocate(), Some(id(3)));
        // 2 completes, but the search goes on from 4 rather than reusing 2 at once.
        ids.release(id(2));
        assert_eq!(ids.allocate(), Some(id(4)));
        assert!(!ids.contains(id(2)));
    }

    #[test]
    fn mqtt_2_2_1_3_identifiers_in_flight_are_skipped_after_wrapping() {
        let mut ids = PacketIds::new();
        // A resumed session still waits on 1 and 3.
        ids.take(id(1));
        ids.take(id(3));
        ids.next = u16::MAX;
        assert_eq!(ids.allocate(), Some(id(u16::MAX)));
        // Past 65,535 the search wraps, skips 0, and skips 1 and 3, which are in flight.
        assert_eq!(ids.allocate(), Some(id(2)));
        assert_eq!(ids.allocate(), Some(id(4)));
    }

    #[test]
    fn exhaustion_is_none_and_a_release_frees_one() {
        let mut ids = PacketIds::new();
        for expected in 1..=u16::MAX {
            assert_eq!(ids.allocate(), Some(id(expected)));
        }
        assert_eq!(ids.allocate(), None);
        ids.release(id(40_000));
        assert_eq!(ids.allocate(), Some(id(40_000)));
        assert_eq!(ids.allocate(), None);
    }

    #[test]
    fn taking_or_releasing_twice_counts_once() {
        let mut ids = PacketIds::new();
        ids.take(id(9));
        ids.take(id(9));
        assert_eq!(ids.taken, 1);
        ids.release(id(9));
        ids.release(id(9));
        assert_eq!(ids.taken, 0);
    }
}
