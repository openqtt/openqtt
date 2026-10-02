//! Reading the log: the committed messages of each partition, in order.

use openqtt_core::Message;

use crate::{BoxFuture, Error};

/// One committed message of a log partition.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Entry {
    /// The partition.
    pub partition: u32,
    /// Its place in the partition: higher for every later entry, never reused.
    pub sequence: u64,
    /// The message, its topic as the cluster routes it.
    pub message: Message,
}

impl Entry {
    /// The message at `sequence` in `partition`.
    pub fn new(partition: u32, sequence: u64, message: Message) -> Self {
        Self {
            partition,
            sequence,
            message,
        }
    }
}

/// Reads the committed messages of log partitions, each partition in order, from a cursor the
/// log keeps for it.
///
/// The log role offers each partition's entries in batches, in sequence order. When
/// [`consume`](Self::consume) completes with `Ok`, the log moves the consumer's cursor past the
/// batch and commits the cursor like any other write, so it survives a restart; with `Err`,
/// the same entries are offered again. Delivery is therefore at least once, and an entry's
/// [`sequence`](Entry::sequence) lets a consumer drop one it has seen.
pub trait LogConsumer: Send + Sync + 'static {
    /// The name the log keeps this consumer's cursors under, unique in the cluster and stable
    /// across restarts: a new name starts from the oldest entry still kept.
    fn name(&self) -> &str;

    /// Whether this consumer reads `partition`. By default it reads every one.
    fn reads(&self, partition: u32) -> bool {
        let _ = partition;
        true
    }

    /// Takes the next entries of one partition, at least one, in sequence order.
    fn consume<'a>(&'a self, batch: &'a [Entry]) -> BoxFuture<'a, Result<(), Error>>;
}
