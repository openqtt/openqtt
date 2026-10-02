//! Destinations outside the cluster: the interest they register and the messages they are
//! given (report R3, the hot path and the extension seam).

use std::fmt;
use std::sync::Arc;

use openqtt_core::{Message, TopicFilter};

use crate::{BoxFuture, Error};

/// A destination outside the cluster, numbered by the extension that serves it. Each
/// [`Forwarder`] has its own numbers; the node tells apart two forwarders' destinations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub struct ExternalId(u32);

impl ExternalId {
    /// The destination numbered `id`.
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ExternalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A filter a destination outside the cluster wants the messages of.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Interest {
    /// The filter, as the cluster routes it: no mountpoint applies.
    pub filter: TopicFilter,
    /// Who wants it.
    pub destination: ExternalId,
    /// Whether the destination's acknowledgement counts toward a PUBACK: when it does, a
    /// PUBACK or PUBREC is sent only once the destination has the message, as it is only once
    /// every durable log partition has committed it (R3). When it does not, the message is
    /// handed over and not waited for, as on a lane to another edge.
    pub durable: bool,
}

impl Interest {
    /// `destination` wants `filter`, and its acknowledgement counts toward a PUBACK when
    /// `durable`.
    pub fn new(filter: TopicFilter, destination: ExternalId, durable: bool) -> Self {
        Self {
            filter,
            destination,
            durable,
        }
    }
}

/// Where an [`InterestSource`] registers and withdraws interest. The node implements it, and
/// routes it as it routes the interest of its own edges and partitions.
pub trait InterestRegistry: Send + Sync {
    /// Adds `interest`; adding it again changes nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] when the cluster cannot take it now, and the source should try
    /// again.
    fn register(&self, interest: Interest) -> Result<(), Error>;

    /// Removes `interest`; removing what is not there changes nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] when the cluster cannot take the change now.
    fn withdraw(&self, interest: &Interest) -> Result<(), Error>;
}

/// Says which messages destinations outside the cluster want. Each source is paired, when the
/// node is assembled, with the [`Forwarder`] that delivers to its destinations.
pub trait InterestSource: Send + Sync + 'static {
    /// Called once, when the node starts, with the registry to register interest in now and
    /// to change it through later. The source keeps the registry for as long as it runs.
    fn start(&self, registry: Arc<dyn InterestRegistry>);
}

/// The acknowledgement of a forwarded message: it completes once the destination has it, and
/// owes nothing to the forwarder or the message, so the edge can hold it beside its log
/// commits.
pub type Ack = BoxFuture<'static, Result<(), Error>>;

/// Delivers messages to destinations outside the cluster.
///
/// The publishing edge calls it for each message that matched the interest of one of its
/// destinations, once per destination, after authorization and before the PUBACK.
pub trait Forwarder: Send + Sync + 'static {
    /// Hands `message` to `destination`. For durable interest the PUBACK or PUBREC waits for
    /// the acknowledgement, and an error fails the publish as a failed commit to the log does;
    /// for the rest the edge may drop the acknowledgement unawaited.
    fn forward(&self, destination: ExternalId, message: &Message) -> Ack;
}
