//! Encoding: check a packet, measure it, then write it in one pass.
//!
//! Every packet is checked against the rules a receiver applies before a byte is written, so a
//! packet that encodes is one the decoder accepts, and a packet that does not encode leaves
//! the buffer as it was.

use bytes::{BufMut, BytesMut};

use crate::primitives::{put_variable_byte_integer, variable_byte_integer_len};
use crate::{Error, MAX_PACKET_SIZE, MAX_VARIABLE_BYTE_INTEGER};

/// A packet below its fixed header.
pub(crate) trait Encode {
    /// The first byte of the fixed header: the packet type and its flags.
    fn first_byte(&self) -> u8;

    /// Checks the packet against every rule a receiver applies, and returns its Remaining
    /// Length.
    fn remaining_length(&self) -> Result<usize, Error>;

    /// Writes the variable header and payload of a packet that
    /// [`remaining_length`](Self::remaining_length) accepted, exactly that many bytes.
    fn write_body(&self, dst: &mut BytesMut);
}

/// The size of a packet with this Remaining Length, fixed header included, or
/// [`Error::PacketTooLarge`] when no Variable Byte Integer can hold the length.
fn packet_len(remaining_length: usize) -> Result<usize, Error> {
    match u32::try_from(remaining_length) {
        Ok(length) if length <= MAX_VARIABLE_BYTE_INTEGER => {
            Ok(1 + variable_byte_integer_len(length) + remaining_length)
        }
        _ => Err(Error::PacketTooLarge {
            size: remaining_length.saturating_add(5),
            maximum: MAX_PACKET_SIZE,
        }),
    }
}

/// Whether a packet of `size` bytes is larger than `maximum`.
pub(crate) fn exceeds(size: usize, maximum: u32) -> bool {
    u64::try_from(size).map_or(true, |size| size > u64::from(maximum))
}

/// The exact size of the encoded packet.
pub(crate) fn encoded_len(packet: &impl Encode) -> Result<usize, Error> {
    packet_len(packet.remaining_length()?)
}

/// Appends the packet to `dst` if it is at most `max_packet_size` bytes.
pub(crate) fn encode(
    packet: &impl Encode,
    dst: &mut BytesMut,
    max_packet_size: u32,
) -> Result<(), Error> {
    let remaining_length = packet.remaining_length()?;
    let size = packet_len(remaining_length)?;
    if exceeds(size, max_packet_size) {
        return Err(Error::PacketTooLarge {
            size,
            maximum: max_packet_size,
        });
    }
    dst.reserve(size);
    let start = dst.len();
    dst.put_u8(packet.first_byte());
    put_variable_byte_integer(
        dst,
        u32::try_from(remaining_length).unwrap_or(MAX_VARIABLE_BYTE_INTEGER),
    );
    packet.write_body(dst);
    debug_assert_eq!(dst.len() - start, size, "a packet writes what it measured");
    Ok(())
}

/// Gives each packet type the public encoding methods.
macro_rules! encode_methods {
    ($($packet:ty),+ $(,)?) => {$(
        impl $packet {
            /// The exact number of bytes [`encode`](Self::encode) writes, so a caller can
            /// hold the packet to the peer's Maximum Packet Size before writing it. The packet
            /// is checked as `encode` checks it, and an error here is the error `encode` would
            /// return.
            pub fn encoded_len(&self) -> Result<usize, $crate::Error> {
                $crate::encode::encoded_len(self)
            }

            /// Appends the packet to `dst`.
            ///
            /// # Errors
            ///
            /// When the packet breaks a rule its receiver would refuse it for, or holds a
            /// value the wire cannot represent. Nothing is written then.
            pub fn encode(&self, dst: &mut ::bytes::BytesMut) -> Result<(), $crate::Error> {
                $crate::encode::encode(self, dst, $crate::MAX_PACKET_SIZE)
            }

            /// [`encode`](Self::encode), for a peer whose Maximum Packet Size is
            /// `max_packet_size`. A larger packet is refused with
            /// [`Error::PacketTooLarge`](crate::Error::PacketTooLarge) before anything is
            /// written, since neither end may send a packet over the other's limit
            /// ([MQTT-3.1.2-24], [MQTT-3.2.2-15]). Dropping it and carrying on as if it had
            /// been sent ([MQTT-3.1.2-25]) is the caller's part.
            ///
            /// # Errors
            ///
            /// As [`encode`](Self::encode), and when the packet is larger than
            /// `max_packet_size`.
            pub fn encode_within(
                &self,
                dst: &mut ::bytes::BytesMut,
                max_packet_size: u32,
            ) -> Result<(), $crate::Error> {
                $crate::encode::encode(self, dst, max_packet_size)
            }
        }
    )+};
}

pub(crate) use encode_methods;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_2_1_4_a_packet_is_its_fixed_header_plus_its_remaining_length() {
        assert_eq!(packet_len(0), Ok(2));
        assert_eq!(packet_len(127), Ok(129));
        assert_eq!(packet_len(128), Ok(131));
        let largest = usize::try_from(MAX_VARIABLE_BYTE_INTEGER).unwrap();
        assert_eq!(
            packet_len(largest),
            Ok(usize::try_from(MAX_PACKET_SIZE).unwrap())
        );
        assert!(matches!(
            packet_len(largest + 1),
            Err(Error::PacketTooLarge {
                maximum: MAX_PACKET_SIZE,
                ..
            })
        ));
    }

    #[test]
    fn exceeds_compares_sizes_in_bytes() {
        assert!(!exceeds(10, 10));
        assert!(exceeds(11, 10));
        assert!(!exceeds(0, 0));
        assert!(exceeds(usize::MAX, u32::MAX));
    }
}
