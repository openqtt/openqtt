//! What the unit tests share: hand assembly of wire bytes, and encoding a packet into its
//! first byte and body.

use bytes::{Bytes, BytesMut};

use crate::encode::{Encode, encode};
use crate::primitives::decode_variable_byte_integer;
use crate::{DataType, MAX_PACKET_SIZE, PropertyId};

/// Joins byte strings into one buffer.
pub(crate) fn concat(parts: &[&[u8]]) -> Bytes {
    Bytes::from(parts.concat())
}

/// A UTF-8 Encoded String or Binary Data as the wire carries it: a Two Byte Integer length,
/// then the bytes.
pub(crate) fn prefixed(data: &[u8]) -> Vec<u8> {
    let mut wire = u16::try_from(data.len()).unwrap().to_be_bytes().to_vec();
    wire.extend_from_slice(data);
    wire
}

/// A Variable Byte Integer, written the way section 1.5.5 describes.
pub(crate) fn vbi(value: usize) -> Vec<u8> {
    let mut rest = value;
    let mut wire = Vec::new();
    loop {
        let digit = u8::try_from(rest % 128).unwrap();
        rest /= 128;
        if rest == 0 {
            wire.push(digit);
            return wire;
        }
        wire.push(digit | 0x80);
    }
}

/// A set of properties as the wire carries it: the Property Length, then the properties.
pub(crate) fn properties(properties: &[&[u8]]) -> Vec<u8> {
    let body = properties.concat();
    let mut wire = vbi(body.len());
    wire.extend(body);
    wire
}

/// One valid occurrence of property `id`: its identifier and a value of its data type that
/// every property of that type accepts.
pub(crate) fn sample(id: PropertyId) -> Vec<u8> {
    let mut wire = vec![id.value()];
    match id.data_type() {
        DataType::Byte => wire.push(1),
        DataType::TwoByteInteger => wire.extend([0x00, 0x0A]),
        DataType::FourByteInteger => wire.extend([0x00, 0x00, 0x00, 0x0A]),
        DataType::VariableByteInteger => wire.push(0x0A),
        DataType::Utf8EncodedString => wire.extend(prefixed(b"text")),
        DataType::BinaryData => wire.extend(prefixed(&[1, 2, 3])),
        DataType::Utf8StringPair => {
            wire.extend(prefixed(b"name"));
            wire.extend(prefixed(b"value"));
        }
    }
    wire
}

/// Encodes a packet and splits it into its first byte and its body, the bytes after the
/// fixed header, checking the Remaining Length and [`Encode`]'s measure on the way.
pub(crate) fn encode_parts(packet: &impl Encode) -> (u8, Bytes) {
    let mut dst = BytesMut::new();
    encode(packet, &mut dst, MAX_PACKET_SIZE).unwrap();
    let packet = dst.freeze();
    let (len, header) = decode_variable_byte_integer(&packet[1..], "Remaining Length")
        .unwrap()
        .unwrap();
    assert_eq!(packet.len(), 1 + header + usize::try_from(len).unwrap());
    (packet[0], packet.slice(1 + header..))
}
