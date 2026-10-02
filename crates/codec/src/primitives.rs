//! The data representations of section 1.5: Two and Four Byte Integers, the Variable Byte
//! Integer, UTF-8 Encoded Strings, Binary Data and UTF-8 String Pairs.
//!
//! [`Reader`] reads them from a packet already known to be complete, so running out of bytes
//! is a Malformed Packet rather than a reason to wait for more. The `put_*` functions write
//! them into a packet that has already been measured and checked, so they cannot fail.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the decoder that calls the packet decoders arrives in a following commit"
    )
)]

use bytes::{BufMut, Bytes, BytesMut};

use crate::{Error, PacketId, PacketType};

/// The largest value a Variable Byte Integer holds: four bytes of seven bits (section 1.5.5,
/// Table 1-1). It bounds the Remaining Length, every Property Length and the Subscription
/// Identifier.
pub const MAX_VARIABLE_BYTE_INTEGER: u32 = 268_435_455;

/// The longest UTF-8 Encoded String or Binary Data value, in bytes. Both are prefixed by a Two
/// Byte Integer length (sections 1.5.4 and 1.5.6).
pub const MAX_STRING_LEN: usize = 65_535;

/// The largest packet the protocol can express: one byte of type and flags, four of Remaining
/// Length, and the largest Remaining Length (section 2.1.4).
pub const MAX_PACKET_SIZE: u32 = 1 + 4 + MAX_VARIABLE_BYTE_INTEGER;

/// Reads a Variable Byte Integer from the front of `bytes`, returning its value and how many
/// bytes it took, or `None` when `bytes` ends before the integer does.
pub(crate) fn decode_variable_byte_integer(
    bytes: &[u8],
    field: &'static str,
) -> Result<Option<(u32, usize)>, Error> {
    let mut value = 0u32;
    for (index, &byte) in bytes.iter().take(4).enumerate() {
        value |= u32::from(byte & 0x7F) << (7 * index);
        if byte & 0x80 == 0 {
            // The last byte holds the most significant digit. Zero there, after the first
            // byte, means fewer bytes would have held the value [MQTT-1.5.5-1].
            if index > 0 && byte == 0 {
                return Err(Error::MalformedVariableByteInteger { field });
            }
            return Ok(Some((value, index + 1)));
        }
    }
    if bytes.len() >= 4 {
        // A continuation bit on the fourth byte asks for a fifth, which section 1.5.5 forbids.
        Err(Error::MalformedVariableByteInteger { field })
    } else {
        Ok(None)
    }
}

/// How many bytes the Variable Byte Integer encoding of `value` takes. Values above
/// [`MAX_VARIABLE_BYTE_INTEGER`] have no encoding; callers refuse them first.
pub(crate) const fn variable_byte_integer_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

/// Writes `value` as a Variable Byte Integer in the fewest bytes, as [MQTT-1.5.5-1] requires.
pub(crate) fn put_variable_byte_integer(dst: &mut BytesMut, value: u32) {
    debug_assert!(
        value <= MAX_VARIABLE_BYTE_INTEGER,
        "measured before writing"
    );
    let mut rest = value;
    loop {
        let digit = rest.to_le_bytes()[0] & 0x7F;
        rest >>= 7;
        if rest == 0 {
            dst.put_u8(digit);
            return;
        }
        dst.put_u8(digit | 0x80);
    }
}

/// Checks the character data of a received UTF-8 Encoded String.
///
/// Rust's UTF-8 validation refuses everything [MQTT-1.5.4-1] does: overlong forms, code points
/// above U+10FFFF and encoded surrogates. U+0000 is checked separately because it is
/// well-formed UTF-8 that [MQTT-1.5.4-2] still forbids. A byte order mark is kept as the
/// character U+FEFF, never stripped ([MQTT-1.5.4-3]).
pub(crate) fn check_utf8<'a>(raw: &'a [u8], field: &'static str) -> Result<&'a str, Error> {
    let text = std::str::from_utf8(raw).map_err(|_| Error::InvalidUtf8 { field })?;
    if raw.contains(&0) {
        return Err(Error::NullCharacter { field });
    }
    Ok(text)
}

/// Checks an outgoing UTF-8 Encoded String against the rules a receiver applies, and returns
/// its encoded size with the length prefix.
pub(crate) fn string_len(text: &str, field: &'static str) -> Result<usize, Error> {
    if text.as_bytes().contains(&0) {
        return Err(Error::NullCharacter { field });
    }
    binary_len(text.as_bytes(), field)
}

/// Checks that outgoing Binary Data fits its length prefix, and returns its encoded size with
/// the prefix.
pub(crate) fn binary_len(data: &[u8], field: &'static str) -> Result<usize, Error> {
    if data.len() > MAX_STRING_LEN {
        return Err(Error::TooLong {
            field,
            len: data.len(),
        });
    }
    Ok(2 + data.len())
}

/// Writes a UTF-8 Encoded String checked by [`string_len`].
pub(crate) fn put_string(dst: &mut BytesMut, text: &str) {
    put_binary(dst, text.as_bytes());
}

/// Writes Binary Data checked by [`binary_len`].
pub(crate) fn put_binary(dst: &mut BytesMut, data: &[u8]) {
    debug_assert!(data.len() <= MAX_STRING_LEN, "measured before writing");
    dst.put_u16(u16::try_from(data.len()).unwrap_or(u16::MAX));
    dst.put_slice(data);
}

/// Whether `c` is one of the Disallowed Unicode code points of section 1.5.4: a control
/// character in U+0001 to U+001F or U+007F to U+009F, or a noncharacter (U+FDD0 to U+FDEF, and
/// the last two code points of every plane, such as U+FFFE and U+FFFF).
///
/// The specification says a string SHOULD NOT contain them and a receiver MAY treat one as a
/// Malformed Packet. The codec accepts them: refusing them here would refuse them in every
/// field alike, User Property values included, where a tab or a line feed is ordinary. A
/// caller that wants them gone, as section 5.4.9 suggests for Topic Names, refuses them with
/// [`disallowed_code_point`] in the fields it chooses.
pub const fn is_disallowed_code_point(c: char) -> bool {
    let code = c as u32;
    matches!(code, 0x0001..=0x001F | 0x007F..=0x009F | 0xFDD0..=0xFDEF) || code & 0xFFFE == 0xFFFE
}

/// The first Disallowed Unicode code point in `text`, if any; see [`is_disallowed_code_point`].
pub fn disallowed_code_point(text: &str) -> Option<char> {
    text.chars().find(|&c| is_disallowed_code_point(c))
}

/// Reads the section 1.5 data types from a complete packet, front to back.
///
/// Binary Data comes out as a slice of the packet's own buffer, so a payload is never copied.
#[derive(Debug, Clone)]
pub(crate) struct Reader<'a> {
    buf: &'a Bytes,
    pos: usize,
    end: usize,
}

impl<'a> Reader<'a> {
    /// Reads all of `buf`.
    pub(crate) fn new(buf: &'a Bytes) -> Self {
        Self {
            buf,
            pos: 0,
            end: buf.len(),
        }
    }

    /// Whether every byte has been read.
    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.end
    }

    /// How many bytes are left.
    pub(crate) fn remaining(&self) -> usize {
        self.end.saturating_sub(self.pos)
    }

    /// Steps over `len` bytes, returning where they start.
    fn advance(&mut self, len: usize, field: &'static str) -> Result<usize, Error> {
        if len > self.remaining() {
            return Err(Error::Truncated { field });
        }
        let start = self.pos;
        self.pos += len;
        Ok(start)
    }

    /// The next `len` bytes.
    fn slice(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], Error> {
        let start = self.advance(len, field)?;
        let buf: &'a [u8] = self.buf;
        buf.get(start..start + len)
            .ok_or(Error::Truncated { field })
    }

    /// The next `N` bytes, as an array.
    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], Error> {
        let bytes = self.slice(N, field)?;
        <[u8; N]>::try_from(bytes).map_err(|_| Error::Truncated { field })
    }

    /// A Byte.
    pub(crate) fn u8(&mut self, field: &'static str) -> Result<u8, Error> {
        let [byte] = self.array(field)?;
        Ok(byte)
    }

    /// A Two Byte Integer, big-endian (section 1.5.2).
    pub(crate) fn u16(&mut self, field: &'static str) -> Result<u16, Error> {
        self.array(field).map(u16::from_be_bytes)
    }

    /// A Four Byte Integer, big-endian (section 1.5.3).
    pub(crate) fn u32(&mut self, field: &'static str) -> Result<u32, Error> {
        self.array(field).map(u32::from_be_bytes)
    }

    /// A Packet Identifier, which is never 0 where a packet carries one ([MQTT-2.2.1-3],
    /// [MQTT-2.2.1-4]; an acknowledgement repeats one of those, [MQTT-2.2.1-5] and
    /// [MQTT-2.2.1-6]).
    pub(crate) fn packet_id(&mut self, packet_type: PacketType) -> Result<PacketId, Error> {
        PacketId::new(self.u16("Packet Identifier")?)
            .ok_or(Error::ZeroPacketIdentifier { packet_type })
    }

    /// A Variable Byte Integer (section 1.5.5).
    pub(crate) fn variable_byte_integer(&mut self, field: &'static str) -> Result<u32, Error> {
        let buf: &'a [u8] = self.buf;
        let rest = buf.get(self.pos..self.end).unwrap_or_default();
        let (value, len) =
            decode_variable_byte_integer(rest, field)?.ok_or(Error::Truncated { field })?;
        self.pos += len;
        Ok(value)
    }

    /// A UTF-8 Encoded String (section 1.5.4), checked by [`check_utf8`].
    pub(crate) fn string(&mut self, field: &'static str) -> Result<String, Error> {
        let len = usize::from(self.u16(field)?);
        let raw = self.slice(len, field)?;
        check_utf8(raw, field).map(str::to_owned)
    }

    /// Binary Data (section 1.5.6), sharing the packet's buffer.
    pub(crate) fn binary(&mut self, field: &'static str) -> Result<Bytes, Error> {
        let len = usize::from(self.u16(field)?);
        let start = self.advance(len, field)?;
        Ok(self.buf.slice(start..start + len))
    }

    /// A UTF-8 String Pair (section 1.5.7); both strings follow the rules of a UTF-8 Encoded
    /// String ([MQTT-1.5.7-1]).
    pub(crate) fn string_pair(&mut self, field: &'static str) -> Result<(String, String), Error> {
        let name = self.string(field)?;
        let value = self.string(field)?;
        Ok((name, value))
    }

    /// A reader over the next `len` bytes, which this reader then steps over.
    pub(crate) fn take(&mut self, len: usize, field: &'static str) -> Result<Reader<'a>, Error> {
        let start = self.advance(len, field)?;
        Ok(Reader {
            buf: self.buf,
            pos: start,
            end: start + len,
        })
    }

    /// Every byte left, sharing the packet's buffer.
    pub(crate) fn rest(&mut self) -> Bytes {
        let rest = self.buf.slice(self.pos..self.end);
        self.pos = self.end;
        rest
    }

    /// Checks that the last field of a packet has been read: anything after it means the
    /// packet does not match its format, a Malformed Packet.
    pub(crate) fn finish(&self, packet_type: PacketType) -> Result<(), Error> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(Error::TrailingBytes {
                packet_type,
                count: self.remaining(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_vbi(bytes: &[u8]) -> Result<Option<(u32, usize)>, Error> {
        decode_variable_byte_integer(bytes, "Remaining Length")
    }

    fn encode_vbi(value: u32) -> Vec<u8> {
        let mut dst = BytesMut::new();
        put_variable_byte_integer(&mut dst, value);
        dst.to_vec()
    }

    /// Every row of Table 1-1, both ends of each size.
    const TABLE_1_1: [(u32, &[u8]); 8] = [
        (0, &[0x00]),
        (127, &[0x7F]),
        (128, &[0x80, 0x01]),
        (16_383, &[0xFF, 0x7F]),
        (16_384, &[0x80, 0x80, 0x01]),
        (2_097_151, &[0xFF, 0xFF, 0x7F]),
        (2_097_152, &[0x80, 0x80, 0x80, 0x01]),
        (268_435_455, &[0xFF, 0xFF, 0xFF, 0x7F]),
    ];

    #[test]
    fn mqtt_1_5_5_table_1_1_round_trips() {
        for (value, bytes) in TABLE_1_1 {
            assert_eq!(encode_vbi(value), bytes, "encoding {value}");
            assert_eq!(
                variable_byte_integer_len(value),
                bytes.len(),
                "length of {value}"
            );
            assert_eq!(
                decode_vbi(bytes),
                Ok(Some((value, bytes.len()))),
                "decoding {value}"
            );
        }
        assert_eq!(MAX_VARIABLE_BYTE_INTEGER, 268_435_455);
    }

    #[test]
    fn mqtt_1_5_5_1_every_value_encodes_in_the_fewest_bytes() {
        let samples = (0..=MAX_VARIABLE_BYTE_INTEGER)
            .step_by(9_973)
            .chain(TABLE_1_1.iter().map(|&(value, _)| value));
        for value in samples {
            let bytes = encode_vbi(value);
            assert_eq!(bytes.len(), variable_byte_integer_len(value));
            assert_eq!(decode_vbi(&bytes), Ok(Some((value, bytes.len()))));
            // A trailing byte after the integer is not part of it.
            let mut longer = bytes.clone();
            longer.push(0xAB);
            assert_eq!(decode_vbi(&longer), Ok(Some((value, bytes.len()))));
        }
    }

    #[test]
    fn mqtt_1_5_5_1_an_encoding_longer_than_needed_is_malformed() {
        for bytes in [
            &[0x80, 0x00][..],
            &[0xFF, 0x00],
            &[0x80, 0x80, 0x00],
            &[0xFF, 0xFF, 0x00],
            &[0x80, 0x80, 0x80, 0x00],
            &[0x81, 0x80, 0x80, 0x00],
        ] {
            assert_eq!(
                decode_vbi(bytes),
                Err(Error::MalformedVariableByteInteger {
                    field: "Remaining Length"
                }),
                "{bytes:02X?}"
            );
        }
    }

    #[test]
    fn mqtt_1_5_5_a_fifth_byte_is_malformed() {
        for bytes in [
            &[0x80, 0x80, 0x80, 0x80][..],
            &[0x80, 0x80, 0x80, 0x80, 0x01],
            &[0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
        ] {
            assert!(
                matches!(
                    decode_vbi(bytes),
                    Err(Error::MalformedVariableByteInteger { .. })
                ),
                "{bytes:02X?}"
            );
        }
    }

    #[test]
    fn a_variable_byte_integer_cut_short_is_incomplete() {
        for bytes in [&[][..], &[0x80], &[0xFF, 0x80], &[0x80, 0x80, 0x80]] {
            assert_eq!(decode_vbi(bytes), Ok(None), "{bytes:02X?}");
        }
    }

    #[test]
    fn mqtt_1_5_2_and_1_5_3_integers_are_big_endian() {
        let buf = Bytes::from_static(&[0x12, 0x34, 0xDE, 0xAD, 0xBE, 0xEF, 0x07]);
        let mut reader = Reader::new(&buf);
        assert_eq!(reader.u16("Keep Alive"), Ok(0x1234));
        assert_eq!(reader.u32("Session Expiry Interval"), Ok(0xDEAD_BEEF));
        assert_eq!(reader.u8("Reason Code"), Ok(0x07));
        assert!(reader.is_empty());
        assert_eq!(
            reader.u8("Reason Code"),
            Err(Error::Truncated {
                field: "Reason Code"
            })
        );
    }

    #[test]
    fn mqtt_1_5_4_figure_1_2_example_decodes_and_encodes() {
        // Figure 1-2: "A" followed by U+2A6D4, five bytes of character data.
        let wire = [0x00, 0x05, 0x41, 0xF0, 0xAA, 0x9B, 0x94];
        let buf = Bytes::copy_from_slice(&wire);
        let mut reader = Reader::new(&buf);
        assert_eq!(reader.string("Topic Name").as_deref(), Ok("A\u{2A6D4}"));
        assert!(reader.is_empty());

        let mut dst = BytesMut::new();
        assert_eq!(string_len("A\u{2A6D4}", "Topic Name"), Ok(wire.len()));
        put_string(&mut dst, "A\u{2A6D4}");
        assert_eq!(dst[..], wire);
    }

    fn read_string(character_data: &[u8]) -> Result<String, Error> {
        let mut wire = u16::try_from(character_data.len())
            .unwrap()
            .to_be_bytes()
            .to_vec();
        wire.extend_from_slice(character_data);
        let buf = Bytes::from(wire);
        Reader::new(&buf).string("Client Identifier")
    }

    #[test]
    fn mqtt_1_5_4_1_encoded_surrogates_are_malformed() {
        // U+D800 and U+DFFF in the three-byte form a surrogate would take.
        for data in [
            &[0xED, 0xA0, 0x80][..],
            &[0xED, 0xBF, 0xBF],
            &[0x41, 0xED, 0xB0, 0x80],
        ] {
            assert_eq!(
                read_string(data),
                Err(Error::InvalidUtf8 {
                    field: "Client Identifier"
                }),
                "{data:02X?}"
            );
        }
    }

    #[test]
    fn mqtt_1_5_4_1_ill_formed_utf8_is_malformed() {
        for data in [
            &[0xC0, 0x80][..],               // overlong U+0000
            &[0xC1, 0xBF],                   // overlong U+007F
            &[0xE0, 0x80, 0xAF],             // overlong U+002F
            &[0xE2, 0x82],                   // sequence cut short
            &[0xFF],                         // never a UTF-8 byte
            &[0x80],                         // continuation byte without a lead byte
            &[0xF4, 0x90, 0x80, 0x80],       // above U+10FFFF
            &[0xF8, 0x88, 0x80, 0x80, 0x80], // five-byte form
        ] {
            assert_eq!(
                read_string(data),
                Err(Error::InvalidUtf8 {
                    field: "Client Identifier"
                }),
                "{data:02X?}"
            );
        }
    }

    #[test]
    fn mqtt_1_5_4_2_the_null_character_is_malformed() {
        for data in [&[0x00][..], b"a\0b", b"abc\0"] {
            assert_eq!(
                read_string(data),
                Err(Error::NullCharacter {
                    field: "Client Identifier"
                }),
                "{data:02X?}"
            );
        }
        assert_eq!(
            string_len("a\0b", "User Name"),
            Err(Error::NullCharacter { field: "User Name" })
        );
    }

    #[test]
    fn mqtt_1_5_4_3_a_byte_order_mark_is_kept() {
        let text = read_string(&[0xEF, 0xBB, 0xBF, 0x41, 0xEF, 0xBB, 0xBF]).unwrap();
        assert_eq!(text, "\u{FEFF}A\u{FEFF}");
        assert_eq!(text.len(), 7);
    }

    #[test]
    fn mqtt_1_5_4_strings_hold_zero_to_65535_bytes() {
        assert_eq!(read_string(b"").as_deref(), Ok(""));
        let longest = "x".repeat(MAX_STRING_LEN);
        assert_eq!(read_string(longest.as_bytes()), Ok(longest.clone()));
        assert_eq!(string_len(&longest, "Topic Name"), Ok(2 + MAX_STRING_LEN));
        let too_long = "x".repeat(MAX_STRING_LEN + 1);
        assert_eq!(
            string_len(&too_long, "Topic Name"),
            Err(Error::TooLong {
                field: "Topic Name",
                len: MAX_STRING_LEN + 1
            })
        );
    }

    #[test]
    fn disallowed_code_points_are_accepted_by_the_reader() {
        // Section 1.5.4 makes refusing them optional; the codec passes them through.
        assert_eq!(
            read_string("a\tb\u{7F}\u{FFFF}".as_bytes()).as_deref(),
            Ok("a\tb\u{7F}\u{FFFF}")
        );
    }

    #[test]
    fn disallowed_code_points_are_the_controls_and_noncharacters_of_section_1_5_4() {
        let disallowed: Vec<char> = (0..=0x10_FFFF)
            .filter_map(char::from_u32)
            .filter(|&c| is_disallowed_code_point(c))
            .collect();
        // 31 C0 controls without U+0000, 33 from U+007F to U+009F, and 66 noncharacters.
        assert_eq!(disallowed.len(), 31 + 33 + 66);
        for c in [
            '\u{1}',
            '\u{1F}',
            '\u{7F}',
            '\u{9F}',
            '\u{FDD0}',
            '\u{FDEF}',
            '\u{FFFE}',
            '\u{FFFF}',
            '\u{1FFFE}',
            '\u{10FFFF}',
        ] {
            assert!(is_disallowed_code_point(c), "U+{:04X}", u32::from(c));
        }
        for c in [
            '\0',
            ' ',
            '\u{A0}',
            '\u{FDCF}',
            '\u{FDF0}',
            '\u{FFFD}',
            '\u{1FFFD}',
        ] {
            assert!(!is_disallowed_code_point(c), "U+{:04X}", u32::from(c));
        }
        assert_eq!(
            disallowed_code_point("topic/\u{FFFE}/x\u{1}"),
            Some('\u{FFFE}')
        );
        assert_eq!(disallowed_code_point("sensors/temperature"), None);
    }

    #[test]
    fn binary_data_shares_the_packet_buffer() {
        let buf = Bytes::from_static(&[0x00, 0x03, 0xAA, 0xBB, 0xCC, 0xDD]);
        let mut reader = Reader::new(&buf);
        let data = reader.binary("Correlation Data").unwrap();
        assert_eq!(data[..], [0xAA, 0xBB, 0xCC]);
        assert_eq!(data.as_ptr(), buf[2..].as_ptr());
        let rest = reader.rest();
        assert_eq!(rest[..], [0xDD]);
        assert_eq!(rest.as_ptr(), buf[5..].as_ptr());
        assert!(reader.is_empty());
    }

    #[test]
    fn mqtt_1_5_6_binary_data_holds_zero_to_65535_bytes() {
        assert_eq!(binary_len(&[], "Password"), Ok(2));
        assert_eq!(
            binary_len(&[0; MAX_STRING_LEN], "Password"),
            Ok(2 + MAX_STRING_LEN)
        );
        assert_eq!(
            binary_len(&[0; MAX_STRING_LEN + 1], "Password"),
            Err(Error::TooLong {
                field: "Password",
                len: MAX_STRING_LEN + 1
            })
        );
    }

    #[test]
    fn mqtt_1_5_7_1_both_strings_of_a_pair_follow_the_string_rules() {
        let good = Bytes::from_static(&[0x00, 0x01, b'k', 0x00, 0x02, b'v', b'w']);
        assert_eq!(
            Reader::new(&good).string_pair("User Property"),
            Ok(("k".to_owned(), "vw".to_owned()))
        );
        let null_value = Bytes::from_static(&[0x00, 0x01, b'k', 0x00, 0x01, 0x00]);
        assert_eq!(
            Reader::new(&null_value).string_pair("User Property"),
            Err(Error::NullCharacter {
                field: "User Property"
            })
        );
        let bad_name = Bytes::from_static(&[0x00, 0x01, 0xFF, 0x00, 0x00]);
        assert_eq!(
            Reader::new(&bad_name).string_pair("User Property"),
            Err(Error::InvalidUtf8 {
                field: "User Property"
            })
        );
    }

    #[test]
    fn a_field_running_past_the_packet_is_truncated() {
        for wire in [&[0x00][..], &[0x00, 0x02, b'a']] {
            let buf = Bytes::copy_from_slice(wire);
            assert_eq!(
                Reader::new(&buf).string("Will Topic"),
                Err(Error::Truncated {
                    field: "Will Topic"
                }),
                "{wire:02X?}"
            );
        }
        let buf = Bytes::from_static(&[0x00, 0x05, 0x01]);
        assert_eq!(
            Reader::new(&buf).binary("Will Payload"),
            Err(Error::Truncated {
                field: "Will Payload"
            })
        );
        let buf = Bytes::from_static(&[0x80, 0x80]);
        assert_eq!(
            Reader::new(&buf).variable_byte_integer("Property Length"),
            Err(Error::Truncated {
                field: "Property Length"
            })
        );
    }

    #[test]
    fn a_taken_reader_stops_at_its_length() {
        let buf = Bytes::from_static(&[0x01, 0x02, 0x03, 0x04]);
        let mut outer = Reader::new(&buf);
        let mut inner = outer.take(2, "Properties").unwrap();
        assert_eq!(inner.remaining(), 2);
        assert_eq!(inner.u16("Receive Maximum"), Ok(0x0102));
        assert_eq!(
            inner.u8("Maximum QoS"),
            Err(Error::Truncated {
                field: "Maximum QoS"
            })
        );
        assert_eq!(outer.u16("Packet Identifier"), Ok(0x0304));
        assert_eq!(
            outer.take(1, "Properties").map(|r| r.remaining()),
            Err(Error::Truncated {
                field: "Properties"
            })
        );
    }
}
