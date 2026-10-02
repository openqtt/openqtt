//! Decode, re-encode, decode again: the two decodings must be the same packet.
//!
//! Every packet the decoder takes from arbitrary bytes is encoded and decoded again. The
//! second decoding must equal the first and consume exactly the encoding, and encoding it
//! again must give the same bytes: the codec's encoding of a packet is its one canonical form,
//! even where the input used a longer form the specification allows.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use openqtt_codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let decoder = Decoder::new();
    let mut src = BytesMut::from(data);
    while let Ok(Some(packet)) = decoder.decode(&mut src) {
        let mut encoded = BytesMut::new();
        packet
            .encode(&mut encoded)
            .expect("a decoded packet encodes");
        let canonical = encoded.clone().freeze();

        let again = decoder
            .decode(&mut encoded)
            .expect("an encoded packet decodes")
            .expect("an encoded packet is whole");
        assert!(encoded.is_empty(), "the encoding is exactly one packet");
        assert_eq!(again, packet, "decode, encode, decode is the same packet");

        let mut reencoded = BytesMut::new();
        again.encode(&mut reencoded).expect("it encodes again");
        assert_eq!(reencoded.freeze(), canonical, "the encoding is canonical");
    }
});
