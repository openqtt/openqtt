//! Arbitrary bytes into the decoder, as one connection's stream.
//!
//! The decoder must take a packet, ask for more, or report an error, and never panic. A packet
//! it takes must be one that encodes, to exactly the bytes `encoded_len` says. The first byte
//! picks a decoder: with or without a small Maximum Packet Size, and told the sender or not, so
//! the early size check and the sender rules are fuzzed too.

#![no_main]

use std::num::NonZeroU32;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use openqtt_codec::{Decoder, Sender};

fuzz_target!(|data: &[u8]| {
    let Some((&config, stream)) = data.split_first() else {
        return;
    };
    let mut decoder = Decoder::new();
    if config & 0x80 != 0 {
        let maximum = NonZeroU32::new(u32::from(config & 0x3F) + 2).expect("at least 2");
        decoder = decoder.with_max_packet_size(maximum);
    }
    match config & 0x03 {
        1 => decoder = decoder.with_sender(Sender::Client),
        2 => decoder = decoder.with_sender(Sender::Server),
        _ => {}
    }

    let mut src = BytesMut::from(stream);
    while let Ok(Some(packet)) = decoder.decode(&mut src) {
        let len = packet
            .encoded_len()
            .expect("a decoded packet has an encoding");
        let mut dst = BytesMut::new();
        packet.encode(&mut dst).expect("a decoded packet encodes");
        assert_eq!(dst.len(), len, "encoded_len is exact");
        let _ = packet.check_sender(Sender::Client);
        let _ = packet.check_sender(Sender::Server);
    }
});
