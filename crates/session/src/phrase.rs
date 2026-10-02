//! The Reason Strings the server sends (report R1, O20): a fixed phrase for each refusal,
//! naming the reason and no rule or internal name. Human readable, never for parsing.

/// The phrase for a failure reason code, or `None` for a code that reports no failure.
pub(crate) fn phrase(code: u8) -> Option<String> {
    let text = match code {
        0x80 => "unspecified error",
        0x81 => "malformed packet",
        0x82 => "protocol error",
        0x83 => "not accepted by this server",
        0x85 => "client identifier not valid",
        0x86 => "bad user name or password",
        0x87 => "not authorized",
        0x88 => "server unavailable",
        0x89 => "server busy",
        0x8A => "banned",
        0x8B => "server shutting down",
        0x8C => "bad authentication method",
        0x8D => "keep alive timeout",
        0x8E => "session taken over",
        0x8F => "topic filter invalid",
        0x90 => "topic name invalid",
        0x92 => "packet identifier not found",
        0x93 => "receive maximum exceeded",
        0x94 => "topic alias invalid",
        0x95 => "packet too large",
        0x97 => "quota exceeded",
        0x98 => "administrative action",
        0x9A => "retain not supported",
        0x9B => "qos not supported",
        0x9C => "use another server",
        0x9D => "server moved",
        0x9E => "shared subscriptions not supported",
        0xA1 => "subscription identifiers not supported",
        0xA2 => "wildcard subscriptions not supported",
        _ => return None,
    };
    Some(text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r1_o20_every_failure_the_server_sends_has_a_phrase_and_success_none() {
        for code in [
            0x80, 0x81, 0x82, 0x83, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E,
            0x8F, 0x90, 0x92, 0x93, 0x94, 0x95, 0x97, 0x98, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0xA1,
            0xA2,
        ] {
            let text = phrase(code).unwrap();
            assert!(!text.is_empty() && text.is_ascii(), "{code:#04x}");
        }
        for code in [0x00, 0x01, 0x02, 0x04, 0x10, 0x11, 0x18, 0x19] {
            assert_eq!(phrase(code), None, "{code:#04x}");
        }
    }
}
