//! Debug formatting for fields that carry credentials.

use bytes::Bytes;

/// Formats an optional credential as its presence and length only.
pub(crate) struct Redacted<'a>(pub(crate) &'a Option<Bytes>);

impl core::fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(value) => write!(f, "Some(<redacted, {} bytes>)", value.len()),
            None => f.write_str("None"),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{AuthProperties, ConnAckProperties, Connect, ConnectProperties};

    #[test]
    fn credentials_never_appear_in_debug_output() {
        let secret = Bytes::from_static(b"hunter2-secret");
        let connect = Connect {
            username: Some("device".into()),
            password: Some(secret.clone()),
            properties: ConnectProperties {
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(secret.clone()),
                ..ConnectProperties::default()
            },
            ..Connect::default()
        };
        let auth = AuthProperties {
            authentication_data: Some(secret.clone()),
            ..AuthProperties::default()
        };
        let connack = ConnAckProperties {
            authentication_data: Some(secret.clone()),
            ..ConnAckProperties::default()
        };
        for text in [
            format!("{connect:?}"),
            format!("{auth:?}"),
            format!("{connack:?}"),
        ] {
            assert!(!text.contains("hunter2"), "{text}");
            assert!(!text.contains("104, 117, 110"), "{text}");
            assert!(text.contains("<redacted, 14 bytes>"), "{text}");
        }
        assert!(format!("{connect:?}").contains("device"));
    }
}
