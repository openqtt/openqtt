//! Property tests over the public API.
//!
//! - A Client Identifier is accepted exactly when it is 1 to 256 bytes without U+0000.
//! - Assigned identifiers are 23 characters from `0-9a-zA-Z`, one for each value below 62^21.
//! - A session's partition is below the partition count, and depends on nothing but the
//!   identifier and the count.

use std::num::NonZeroU32;

use openqtt_core::{ClientId, partition_of};
use proptest::prelude::*;

/// 62^21, the number of assigned identifiers.
const SPACE: u128 = 62u128.pow(21);

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn client_identifiers_are_1_to_256_bytes_without_the_null_character(text in ".{0,300}") {
        let valid = !text.is_empty() && text.len() <= 256 && !text.contains('\0');
        prop_assert_eq!(ClientId::new(&text).is_ok(), valid);
    }

    #[test]
    fn assigned_identifiers_are_distinct_below_the_space(a in 0..SPACE, b in 0..SPACE) {
        let (x, y) = (ClientId::assigned(a), ClientId::assigned(b));
        prop_assert_eq!(x == y, a == b);
        prop_assert_eq!(x.as_str().len(), 23);
        prop_assert!(x.as_str().starts_with("oq"));
        prop_assert!(x.as_str().bytes().all(|b| b.is_ascii_alphanumeric()));
        prop_assert_eq!(ClientId::new(x.as_str()), Ok(x.clone()));
        // Above the space, values wrap round to the same identifiers.
        if let Some(wrapped) = a.checked_add(SPACE) {
            prop_assert_eq!(ClientId::assigned(wrapped), x);
        }
    }

    #[test]
    fn a_partition_is_below_the_count(random in any::<u128>(), count in 1..=u32::MAX) {
        let id = ClientId::assigned(random);
        let count = NonZeroU32::new(count).expect("not zero");
        let partition = partition_of(&id, count);
        prop_assert!(partition < count.get());
        prop_assert_eq!(partition_of(&id.clone(), count), partition);
    }
}
