//! Destination sets: the ids that want one filter, or the members of one shared group.
//!
//! Report R3 specifies small sorted vectors promoted to roaring bitmaps above 64 entries. Most
//! filters have exactly one destination, a device's own command filter on its edge, so up to
//! three ids live inline and cost no allocation, and a set is 16 bytes (report R6, D1). Every
//! form keeps its ids sorted and unique.

use std::mem::size_of;

use roaring::RoaringBitmap;

/// Above this many entries a set becomes a roaring bitmap (R3).
pub(crate) const PROMOTE: usize = 64;
/// A bitmap that shrinks to this many entries becomes a vector again. Lower than [`PROMOTE`],
/// so a set hovering at the boundary does not convert on every change (R6, D1).
pub(crate) const DEMOTE: usize = 32;
/// Ids held inline.
pub(crate) const INLINE: usize = 3;

#[derive(Clone, Debug)]
pub(crate) enum DestSet {
    Inline {
        len: u8,
        ids: [u32; INLINE],
    },
    #[expect(
        clippy::box_collection,
        reason = "a boxed vector keeps the set at 16 bytes, where a bare one would take 32"
    )]
    Small(Box<Vec<u32>>),
    Big(Box<RoaringBitmap>),
}

impl Default for DestSet {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            ids: [0; INLINE],
        }
    }
}

impl DestSet {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => usize::from(*len),
            Self::Small(ids) => ids.len(),
            // A bitmap holds at most 2^32 ids, which a usize holds wherever this compiles.
            Self::Big(ids) => usize::try_from(ids.len()).unwrap_or(usize::MAX),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The inline ids, or a vector's.
    fn slice(&self) -> Option<&[u32]> {
        match self {
            Self::Inline { len, ids } => ids.get(..usize::from(*len)),
            Self::Small(ids) => Some(ids),
            Self::Big(_) => None,
        }
    }

    pub(crate) fn contains(&self, id: u32) -> bool {
        match self {
            Self::Big(ids) => ids.contains(id),
            _ => self
                .slice()
                .is_some_and(|ids| ids.binary_search(&id).is_ok()),
        }
    }

    /// Adds `id`; false if it was there already.
    pub(crate) fn insert(&mut self, id: u32) -> bool {
        match self {
            Self::Inline { len, ids } => {
                let n = usize::from(*len);
                let Err(at) = ids[..n].binary_search(&id) else {
                    return false;
                };
                if n < INLINE {
                    ids.copy_within(at..n, at + 1);
                    ids[at] = id;
                    *len += 1;
                } else {
                    let mut grown = Vec::with_capacity(2 * INLINE);
                    grown.extend_from_slice(&ids[..at]);
                    grown.push(id);
                    grown.extend_from_slice(&ids[at..]);
                    *self = Self::Small(Box::new(grown));
                }
                true
            }
            Self::Small(ids) => {
                let Err(at) = ids.binary_search(&id) else {
                    return false;
                };
                ids.insert(at, id);
                if ids.len() > PROMOTE {
                    let bitmap: RoaringBitmap = ids.iter().copied().collect();
                    *self = Self::Big(Box::new(bitmap));
                }
                true
            }
            Self::Big(ids) => ids.insert(id),
        }
    }

    /// Removes `id`; false if it was not there.
    pub(crate) fn remove(&mut self, id: u32) -> bool {
        match self {
            Self::Inline { len, ids } => {
                let n = usize::from(*len);
                let Ok(at) = ids[..n].binary_search(&id) else {
                    return false;
                };
                ids.copy_within(at + 1..n, at);
                *len -= 1;
                true
            }
            Self::Small(ids) => {
                let Ok(at) = ids.binary_search(&id) else {
                    return false;
                };
                ids.remove(at);
                if let Some(fit) = inline(ids) {
                    *self = fit;
                }
                true
            }
            Self::Big(ids) => {
                if !ids.remove(id) {
                    return false;
                }
                if ids.len() <= DEMOTE as u64 {
                    *self = Self::Small(Box::new(ids.iter().collect()));
                }
                true
            }
        }
    }

    /// The id at `index` in ascending order.
    pub(crate) fn get(&self, index: usize) -> Option<u32> {
        match self {
            Self::Big(ids) => ids.select(u32::try_from(index).ok()?),
            _ => self.slice()?.get(index).copied(),
        }
    }

    /// Every id, ascending.
    pub(crate) fn iter(&self) -> Iter<'_> {
        match self {
            Self::Big(ids) => Iter::Bitmap(ids.iter()),
            _ => Iter::Slice(self.slice().unwrap_or_default().iter()),
        }
    }

    pub(crate) fn as_bitmap(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Big(ids) => Some(ids),
            _ => None,
        }
    }

    /// Heap bytes outside the set itself. A bitmap's are estimated from its containers.
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Inline { .. } => 0,
            Self::Small(ids) => size_of::<Vec<u32>>() + ids.capacity() * size_of::<u32>(),
            Self::Big(ids) => {
                let stats = ids.statistics();
                let containers = stats.n_bytes_array_containers
                    + stats.n_bytes_run_containers
                    + stats.n_bytes_bitset_containers;
                size_of::<RoaringBitmap>()
                    + usize::try_from(containers).unwrap_or(usize::MAX)
                    + usize::try_from(stats.n_containers).unwrap_or(usize::MAX) * 32
            }
        }
    }
}

/// The inline form of a vector that fits in one.
fn inline(ids: &[u32]) -> Option<DestSet> {
    let len = u8::try_from(ids.len()).ok()?;
    let mut inline = [0; INLINE];
    inline.get_mut(..ids.len())?.copy_from_slice(ids);
    Some(DestSet::Inline { len, ids: inline })
}

/// The ids of a set, ascending.
pub(crate) enum Iter<'a> {
    Slice(std::slice::Iter<'a, u32>),
    Bitmap(roaring::bitmap::Iter<'a>),
}

impl Iterator for Iter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        match self {
            Self::Slice(ids) => ids.next().copied(),
            Self::Bitmap(ids) => ids.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Slice(ids) => ids.size_hint(),
            Self::Bitmap(ids) => ids.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_set_is_sixteen_bytes() {
        assert_eq!(size_of::<DestSet>(), 16);
    }

    #[test]
    fn grows_through_every_form_and_back() {
        let mut set = DestSet::default();
        for id in (0..200u32).rev() {
            assert!(set.insert(id * 3));
            assert!(!set.insert(id * 3));
            let len = 200 - usize::try_from(id).unwrap();
            assert_eq!(set.len(), len);
            match len {
                0..=INLINE => assert!(matches!(set, DestSet::Inline { .. })),
                4..=PROMOTE => assert!(matches!(set, DestSet::Small(_))),
                _ => assert!(matches!(set, DestSet::Big(_))),
            }
        }
        let all: Vec<u32> = set.iter().collect();
        assert_eq!(all, (0..200u32).map(|id| id * 3).collect::<Vec<_>>());
        assert_eq!(set.get(0), Some(0));
        assert_eq!(set.get(199), Some(597));
        assert_eq!(set.get(200), None);
        assert!(set.contains(597) && !set.contains(596));
        assert!(set.heap_bytes() > 0);

        for id in 0..199u32 {
            assert!(set.remove(id * 3));
            assert!(!set.remove(id * 3));
            let len = usize::try_from(199 - id).unwrap();
            match len {
                0..=INLINE => assert!(matches!(set, DestSet::Inline { .. })),
                4..=DEMOTE => assert!(matches!(set, DestSet::Small(_))),
                _ => assert!(matches!(set, DestSet::Big(_))),
            }
        }
        assert_eq!(set.iter().collect::<Vec<_>>(), [597]);
        assert_eq!(set.heap_bytes(), 0);
        assert!(set.remove(597));
        assert!(set.is_empty());
        assert_eq!(set.get(0), None);
    }

    #[test]
    fn a_bitmap_comes_back_to_a_vector_only_at_32() {
        let mut set = DestSet::default();
        for id in 0..65u32 {
            set.insert(id);
        }
        assert!(matches!(set, DestSet::Big(_)));
        // Down to 33 it stays a bitmap; at 32 it is a vector again.
        for id in 0..32u32 {
            set.remove(id);
        }
        assert!(matches!(set, DestSet::Big(_)));
        set.remove(32);
        assert!(matches!(set, DestSet::Small(_)));
        assert_eq!(set.iter().collect::<Vec<_>>(), (33..65).collect::<Vec<_>>());
    }
}
