//! Destination sets: who wants a filter.
//!
//! R3 specifies small sorted vectors promoted to roaring bitmaps above 64 entries. Most filters
//! have exactly one destination (a device's own command filter, held by one edge), so the first
//! four entries live inline and cost no allocation of their own.

use roaring::RoaringBitmap;

/// Above this many entries a set becomes a roaring bitmap.
pub const PROMOTE: usize = 64;
/// A bitmap that shrinks to this many entries becomes a vector again. Lower than `PROMOTE`, so
/// a set hovering at the boundary does not convert on every change.
pub const DEMOTE: usize = 32;
const INLINE: usize = 4;

pub enum DestSet {
    Inline { len: u8, v: [u32; INLINE] },
    Small(Vec<u32>),
    Big(Box<RoaringBitmap>),
}

impl Default for DestSet {
    fn default() -> Self {
        DestSet::Inline {
            len: 0,
            v: [0; INLINE],
        }
    }
}

impl DestSet {
    pub fn len(&self) -> usize {
        match self {
            DestSet::Inline { len, .. } => usize::from(*len),
            DestSet::Small(v) => v.len(),
            DestSet::Big(b) => usize::try_from(b.len()).unwrap_or(usize::MAX),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds `d`; false if it was already there.
    pub fn insert(&mut self, d: u32) -> bool {
        match self {
            DestSet::Inline { len, v } => {
                let n = usize::from(*len);
                let at = match v[..n].binary_search(&d) {
                    Ok(_) => return false,
                    Err(at) => at,
                };
                if n < INLINE {
                    v.copy_within(at..n, at + 1);
                    v[at] = d;
                    *len += 1;
                } else {
                    let mut s = Vec::with_capacity(INLINE * 2);
                    s.extend_from_slice(&v[..at]);
                    s.push(d);
                    s.extend_from_slice(&v[at..n]);
                    *self = DestSet::Small(s);
                }
                true
            }
            DestSet::Small(s) => match s.binary_search(&d) {
                Ok(_) => false,
                Err(at) => {
                    s.insert(at, d);
                    if s.len() > PROMOTE {
                        let b: RoaringBitmap = s.iter().copied().collect();
                        *self = DestSet::Big(Box::new(b));
                    }
                    true
                }
            },
            DestSet::Big(b) => b.insert(d),
        }
    }

    /// Removes `d`; false if it was not there.
    pub fn remove(&mut self, d: u32) -> bool {
        match self {
            DestSet::Inline { len, v } => {
                let n = usize::from(*len);
                match v[..n].binary_search(&d) {
                    Ok(at) => {
                        v.copy_within(at + 1..n, at);
                        *len -= 1;
                        true
                    }
                    Err(_) => false,
                }
            }
            DestSet::Small(s) => match s.binary_search(&d) {
                Ok(at) => {
                    s.remove(at);
                    if s.len() <= INLINE {
                        let mut v = [0; INLINE];
                        v[..s.len()].copy_from_slice(s);
                        let len = u8::try_from(s.len()).unwrap_or(0);
                        *self = DestSet::Inline { len, v };
                    }
                    true
                }
                Err(_) => false,
            },
            DestSet::Big(b) => {
                let gone = b.remove(d);
                if gone && b.len() <= DEMOTE as u64 {
                    *self = DestSet::Small(b.iter().collect());
                }
                gone
            }
        }
    }

    /// Appends every destination to `out`, ascending.
    pub fn extend_into(&self, out: &mut Vec<u32>) {
        match self {
            DestSet::Inline { len, v } => out.extend_from_slice(&v[..usize::from(*len)]),
            DestSet::Small(s) => out.extend_from_slice(s),
            DestSet::Big(b) => out.extend(b.iter()),
        }
    }

    /// The destination at position `h` modulo the size: one member of a shared group.
    pub fn pick(&self, h: u64) -> Option<u32> {
        let n = self.len() as u64;
        if n == 0 {
            return None;
        }
        let i = h % n;
        match self {
            DestSet::Inline { v, .. } => v.get(usize::try_from(i).ok()?).copied(),
            DestSet::Small(s) => s.get(usize::try_from(i).ok()?).copied(),
            DestSet::Big(b) => b.select(u32::try_from(i).ok()?),
        }
    }

    pub fn as_bitmap(&self) -> Option<&RoaringBitmap> {
        match self {
            DestSet::Big(b) => Some(b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_through_every_representation_and_back() {
        let mut s = DestSet::default();
        for d in (0..200u32).rev() {
            assert!(s.insert(d * 3));
            assert!(!s.insert(d * 3));
        }
        assert!(matches!(s, DestSet::Big(_)));
        assert_eq!(s.len(), 200);
        let mut out = Vec::new();
        s.extend_into(&mut out);
        assert_eq!(out, (0..200u32).map(|d| d * 3).collect::<Vec<_>>());
        for d in 0..199u32 {
            assert!(s.remove(d * 3));
            assert!(!s.remove(d * 3));
        }
        assert!(matches!(s, DestSet::Inline { len: 1, .. }));
        assert_eq!(s.pick(7), Some(597));
        assert!(s.remove(597));
        assert!(s.is_empty());
    }
}
