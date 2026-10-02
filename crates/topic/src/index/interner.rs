//! Level strings, each stored once in one arena, with reference counts so that a level no node
//! or shared group uses any more is freed.

use std::hash::BuildHasher;
use std::mem::size_of;

use foldhash::fast::RandomState;
use hashbrown::HashTable;

use super::{MAX_ID, NIL};
use crate::Error;

/// Freed text in the arena that sets off a compaction: at least this many bytes, and half of
/// the arena.
const COMPACT_AT: usize = 1 << 20;

/// Where a level's text is in the arena, and how many nodes and groups use it.
#[derive(Clone, Copy, Debug)]
struct Span {
    off: u32,
    len: u32,
    refs: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct Interner {
    arena: String,
    spans: Vec<Span>,
    /// Level ids, found by their text's hash and compared against the arena.
    table: HashTable<u32>,
    free: Vec<u32>,
    /// Bytes of the arena that belong to freed levels.
    garbage: usize,
    state: RandomState,
}

impl Interner {
    pub(crate) fn new(state: RandomState) -> Self {
        Self {
            arena: String::new(),
            spans: Vec::new(),
            table: HashTable::new(),
            free: Vec::new(),
            garbage: 0,
            state,
        }
    }

    fn text<'a>(arena: &'a str, span: &Span) -> &'a str {
        let start = span.off as usize;
        &arena[start..start + span.len as usize]
    }

    /// The text of a live level.
    pub(crate) fn resolve(&self, id: u32) -> &str {
        Self::text(&self.arena, &self.spans[id as usize])
    }

    pub(crate) fn get(&self, level: &str) -> Option<u32> {
        let hash = self.state.hash_one(level);
        let (arena, spans) = (&self.arena, &self.spans);
        self.table
            .find(hash, |&id| Self::text(arena, &spans[id as usize]) == level)
            .copied()
    }

    /// The id of `level`, adding a reference to it.
    pub(crate) fn intern(&mut self, level: &str) -> Result<u32, Error> {
        let hash = self.state.hash_one(level);
        let (arena, spans) = (&self.arena, &mut self.spans);
        if let Some(&id) = self
            .table
            .find(hash, |&id| Self::text(arena, &spans[id as usize]) == level)
        {
            spans[id as usize].refs += 1;
            return Ok(id);
        }
        if self.free.is_empty() && self.spans.len() > MAX_ID as usize {
            return Err(Error::IndexFull);
        }
        if self.arena.len() + level.len() > u32::MAX as usize && self.garbage > 0 {
            self.compact();
        }
        let (Ok(off), Ok(len)) = (u32::try_from(self.arena.len()), u32::try_from(level.len()))
        else {
            return Err(Error::IndexFull);
        };
        if off.checked_add(len).is_none() {
            return Err(Error::IndexFull);
        }
        self.arena.push_str(level);
        let span = Span { off, len, refs: 1 };
        let id = match self.free.pop() {
            Some(id) => {
                self.spans[id as usize] = span;
                id
            }
            None => {
                self.spans.push(span);
                // Below MAX_ID, checked above.
                u32::try_from(self.spans.len() - 1).unwrap_or(NIL)
            }
        };
        let (arena, spans, state) = (&self.arena, &self.spans, &self.state);
        self.table.insert_unique(hash, id, |&id| {
            state.hash_one(Self::text(arena, &spans[id as usize]))
        });
        Ok(id)
    }

    /// Drops a reference to a live level, freeing it with the last one.
    pub(crate) fn release(&mut self, id: u32) {
        let span = &mut self.spans[id as usize];
        span.refs -= 1;
        if span.refs > 0 {
            return;
        }
        let hash = self.state.hash_one(self.resolve(id));
        if let Ok(entry) = self.table.find_entry(hash, |&x| x == id) {
            entry.remove();
        }
        let span = &mut self.spans[id as usize];
        self.garbage += span.len as usize;
        *span = Span {
            off: 0,
            len: 0,
            refs: 0,
        };
        self.free.push(id);
        if self.garbage >= COMPACT_AT && 2 * self.garbage >= self.arena.len() {
            self.compact();
        }
    }

    /// Live levels.
    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    /// Copies the live levels into a fresh arena, leaving the text of freed ones behind. Ids do
    /// not change.
    fn compact(&mut self) {
        let mut arena = String::with_capacity(self.arena.len() - self.garbage);
        for span in &mut self.spans {
            if span.refs == 0 {
                continue;
            }
            let text = Self::text(&self.arena, span);
            // The live text fits where it was.
            span.off = u32::try_from(arena.len()).unwrap_or(NIL);
            arena.push_str(text);
        }
        self.arena = arena;
        self.garbage = 0;
    }

    /// Numbers the live levels from 0 in their current order, packs their text, and returns
    /// each old id's new one, [`NIL`] for a freed level.
    pub(crate) fn renumber(&mut self) -> Vec<u32> {
        let live = self.spans.len() - self.free.len();
        let mut map = vec![NIL; self.spans.len()];
        let mut arena = String::with_capacity(self.arena.len() - self.garbage);
        let mut spans = Vec::with_capacity(live);
        for (old, span) in self.spans.iter().enumerate() {
            if span.refs == 0 {
                continue;
            }
            // Both counts shrink, so both still fit.
            let new = u32::try_from(spans.len()).unwrap_or(NIL);
            let off = u32::try_from(arena.len()).unwrap_or(NIL);
            arena.push_str(Self::text(&self.arena, span));
            spans.push(Span { off, ..*span });
            map[old] = new;
        }
        let mut table = HashTable::with_capacity(spans.len());
        for (id, span) in spans.iter().enumerate() {
            let hash = self.state.hash_one(Self::text(&arena, span));
            let id = u32::try_from(id).unwrap_or(NIL);
            table.insert_unique(hash, id, |&id| {
                self.state.hash_one(Self::text(&arena, &spans[id as usize]))
            });
        }
        self.arena = arena;
        self.spans = spans;
        self.table = table;
        self.free = Vec::new();
        self.garbage = 0;
        map
    }

    /// Bytes held by the arena, the spans, the table and the free list.
    pub(crate) fn memory(&self) -> (usize, usize, usize, usize) {
        (
            self.arena.capacity(),
            self.spans.capacity() * size_of::<Span>(),
            self.table.allocation_size(),
            self.free.capacity() * size_of::<u32>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_level_is_stored_once_and_freed_with_its_last_reference() {
        let mut levels = Interner::new(RandomState::default());
        let a = levels.intern("commands").unwrap();
        assert_eq!(levels.intern("commands").unwrap(), a);
        let b = levels.intern("").unwrap();
        assert_ne!(a, b);
        assert_eq!(levels.get("commands"), Some(a));
        assert_eq!(levels.get(""), Some(b));
        assert_eq!(levels.get("telemetry"), None);
        assert_eq!(levels.resolve(a), "commands");
        assert_eq!(levels.len(), 2);

        levels.release(a);
        assert_eq!(levels.get("commands"), Some(a));
        levels.release(a);
        assert_eq!(levels.get("commands"), None);
        assert_eq!(levels.len(), 1);
        // A freed id is used again.
        assert_eq!(levels.intern("events").unwrap(), a);
        assert_eq!(levels.resolve(a), "events");
    }

    #[test]
    fn freed_text_is_compacted_away() {
        let mut levels = Interner::new(RandomState::default());
        let keep = levels.intern("keep").unwrap();
        let big = "x".repeat(COMPACT_AT);
        let gone = levels.intern(&big).unwrap();
        levels.release(gone);
        // The arena dropped the freed text when it became most of it.
        assert_eq!(levels.garbage, 0);
        assert_eq!(levels.arena, "keep");
        assert_eq!(levels.resolve(keep), "keep");
        assert_eq!(levels.get("keep"), Some(keep));
    }

    #[test]
    fn renumbering_packs_the_ids() {
        let mut levels = Interner::new(RandomState::default());
        let ids: Vec<u32> = ["a", "b", "c", "d"]
            .iter()
            .map(|l| levels.intern(l).unwrap())
            .collect();
        levels.release(ids[0]);
        levels.release(ids[2]);
        let map = levels.renumber();
        assert_eq!(map, [NIL, 0, NIL, 1]);
        assert_eq!(levels.get("b"), Some(0));
        assert_eq!(levels.get("d"), Some(1));
        assert_eq!(levels.resolve(1), "d");
        assert_eq!(levels.arena, "bd");
        assert_eq!(levels.intern("e").unwrap(), 2);
    }
}
