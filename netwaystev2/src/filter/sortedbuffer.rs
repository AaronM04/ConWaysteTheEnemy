///! A sorted buffer which maintains ordering and uniqueness in the buffer.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct SequencedMinHeap<T> {
    heap: BinaryHeap<Reverse<SequencedItem<T>>>,
}

impl<T> SequencedMinHeap<T> {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    fn contains_sequence_number(&self, sequence: u64) -> bool {
        for tuple in &self.heap {
            if tuple.0.0 == sequence {
                return true
            }
        }
        false
    }

    /// Add this T to the sequenced min-heap. Returns false if not added because
    /// this sequence is already in the min-heap; otherwise, returns true.
    pub fn add(&mut self, sequence: u64, t: T) -> bool {
        if self.contains_sequence_number(sequence) {
            return false;
        }
        self.heap.push(Reverse(SequencedItem(sequence, t)));
        true
    }

    /// Gets the minimum sequence number in the min-heap. This is the sequence number of
    /// what we would .take()
    pub fn peek_sequence_number(&self) -> Option<u64> {
        self.heap.peek().map(|reversed_tup| reversed_tup.0.0)
    }

    /// Takes the T with the lowest sequence number
    pub fn take(&mut self) -> Option<T> {
        self.heap.pop().map(|reversed_tup| reversed_tup.0.1)
    }
}

struct SequencedItem<T>(u64, T);

impl<T> Ord for SequencedItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T> PartialEq for SequencedItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> PartialOrd for SequencedItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Marker trait impl for Ord without needing to pass `Eq` down to the item
impl<T> Eq for SequencedItem<T> {
}
