use std::collections::VecDeque;

/// Bounded FIFO buffer: pushing past `capacity` evicts the oldest item. Used by tail-mode
/// message browsing only — seek mode owns its own bounded `Vec` per PLAN.md, so this never
/// needs to reconcile state with anything else.
pub struct RingBuffer<T> {
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "RingBuffer capacity must be > 0");
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, item: T) {
        if self.items.len() == self.capacity {
            self.items.pop_front();
        }
        self.items.push_back(item);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_under_capacity_keeps_everything_in_order() {
        let mut buf = RingBuffer::new(3);
        buf.push(1);
        buf.push(2);
        assert_eq!(buf.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn push_past_capacity_evicts_oldest() {
        let mut buf = RingBuffer::new(3);
        buf.push(1);
        buf.push(2);
        buf.push(3);
        buf.push(4);
        assert_eq!(buf.iter().copied().collect::<Vec<_>>(), vec![2, 3, 4]);
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn capacity_boundary_exact_fit_does_not_evict() {
        let mut buf = RingBuffer::new(2);
        buf.push(1);
        buf.push(2);
        assert_eq!(buf.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn empty_buffer_behaves_safely() {
        let buf: RingBuffer<i32> = RingBuffer::new(5);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.iter().count(), 0);
    }

    #[test]
    fn clear_empties_without_changing_capacity() {
        let mut buf = RingBuffer::new(3);
        buf.push(1);
        buf.push(2);
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 3);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_panics_on_construction() {
        let _: RingBuffer<i32> = RingBuffer::new(0);
    }
}
