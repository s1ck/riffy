use core::ptr;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

use crate::State::{Empty, Handled, Set};

/// The number of nodes within a single buffer.
const BUFFER_SIZE: usize = 1620;

/// Represents the state of a node within a buffer.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum State {
    /// Initial state, the node contains no data.
    Empty,
    /// The enqueue process was successful, the node contains data.
    Set,
    /// The dequeue process was successful, the node contains no data.
    Handled,
}

impl From<State> for u8 {
    fn from(state: State) -> Self {
        state as u8
    }
}

impl From<u8> for State {
    fn from(state: u8) -> Self {
        match state {
            0 => State::Empty,
            1 => State::Set,
            2 => State::Handled,
            _ => unreachable!(),
        }
    }
}

impl PartialEq<State> for u8 {
    fn eq(&self, other: &State) -> bool {
        *self == *other as u8
    }
}

/// A node is contained in a `BufferList` and owns
/// the actual data that has been enqueued. A node
/// has a state which is updated during enqueue and
/// dequeue operations.
struct Node<T> {
    data: MaybeUninit<T>,
    /// The state of the node needs to be atomic to make
    /// state changes visible to the dequeue thread.
    is_set: AtomicU8,
}

impl<T> Node<T> {
    /// Load the given data into the node and change its state to `Set`.
    unsafe fn set_data(&mut self, data: T) {
        self.data.as_mut_ptr().write(data);
        self.is_set.store(State::Set.into(), Relaxed);
    }

    fn state(&self) -> State {
        self.is_set.load(Acquire).into()
    }

    fn set_state(&mut self, state: State) {
        self.is_set.store(state.into(), Release)
    }
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Node {
            data: MaybeUninit::uninit(),
            is_set: AtomicU8::new(State::Empty.into()),
        }
    }
}

/// The buffer list holds a fixed number of nodes.
/// Buffer lists are connected with each other and
/// form a linked list. Enqueue operations always
/// append to the linked list by creating a new buffer
/// and atomically updating the `next` pointer of the
/// last buffer list in the queue.
struct BufferList<T> {
    /// A fixed size vector of nodes that hold the data.
    nodes: Vec<Node<T>>,
    /// A pointer to the previous buffer list.
    prev: *mut BufferList<T>,
    /// An atomic pointer to the next buffer list.
    next: AtomicPtr<BufferList<T>>,
    /// The position to read the next element from inside
    /// the buffer list. The head index is only updated
    /// by the dequeue thread.
    head: usize,
    /// The position of that buffer list in the queue.
    /// That index is used to compute the number of elements
    /// previously added to the queue.
    pos: usize,
}

impl<T> BufferList<T> {
    fn new(size: usize, position_in_queue: usize) -> Self {
        BufferList::with_prev(size, position_in_queue, ptr::null_mut())
    }

    fn with_prev(size: usize, pos: usize, prev: *mut BufferList<T>) -> Self {
        let mut curr_buffer = Vec::with_capacity(size);
        curr_buffer.resize_with(size, Node::default);

        BufferList {
            nodes: curr_buffer,
            prev,
            next: AtomicPtr::new(ptr::null_mut()),
            head: 0,
            pos,
        }
    }
}

/// A Multi-Producer, Single-Consumer Queue implementation as described in
/// https://arxiv.org/pdf/2010.14189.pdf.
///
/// The queue is unbounded and wait-free.
pub struct MpscQueue<T> {
    head_of_queue: *mut BufferList<T>,
    tail_of_queue: AtomicPtr<BufferList<T>>,

    buffer_size: usize,
    tail: AtomicUsize,
}

impl<T> MpscQueue<T> {

    pub fn new() -> Self {
        let head_of_queue = BufferList::new(BUFFER_SIZE, 1);
        // move to heap
        let head = Box::new(head_of_queue);
        // forget about it but keep pointer
        let head = Box::into_raw(head);
        // make pointer atomic
        let tail = AtomicPtr::new(head);

        MpscQueue {
            head_of_queue: head,
            tail_of_queue: tail,
            buffer_size: BUFFER_SIZE,
            tail: AtomicUsize::new(0),
        }
    }

    pub fn enqueue(&mut self, data: T) -> Result<(), T> {
        // Retrieve an index where we insert the new element.
        // Since this is called by multiple enqueue threads,
        // the generated index can be either past or before
        // the current tail buffer of the queue.
        let location = self.tail.fetch_add(1, Ordering::SeqCst);

        // The buffer in which we eventually insert into.
        let mut temp_tail;
        // Track if the element is inserted in the last buffer.
        let mut is_last_buffer = true;

        loop {
            temp_tail = unsafe { &mut *self.tail_of_queue.load(Ordering::Acquire) };
            // The number of items in the queue without the current buffer.
            let mut prev_size = self.size_without_buffer(temp_tail);

            // The location is in a previous buffer. We need to track back to that one.
            while location < prev_size {
                is_last_buffer = false;
                temp_tail = unsafe { &mut *temp_tail.prev };
                prev_size -= self.buffer_size;
            }

            // The current capacity of the queue.
            let global_size = self.buffer_size + prev_size;

            if prev_size <= location && location < global_size {
                // We found the right buffer to insert.
                return self.insert(data, location - prev_size, temp_tail, is_last_buffer);
            }

            // The location is in the next buffer. We need to allocate a new buffer
            // which becomes the new tail of the queue.
            if location >= global_size {
                let next = temp_tail.next.load(Ordering::Acquire);

                if next.is_null() {
                    // No next buffer, allocate a new one.
                    let new_buffer_ptr = self.allocate_buffer(temp_tail);
                    // Try setting the successor of the current buffer to point to our new buffer.
                    if temp_tail
                        .next
                        .compare_exchange(
                            ptr::null_mut(),
                            new_buffer_ptr,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        // Only one thread comes here and updates the next pointer.
                        temp_tail.next.store(new_buffer_ptr, Ordering::Release)
                    } else {
                        // CAS was unsuccessful, we can drop our buffer.
                        // See the insert method for an optimization that
                        // reduces contention and wasteful allocations.
                        MpscQueue::drop_buffer(new_buffer_ptr)
                    }
                } else {
                    // If next is not null, we update the tail and proceed on the that buffer.
                    self.tail_of_queue.compare_and_swap(
                        temp_tail as *mut _,
                        next,
                        Ordering::SeqCst,
                    );
                }
            }
        }
    }

    pub fn dequeue(&mut self) -> Option<T> {
        // The buffer from which we eventually dequeue from.
        let mut temp_tail;

        loop {
            temp_tail = unsafe { &mut *self.tail_of_queue.load(Ordering::SeqCst) };
            // The number of items in the queue without the current buffer.
            let prev_size = self.size_without_buffer(temp_tail);

            let head = &mut unsafe { &mut *self.head_of_queue }.head;
            let head_is_tail = self.head_of_queue == temp_tail;
            let head_is_empty = *head == self.tail.load(Ordering::Acquire) - prev_size;

            // The queue is empty.
            if head_is_tail && head_is_empty {
                break None;
            }

            // There are un-handled elements in the current buffer.
            if *head < self.buffer_size {
                let node = &mut unsafe { &mut *self.head_of_queue }.nodes[*head];

                // Check if the node has already been dequeued.
                // If yes, we increment head and move on.
                if node.state() == Handled {
                    *head += 1;
                    continue;
                }

                // The current head points to a node that is not yet set or handled.
                // This means an enqueue thread is still ongoing and we need to find
                // the next set element (and potentially dequeue that one).
                if node.state() == Empty {
                    if let Some(data) = self.search(head, node) {
                        return Some(data);
                    }
                }

                // The current head points to a valid node and can be dequeued.
                if node.state() == Set {
                    // Increment head
                    unsafe { (*self.head_of_queue).head += 1 };
                    let data = MpscQueue::read_data(node);
                    return Some(data);
                }
            }

            // The head buffer has been handled and can be removed.
            // The new head becomes the successor of the current head buffer.
            if *head >= self.buffer_size {
                if self.head_of_queue == self.tail_of_queue.load(Acquire) {
                    // There is only one buffer.
                    return None;
                }

                let next = unsafe { &*self.head_of_queue }.next.load(Acquire);
                if next == ptr::null_mut() {
                    return None;
                }

                // Drop head buffer.
                MpscQueue::drop_buffer(self.head_of_queue);

                self.head_of_queue = next;
            }
        }
    }

    fn search(&mut self, head: &mut usize, node: &mut Node<T>) -> Option<T> {
        let mut temp_buffer = self.head_of_queue;
        let mut temp_head = *head;
        // Indicates if we need to continue the search in the next buffer.
        let mut search_next_buffer = false;
        // Indicates if all nodes in the current buffer are handled.
        let mut all_handled = true;

        while node.state() == Empty {
            // There are unhandled elements in the current buffer.
            if temp_head < self.buffer_size {
                // Move forward inside the current buffer.
                let mut temp_node = &mut unsafe { &mut *self.head_of_queue }.nodes[temp_head];
                temp_head += 1;

                // We found a set node which becomes the new candidate for dequeue.
                if temp_node.state() == Set && node.state() == Empty {
                    // We scan from the head of the queue to the new candidate and
                    // check if there has been any node set in the meantime.
                    // If we find a node that is set, that node becomes the new
                    // dequeue candidate and we restart the scan process from the head.
                    // This process continues until there is no change found during scan.
                    // After scanning, `temp_node` stores the candidate node to dequeue.
                    self.scan(
                        node,
                        &mut temp_buffer,
                        &mut temp_head,
                        &mut temp_node,
                    );

                    // Check if the actual head has been set in between.
                    if node.state() == Set {
                        break;
                    }

                    // Dequeue the found candidate.
                    let data = MpscQueue::read_data(&mut temp_node);

                    if search_next_buffer
                        && (temp_head - 1) == unsafe { (*temp_buffer).head }
                    {
                        // If we moved to a new buffer, we need to move the head forward so
                        // in the end we can delete the buffer.
                        unsafe { (*temp_buffer).head += 1 };
                    }

                    Some(data);
                }

                if temp_node.state() == Empty {
                    all_handled = false;
                }
            }

            // We reached the end of the current buffer, move to the next.
            // This also performs a cleanup operation called `fold` that
            // removes buffers in which all elements are handled.
            if temp_head >= self.buffer_size {
                if all_handled && search_next_buffer {
                    // If all nodes in the current buffer are handled, we try to fold the
                    // queue, i.e. remove the intermediate buffer.
                    if self.fold_buffer(&mut temp_buffer, &mut temp_head) {
                        all_handled = true;
                        search_next_buffer = true;
                    } else {
                        // We reached the last buffer in the queue.
                        return None;
                    }
                } else {
                    let next = self.deref_buffer(temp_buffer).next.load(Acquire);
                    if next == ptr::null_mut() {
                        // We reached the last buffer in the queue.
                        return None;
                    }
                    temp_buffer = next;
                    temp_head = unsafe { &*temp_buffer }.head;
                    all_handled = true;
                    search_next_buffer = true;
                }
            }
        }

        None
    }

    fn scan(
        &mut self,
        // The element at the head of the queue (which is not set yet).
        node: &Node<T>,
        temp_head_of_queue: &mut *mut BufferList<T>,
        temp_head: &mut usize,
        temp_node: &mut &mut Node<T>,
    ) {
        let mut scan_head_of_queue = self.head_of_queue;
        let mut scan_head = unsafe { &*scan_head_of_queue }.head;

        while node.state() == Empty && scan_head_of_queue != *temp_head_of_queue
            || scan_head < (*temp_head - 1)
        {
            if scan_head > self.buffer_size {
                // We reached the end of the current buffer and switch to the next.
                scan_head_of_queue = unsafe { (*scan_head_of_queue).next.load(Acquire) };
                scan_head = unsafe { (*scan_head_of_queue).head };
                continue;
            }

            // Move forward inside the current buffer.
            let scan_node = &mut unsafe { &mut *scan_head_of_queue }.nodes[scan_head];
            scan_head += 1;

            if scan_node.state() == Set {
                // We found a new candidate to dequeue and restart
                // the scan from the head of the queue to that element.
                *temp_head = scan_head;
                *temp_head_of_queue = scan_head_of_queue;
                *temp_node = scan_node;

                // re-scan from the beginning of the queue
                scan_head_of_queue = self.head_of_queue;
                scan_head = unsafe { &*scan_head_of_queue }.head;
            }
        }
    }

    fn fold_buffer(
        &mut self,
        buffer_ptr: &mut *mut BufferList<T>,
        buffer_head: &mut usize,
    ) -> bool {
        let buffer = self.deref_buffer(*buffer_ptr);

        let next = buffer.next.load(Acquire);
        let prev = buffer.prev;

        if next == ptr::null_mut() {
            // We reached the last buffer, which cannot be dropped.
            return false;
        }

        self.deref_buffer_mut(next).prev = prev;
        self.deref_buffer_mut(prev)
            .next
            .store(next, Ordering::Release);

        // Drop current buffer
        MpscQueue::drop_buffer(*buffer_ptr);

        // Advance to the next buffer.
        *buffer_ptr = next;
        *buffer_head = self.deref_buffer_mut(*buffer_ptr).head;

        true
    }

    fn size_without_buffer(&mut self, buffer: &BufferList<T>) -> usize {
        self.buffer_size * (buffer.pos - 1)
    }

    fn insert(
        &mut self,
        data: T,
        index: usize,
        buffer: &mut BufferList<T>,
        is_last_buffer: bool,
    ) -> Result<(), T> {
        // Insert the element at the right index. This also atomically
        // sets the state of the node to SET to make that change
        // visible to the dequeue thread.
        unsafe {
            buffer.nodes[index].set_data(data);
        }

        // Optimization to reduce contention on the tail of the queue.
        // If the inserted element is the second entry in the
        // current buffer, we already allocate a new buffer and
        // append it to the queue.
        if index == 1 && is_last_buffer {
            let new_buffer_ptr = self.allocate_buffer(buffer);
            if buffer
                .next
                .compare_exchange(
                    ptr::null_mut(),
                    new_buffer_ptr,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                MpscQueue::drop_buffer(new_buffer_ptr);
            }
        }

        Ok(())
    }

    fn allocate_buffer(&mut self, temp_tail: &mut BufferList<T>) -> *mut BufferList<T> {
        let new_buffer = BufferList::with_prev(
            self.buffer_size,
            temp_tail.pos + 1,
            temp_tail as *mut _,
        );

        let new_buffer = Box::new(new_buffer);
        let new_buffer_ptr = Box::into_raw(new_buffer);
        new_buffer_ptr
    }

    fn deref_buffer(&self, ptr: *mut BufferList<T>) -> &BufferList<T> {
        unsafe { &*ptr }
    }

    fn deref_buffer_mut(&self, ptr: *mut BufferList<T>) -> &mut BufferList<T> {
        unsafe { &mut *ptr }
    }

    fn drop_buffer(ptr: *mut BufferList<T>) {
        drop(unsafe { Box::from_raw(ptr) })
    }

    fn read_data(node: &mut Node<T>) -> T {
        // Read the data from the candidate node.
        let data = unsafe { node.data.as_ptr().read() };
        // Replace the data with MaybeUninit to avoid double free in the case
        // where the consumer drops T and the buffer is dropped afterwards.
        node.data = MaybeUninit::uninit();
        // Mark node as handled to avoid double-dequeue.
        node.set_state(Handled);

        data
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn enqueue() {
        let mut q = MpscQueue::new();

        unsafe {
            assert_eq!(State::Empty, (*q.head_of_queue).nodes[0].state());
        }
        unsafe {
            assert_eq!(State::Empty, (*q.head_of_queue).nodes[1].state());
        }

        assert_eq!(Ok(()), q.enqueue(42));
        assert_eq!(Ok(()), q.enqueue(43));

        unsafe {
            assert_eq!(42, (*q.head_of_queue).nodes[0].data.assume_init());
        }
        unsafe {
            assert_eq!(State::Set, (*q.head_of_queue).nodes[0].state());
        }
        unsafe {
            assert_eq!(43, (*q.head_of_queue).nodes[1].data.assume_init());
        }
        unsafe {
            assert_eq!(State::Set, (*q.head_of_queue).nodes[1].state());
        }
    }

    #[test]
    fn enqueue_exceeds_buffer() {
        let mut q = MpscQueue::new();

        for i in 0..BUFFER_SIZE * 2 {
            let _ = q.enqueue(i);
        }

        for i in 0..BUFFER_SIZE * 2 {
            let buffer = i / BUFFER_SIZE;
            let index = i % BUFFER_SIZE;

            if buffer == 0 {
                unsafe {
                    assert_eq!(i, (*q.head_of_queue).nodes[index].data.assume_init());
                }
            } else {
                unsafe {
                    assert_eq!(
                        i,
                        (*q.tail_of_queue.load(Ordering::SeqCst)).nodes[index]
                            .data
                            .assume_init()
                    );
                }
            }
        }
    }

    #[test]
    fn dequeue() {
        let mut q = MpscQueue::new();

        assert_eq!(None, q.dequeue());
        assert_eq!(Ok(()), q.enqueue(42));
        assert_eq!(Some(42), q.dequeue());
        assert_eq!(None, q.dequeue());
    }
}
