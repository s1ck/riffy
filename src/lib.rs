use core::ptr;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Release};

use crate::State::{Empty, Set};

// NODE_SIZE
const BUFFER_SIZE: usize = 1620;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
enum State {
    Empty,
    Set,
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

struct Node<T> {
    data: MaybeUninit<T>,
    is_set: AtomicU8,
}

impl<T> Node<T> {
    unsafe fn set_data(&mut self, data: T) {
        self.data.as_mut_ptr().write(data);
        self.is_set.store(State::Set.into(), Ordering::Relaxed);
    }
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Node { data: MaybeUninit::uninit(), is_set: AtomicU8::new(State::Empty.into()) }
    }
}

struct BufferList<T> {
    curr_buffer: Vec<Node<T>>,
    prev: *mut BufferList<T>,
    next: AtomicPtr<BufferList<T>>,
    // updated by the consumer
    head: usize,
    // start with 1
    position_in_queue: usize,
}

impl<T> BufferList<T> {
    fn new(size: usize, position_in_queue: usize) -> Self {
        BufferList::with_prev(size, position_in_queue, ptr::null_mut())
    }

    fn with_prev(size: usize, position_in_queue: usize, prev: *mut BufferList<T>) -> Self {
        let mut curr_buffer = Vec::with_capacity(size);
        curr_buffer.resize_with(size, Node::default);

        BufferList {
            curr_buffer,
            prev,
            next: AtomicPtr::new(ptr::null_mut()),
            head: 0,
            position_in_queue,
        }
    }
}

struct MpscQueue<T> {
    head_of_queue: *mut BufferList<T>,
    tail_of_queue: AtomicPtr<BufferList<T>>,

    buffer_size: usize,
    tail: AtomicUsize,
}

impl<T> MpscQueue<T> {
    fn new() -> Self {
        let head_of_queue = BufferList::new(BUFFER_SIZE, 1);
        // move to heap
        let head = Box::new(head_of_queue);
        // forget about it but keep pointer
        let head = Box::into_raw(head);
        // make pointer atomic
        let tail = AtomicPtr::new(head);

        MpscQueue { head_of_queue: head, tail_of_queue: tail, buffer_size: BUFFER_SIZE, tail: AtomicUsize::new(0) }
    }

    fn enqueue(&mut self, data: T) -> Result<(), T> {
        let location = self.tail.fetch_add(1, Ordering::SeqCst);

        let mut temp_tail;
        let mut is_last_buffer = false;

        loop {
            temp_tail = unsafe { &mut *self.tail_of_queue.load(Ordering::Acquire) };
            // the amount of items in the queue without the current buffer
            let mut prev_size = self.buffer_size * ((*temp_tail).position_in_queue - 1);

            // the location is back in the queue - we need to go back in the queue to the right buffer
            while location < prev_size {
                is_last_buffer = true;
                temp_tail = unsafe { &mut *temp_tail.prev };
                prev_size -= self.buffer_size;
            }

            // the amount of items in the queue
            let global_size = self.buffer_size + prev_size;

            if prev_size <= location && location < global_size {
                // we are in the right buffer
                let index = location - prev_size;

                unsafe { temp_tail.curr_buffer[index].set_data(data); }

                if index == 1 && !is_last_buffer {
                    // optimization in the paper to avoid contention on updating tail_of_queue
                    let new_buffer = BufferList::with_prev(
                        self.buffer_size,
                        temp_tail.position_in_queue + 1,
                        temp_tail as *mut _,
                    );

                    let new_buffer = Box::new(new_buffer);
                    let new_buffer_ptr = Box::into_raw(new_buffer);

                    if temp_tail.next.compare_exchange(
                        ptr::null_mut(),
                        new_buffer_ptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ).is_err() {
                        drop(unsafe { Box::from_raw(new_buffer_ptr) })
                    }
                }

                break Ok(());
            }

            if location >= global_size {
                // the location is in the next buffer

                let next = temp_tail.next.load(Ordering::Acquire);
                if next.is_null() {
                    // no next buffer, try to allocate a new one
                    let new_buffer = BufferList::with_prev(
                        self.buffer_size,
                        temp_tail.position_in_queue + 1,
                        temp_tail as *mut _,
                    );

                    let new_buffer = Box::new(new_buffer);
                    let new_buffer_ptr = Box::into_raw(new_buffer);

                    if temp_tail.next.compare_exchange(
                        ptr::null_mut(),
                        new_buffer_ptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ).is_ok() {
                        temp_tail.next.store(new_buffer_ptr, Ordering::Release)
                    } else {
                        drop(unsafe { Box::from_raw(new_buffer_ptr) })
                    }
                } else {
                    self.tail_of_queue.compare_and_swap(temp_tail as *mut _, next, Ordering::SeqCst);
                }
            }
        }
    }

    fn dequeue(&mut self) -> Option<T> {
        let mut temp_tail;

        loop {
            temp_tail = unsafe { &mut *self.tail_of_queue.load(Ordering::Acquire) };
            // the amount of items in the queue without the current buffer
            let mut prev_size = self.buffer_size * ((*temp_tail).position_in_queue - 1);

            let head = &mut unsafe { &mut *self.head_of_queue }.head;
            let head_is_tail = self.head_of_queue == self.tail_of_queue.load(Ordering::Acquire);
            let head_is_empty = *head == self.tail.load(Acquire) - prev_size;

            if head_is_tail && head_is_empty {
                // queue is empty
                break None;
            } else if *head < self.buffer_size {
                // there are elements in the current buffer
                let node = &mut unsafe { &mut *self.head_of_queue }.curr_buffer[*head];

                if node.is_set.load(Acquire) == State::Handled {
                    *head += 1;
                    continue;
                }

                let mut temp_head_of_queue = self.head_of_queue;
                let mut temp_head = *head;
                let mut flag_move_to_new_buffer = false;
                let mut flag_buffer_all_handled = true;

                while node.is_set.load(Acquire) == State::Empty {
                    if temp_head < self.buffer_size {
                        // there are elements in the current buffer
                        let mut temp_node = &mut unsafe { &mut *self.head_of_queue }.curr_buffer[temp_head];
                        temp_head += 1;

                        if temp_node.is_set.load(Acquire) == State::Set && node.is_set.load(Acquire) == State::Empty {
                            // the data is valid and the element in the head is still in enqueue process
                            let mut scan_head_of_queue = self.head_of_queue;

                            let mut scan_head = unsafe { &*scan_head_of_queue }.head;

                            // start scanning
                            while scan_head_of_queue != temp_head_of_queue || scan_head < (temp_head - 1) && node.is_set.load(Acquire) == State::Empty {
                                if scan_head > self.buffer_size {
                                    // end of buffer
                                    // go to next buffer
                                    scan_head_of_queue = unsafe { (*scan_head_of_queue).next.load(Acquire) };
                                    scan_head = unsafe { (*scan_head_of_queue).head };
                                    continue;
                                }

                                let scan_node = &mut unsafe { &mut *scan_head_of_queue }.curr_buffer[scan_head];

                                if scan_node.is_set.load(Acquire) == State::Set {
                                    // there is another element that is set
                                    // this is the new item to be evicted
                                    temp_head = scan_head;
                                    temp_head_of_queue = scan_head_of_queue;
                                    temp_node = scan_node;

                                    // re-scan from the beginning of the queue
                                    scan_head_of_queue = self.head_of_queue;
                                    scan_head = unsafe { &*scan_head_of_queue }.head;
                                }

                                scan_head += 1;
                            }

                            if node.is_set.load(Acquire) == State::Set {
                                break;
                            }

                            // end scanning

                            let data = unsafe { temp_node.data.as_ptr().read() };
                            // idea is to avoid double-free once buffer is dropped (and the consumer dropped T before)
                            temp_node.data = MaybeUninit::uninit();
                            temp_node.is_set.store(State::Handled.into(), Release);

                            if flag_move_to_new_buffer && (temp_head - 1) == unsafe { (*temp_head_of_queue).head } {
                                // if we moved to a new buffer, we need to move the head forward so in the end we can delete the buffer
                                unsafe { (*temp_head_of_queue).head += 1 };
                            }

                            Some(data);
                        }

                        if temp_node.is_set.load(Acquire) == Empty {
                            flag_buffer_all_handled = false;
                        }
                    }

                    if temp_head >= self.buffer_size {
                        // we reach the end of the buffer - move to the next
                        if flag_buffer_all_handled && flag_move_to_new_buffer {
                            // we want to "fold" the queue
                            // if there is a thread that got stuck - we want to keep only that buffer and delete the rest (before it)

                            if temp_head_of_queue == self.tail_of_queue.load(Acquire) {
                                // there is no place to move
                                return None;
                            }

                            let next = unsafe { &*temp_head_of_queue }.next.load(Acquire);
                            let prev = unsafe { &*temp_head_of_queue }.prev;

                            if next == ptr::null_mut() {
                                // if we do not have where to move ? what ?
                                return None;
                            }

                            unsafe { (*next).prev = prev };
                            unsafe { (*prev).next.store(next, Ordering::Release) };

                            // drop folded buffer
                            drop(unsafe { Box::from_raw(temp_head_of_queue) });

                            temp_head_of_queue = next;
                            temp_head = unsafe { &*temp_head_of_queue }.head;
                            flag_buffer_all_handled = true;
                            flag_move_to_new_buffer = true;
                        } else {
                            let next = unsafe { &*temp_head_of_queue }.next.load(Acquire);
                            if next == ptr::null_mut() {
                                // if we do not have where to move ? what ?
                                return None;
                            }
                            temp_head_of_queue = next;
                            temp_head = unsafe { &*temp_head_of_queue }.head;
                            flag_buffer_all_handled = true;
                            flag_move_to_new_buffer = true;
                        }
                    }
                }

                // try to take out set elements that are next in line
                if node.is_set.load(Acquire) == Set {
                    // valid
                    unsafe { (*self.head_of_queue).head += 1 };

                    let data = unsafe { node.data.as_ptr().read() };
                    // idea is to avoid double-free once buffer is dropped (and the consumer dropped T before)
                    node.data = MaybeUninit::uninit();
                    node.is_set.store(State::Handled.into(), Release);

                    return Some(data);
                }
            }
            if unsafe { (*self.head_of_queue).head } >= self.buffer_size {
                // move to next array and the delete the prev array
                if self.head_of_queue == self.tail_of_queue.load(Acquire) {
                    // there is no place to move
                    return None;
                }

                let next = unsafe { &*self.head_of_queue }.next.load(Acquire);
                if next == ptr::null_mut() {
                    return None;
                }

                // drop folded buffer
                drop(unsafe { Box::from_raw(self.head_of_queue) });

                unsafe { self.head_of_queue = next };
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn enqueue() {
        let mut q = MpscQueue::new();

        unsafe { assert_eq!(State::Empty, (*q.head_of_queue).curr_buffer[0].is_set.load(Ordering::SeqCst).into()); }
        unsafe { assert_eq!(State::Empty, (*q.head_of_queue).curr_buffer[1].is_set.load(Ordering::SeqCst).into()); }

        assert_eq!(Ok(()), q.enqueue(42));
        assert_eq!(Ok(()), q.enqueue(43));

        unsafe { assert_eq!(42, (*q.head_of_queue).curr_buffer[0].data.assume_init()); }
        unsafe { assert_eq!(State::Set, (*q.head_of_queue).curr_buffer[0].is_set.load(Ordering::SeqCst).into()); }
        unsafe { assert_eq!(43, (*q.head_of_queue).curr_buffer[1].data.assume_init()); }
        unsafe { assert_eq!(State::Set, (*q.head_of_queue).curr_buffer[1].is_set.load(Ordering::SeqCst).into()); }
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
                unsafe { assert_eq!(i, (*q.head_of_queue).curr_buffer[index].data.assume_init()); }
            } else {
                unsafe { assert_eq!(i, (*q.tail_of_queue.load(Ordering::SeqCst)).curr_buffer[index].data.assume_init()); }
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
