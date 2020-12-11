use core::ptr;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

use crate::State::Empty;

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
            q.enqueue(i);
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
}
