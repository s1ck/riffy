//! Benchmarking is heavily inspired by https://github.com/zesterer/flume/blob/master/benches/basic.rs
#[macro_use]
extern crate criterion;

use std::fmt::Debug;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use criterion::{Bencher, Criterion};

trait Sender: Clone + Send + Sized + 'static {
    type Item: Debug + Default;
    type Receiver: Receiver<Item = Self::Item>;

    fn unbounded() -> (Self, Self::Receiver);
    fn send(&self, msg: Self::Item);
}

trait Receiver: Send + Sized + 'static {
    type Item: Default;
    fn recv(&self) -> Self::Item;
}

impl<T: Send + Debug + Default + 'static> Sender for riffy::Sender<T> {
    type Item = T;
    type Receiver = riffy::Receiver<T>;

    fn unbounded() -> (Self, Self::Receiver) {
        riffy::channel()
    }

    fn send(&self, msg: T) {
        riffy::Sender::send(self, msg).unwrap();
    }
}

impl<T: Send + Default + 'static> Receiver for riffy::Receiver<T> {
    type Item = T;

    fn recv(&self) -> Self::Item {
        riffy::Receiver::recv(self).unwrap().unwrap()
    }
}

impl<T: Send + Debug + Default + 'static> Sender for mpsc::Sender<T> {
    type Item = T;
    type Receiver = mpsc::Receiver<T>;

    fn unbounded() -> (Self, Self::Receiver) {
        mpsc::channel()
    }

    fn send(&self, msg: T) {
        mpsc::Sender::send(self, msg).unwrap();
    }
}

impl<T: Send + Default + 'static> Receiver for mpsc::Receiver<T> {
    type Item = T;

    fn recv(&self) -> Self::Item {
        mpsc::Receiver::recv(self).unwrap()
    }
}

fn test_mpsc_unbounded_no_wait<S: Sender>(b: &mut Bencher, thread_num: u64) {
    b.iter_custom(|iters| {
        let iters = iters * 1000;
        let (tx, rx) = S::unbounded();
        let start = Instant::now();

        let handles = (0..thread_num)
            .map(|_| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for _ in 0..iters {
                        let _ = tx.send(Default::default());
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let _ = handle.join();
        }

        for _ in 0..thread_num * iters {
            rx.recv();
        }

        start.elapsed()
    })
}

fn mpsc_unbounded_no_wait_4t(b: &mut Criterion) {
    b.bench_function("mpsc-bounded-no-wait-4t-std", |b| {
        test_mpsc_unbounded_no_wait::<mpsc::Sender<u32>>(b, 4)
    });
    b.bench_function("mpsc-bounded-no-wait-4t-riffy", |b| {
        test_mpsc_unbounded_no_wait::<riffy::Sender<u32>>(b, 4)
    });
}

criterion_group!(compare, mpsc_unbounded_no_wait_4t,);
criterion_main!(compare);
