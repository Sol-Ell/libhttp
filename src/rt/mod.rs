// The platform reactor (Task 7) and handler now consume these modules, but the
// crate still has no public API: nothing reachable from `lib.rs` uses them in a
// non-test build, so every internal item is dead code. Remove these allows when
// the public runtime surface lands.
#[allow(dead_code)]
pub(crate) mod block;
#[allow(dead_code)]
pub(crate) mod conn;
#[allow(dead_code)]
pub(crate) mod handler;
pub(crate) mod poller;
#[allow(dead_code)]
pub(crate) mod pool;
#[allow(dead_code)]
pub(crate) mod reactor;
#[allow(dead_code)]
pub(crate) mod slab;
#[allow(dead_code)]
pub(crate) mod socket;
#[allow(dead_code)]
pub(crate) mod timer;
#[allow(dead_code)]
pub(crate) mod wake;

#[cfg(test)]
mod test_support;
