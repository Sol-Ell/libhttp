// Blocks, pools and the timer heap are staged for the reactor/handler tasks;
// they have no non-test consumers yet. Remove these allows once those tasks
// wire them up.
#[allow(dead_code)]
pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod handler;
pub(crate) mod poller;
#[allow(dead_code)]
pub(crate) mod pool;
pub(crate) mod reactor;
pub(crate) mod slab;
pub(crate) mod socket;
#[allow(dead_code)]
pub(crate) mod timer;
pub(crate) mod wake;

#[cfg(test)]
mod test_support;
