#![forbid(unsafe_op_in_unsafe_fn)]

mod rt;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_identity_is_libhttp() {
        assert_eq!(env!("CARGO_PKG_NAME"), "libhttp");
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0");
        assert_eq!(option_env!("CARGO_PKG_RUST_VERSION"), Some("1.85"));
    }
}
