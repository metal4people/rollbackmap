//! *rollbackmap* implements a map with rollback support in Rust.
#![deny(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

mod rollbackmap;
pub use crate::rollbackmap::RollbackMap;

#[cfg(test)]
mod tests;
