//! Logic for working with layers.
//!
//! Databases in terminus-store are stacks of layers. The first layer
//! in such a stack is a base layer, which contains an intial data
//! set. On top of that, each layer stores additions and removals.
pub mod builder;
pub mod id_map;
mod internal;
mod layer;
mod simple_builder;
mod query;

pub use id_map::*;
pub use internal::*;
pub use layer::*;
pub use simple_builder::*;
