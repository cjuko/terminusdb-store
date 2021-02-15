#![feature(test)]

extern crate test;

use terminus_store;
use terminus_store::layer::{StringTriple};
use test::Bencher;

#[bench]
fn bench_add_string_triple(b: &mut Bencher) {
    let sync_store = terminus_store::open_sync_directory_store("/tmp/teststore_add_string_triple_bench");
    let layer_builder = sync_store.create_base_layer().unwrap();
    let mut count = 1;
    b.iter(|| {
        layer_builder.add_string_triple(StringTriple::new_value(&count.to_string(), &count.to_string(), &count.to_string())).unwrap();
        count += 1;
    });
}
