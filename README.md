# zarrs_surrealkv

[SurrealKV](https://github.com/surrealdb/surrealkv)-based storage for the [zarrs](https://github.com/zarrs/zarrs) ecosystem.

Note that [SurrealDB](https://surrealdb.com/), the more general data store built on top of SurrealKV and other technologies, is supported by [OpenDAL](https://opendal.apache.org/) and therefore [zarrs_opendal](https://github.com/zarrs/zarrs_opendal).

## Usage

Use the `SurrealKvStore::builder()` method to configure the underlying storage;
build the store with `SurrealKvStore::try_from`.

This implements zarrs' async readable/writable/listable traits.

Ensure writing is complete with `my_store.close().await`.

## Use case

This was motivated by the use of OME-Zarr as an acquisition format for microscopy.

Generally, write-optimised formats (where write patterns should match the microscope's raster scanning)
conflict with read-optimised formats (isotropic chunking, reduce inode count with sharding).

As such, it may be preferable to write to one Zarr storage type, chunking scheme etc. as an intermediate,
and then restructure it into another store, chunking scheme, shard layout etc..
SurrealKV looked like a good option for the write cache: it can handle many small chunks well, has high throughput, is eventually consistent on disk, and handles many writers while keeping the total number of files down.
