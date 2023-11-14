use rand::prelude::*;
use rand::thread_rng;
use reth_libmdbx::{
    DatabaseFlags, Environment, Geometry, PageSize, Transaction, WriteFlags, WriteMap, RW,
};
use std::time::{Duration, Instant};
use tempfile::tempdir;

const SMALL_VALUES_TO_INSERT: usize = 100_000;
const SMALL_VALUE_SIZE: usize = 4 * 1024; // 4KB
const SMALL_VALUES_TO_DELETE: usize = 50_000;

const LARGE_VALUES_TO_INSERT: usize = 10_000;
const LARGE_VALUE_SIZE: usize = 200 * 1024; // 200KB

const BALLAST_VALUES_TO_INSERT: usize = 20_000;
const BALLAST_VALUE_SIZE: usize = 300 * 1024; // 300KB

enum Table {
    Data,
    Ballast,
}

impl Table {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Ballast => "ballast",
        }
    }
}

fn main() -> eyre::Result<()> {
    let dir = tempdir()?;
    let env = Environment::<WriteMap>::new()
        .set_geometry(Geometry {
            size: Some(0..10 * 1024 * 1024 * 1024), // 10GB
            page_size: Some(PageSize::Set(4096)),   // 4KB
            ..Default::default()
        })
        .set_max_dbs(2)
        .open(dir.path())?;

    with_txn(&env, |txn| {
        txn.create_db(Some(Table::Data.as_str()), DatabaseFlags::empty())?;
        txn.create_db(Some(Table::Ballast.as_str()), DatabaseFlags::empty())?;
        Ok(())
    })?;

    println!("Freelist: {}", env.freelist()?);
    println!("Inserting {SMALL_VALUES_TO_INSERT} records {SMALL_VALUE_SIZE} bytes each...");
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..SMALL_VALUES_TO_INSERT {
            let start = Instant::now();
            txn.put(
                dbi,
                data_key(key),
                [0; SMALL_VALUE_SIZE],
                WriteFlags::empty(),
            )?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;

            if key % (SMALL_VALUES_TO_INSERT / 10) == 0 {
                println!(
                    "  {:.1}%, time per put: {:?}",
                    key as f64 / SMALL_VALUES_TO_INSERT as f64 * 100.0,
                    log_duration / (SMALL_VALUES_TO_INSERT / 10) as u32
                );
                log_duration = Duration::default();
            }
        }

        println!(
            "  100.0%, time per put: {:?}",
            log_duration / (SMALL_VALUES_TO_INSERT / 10) as u32
        );
        println!(
            "  Time per put: {:?}",
            total_duration / SMALL_VALUES_TO_INSERT as u32
        );

        Ok(())
    })?;
    println!();

    println!("Freelist: {}", env.freelist()?);
    println!("Deleting {SMALL_VALUES_TO_DELETE} records {SMALL_VALUE_SIZE} bytes each...");
    let mut keys: Vec<_> = (0..SMALL_VALUES_TO_INSERT).collect();
    keys.shuffle(&mut thread_rng());
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        for (i, key) in keys.iter().take(SMALL_VALUES_TO_DELETE).enumerate() {
            txn.del(dbi, data_key(*key), None)?;

            if i % (SMALL_VALUES_TO_DELETE / 10) == 0 {
                println!("  {:.1}%", i as f64 / SMALL_VALUES_TO_DELETE as f64 * 100.0);
            }
        }

        println!("  100.0%");

        Ok(())
    })?;
    println!();

    println!("Freelist: {}", env.freelist()?);
    println!("Inserting {BALLAST_VALUES_TO_INSERT} ballasts {BALLAST_VALUE_SIZE} bytes each...");
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..BALLAST_VALUES_TO_INSERT {
            let start = Instant::now();
            txn.put(
                dbi,
                ballast_key(key),
                [0; BALLAST_VALUE_SIZE],
                WriteFlags::empty(),
            )?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;

            if key % (BALLAST_VALUES_TO_INSERT / 10) == 0 {
                println!(
                    "  {:.1}%, time per put: {:?}",
                    key as f64 / BALLAST_VALUES_TO_INSERT as f64 * 100.0,
                    log_duration / (BALLAST_VALUES_TO_INSERT / 10) as u32
                );
                log_duration = Duration::default();
            }
        }

        println!(
            "  100.0%, time per put: {:?}",
            log_duration / (BALLAST_VALUES_TO_INSERT / 10) as u32
        );
        println!(
            "  Time per put: {:?}",
            total_duration / BALLAST_VALUES_TO_INSERT as u32
        );

        Ok(())
    })?;
    println!();

    println!("Freelist: {}", env.freelist()?);
    println!("Acquiring the ballast for future inserts...");
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..LARGE_VALUES_TO_INSERT {
            let start = Instant::now();
            assert!(txn.del(dbi, ballast_key(key), None)?);
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;

            if key % (LARGE_VALUES_TO_INSERT / 10) == 0 {
                println!(
                    "  {:.1}%, time per del: {:?}",
                    key as f64 / LARGE_VALUES_TO_INSERT as f64 * 100.0,
                    log_duration / (LARGE_VALUES_TO_INSERT / 10) as u32
                );
                log_duration = Duration::default();
            }
        }

        println!(
            "  100.0%, time per del: {:?}",
            log_duration / (LARGE_VALUES_TO_INSERT / 10) as u32
        );
        println!(
            "  Time per del: {:?}",
            total_duration / LARGE_VALUES_TO_INSERT as u32
        );

        Ok(())
    })?;
    println!();

    println!("Freelist: {}", env.freelist()?);
    println!("Inserting {LARGE_VALUES_TO_INSERT} records {LARGE_VALUE_SIZE} bytes each...");
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..LARGE_VALUES_TO_INSERT {
            let start = Instant::now();
            txn.put(
                dbi,
                data_key(key),
                [0; LARGE_VALUE_SIZE],
                WriteFlags::empty(),
            )?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;

            if key % (LARGE_VALUES_TO_INSERT / 10) == 0 {
                println!(
                    "  {:.1}%, time per put: {:?}",
                    key as f64 / LARGE_VALUES_TO_INSERT as f64 * 100.0,
                    log_duration / (LARGE_VALUES_TO_INSERT / 10) as u32
                );
                log_duration = Duration::default();
            }
        }

        println!(
            "  100.0%, time per put: {:?}",
            log_duration / (LARGE_VALUES_TO_INSERT / 10) as u32
        );
        println!(
            "  Time per put: {:?}",
            total_duration / LARGE_VALUES_TO_INSERT as u32
        );

        Ok(())
    })?;
    println!();

    println!("Freelist: {}", env.freelist()?);

    Ok(())
}

fn with_txn(
    env: &Environment<WriteMap>,
    f: impl FnOnce(&Transaction<RW, WriteMap>) -> eyre::Result<()>,
) -> eyre::Result<()> {
    let txn = env.begin_rw_txn()?;
    f(&txn)?;
    txn.commit()?;

    Ok(())
}

fn data_key(key: usize) -> impl AsRef<[u8]> {
    [b"data", key.to_le_bytes().as_ref()].concat()
}

fn ballast_key(key: usize) -> impl AsRef<[u8]> {
    [b"ballast", key.to_le_bytes().as_ref()].concat()
}
