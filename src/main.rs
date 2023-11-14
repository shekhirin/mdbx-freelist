mod config;

use crate::config::*;
use inc_stats::Percentiles;
use itertools::Itertools;
use rand::prelude::*;
use rand::thread_rng;
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, EnvironmentKind, Geometry, PageSize, Transaction,
    WriteFlags, WriteMap, RW,
};
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn main() -> eyre::Result<()> {
    let dir = tempdir()?;
    let env = Environment::<WriteMap>::new()
        .set_geometry(Geometry {
            size: Some(5 * 1024 * 1024 * 1024..10 * 1024 * 1024 * 1024), // min 5GB, max 10GB
            page_size: Some(PageSize::Set(4 * 1024)),                    // 4KB
            ..Default::default()
        })
        .set_max_dbs(2)
        .set_flags(EnvironmentFlags {
            liforeclaim: USE_LIFO,
            ..Default::default()
        })
        .open(dir.path())?;

    with_txn(&env, |txn| {
        txn.create_db(Some(Table::Data.as_str()), DatabaseFlags::empty())?;
        txn.create_db(Some(Table::Ballast.as_str()), DatabaseFlags::empty())?;
        Ok(())
    })?;

    print_stats(&env)?;
    println!("Inserting {SMALL_VALUES_TO_INSERT} records {SMALL_VALUE_SIZE} bytes each...");
    let mut percentiles = Percentiles::new();
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..SMALL_VALUES_TO_INSERT {
            let start = Instant::now();
            txn.put(
                dbi,
                small_value_key(key),
                [0; SMALL_VALUE_SIZE],
                WriteFlags::empty(),
            )?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;

            percentiles.add(elapsed.as_secs_f64());

            if key > 0 && key % (SMALL_VALUES_TO_INSERT / 10) == 0 {
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
        print_percentiles(&percentiles);
        Ok(())
    })?;
    println!();

    if DELETE_SMALL_VALUES {
        print_stats(&env)?;
        println!("Deleting {SMALL_VALUES_TO_DELETE} records {SMALL_VALUE_SIZE} bytes each...");
        let mut keys: Vec<_> = (0..SMALL_VALUES_TO_INSERT).collect();
        keys.shuffle(&mut thread_rng());
        with_txn(&env, |txn| {
            let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

            for (i, key) in keys.iter().take(SMALL_VALUES_TO_DELETE).enumerate() {
                txn.del(dbi, small_value_key(*key), None)?;

                if i % (SMALL_VALUES_TO_DELETE / 10) == 0 {
                    println!("  {:.1}%", i as f64 / SMALL_VALUES_TO_DELETE as f64 * 100.0);
                }
            }

            println!("  100.0%");

            Ok(())
        })?;
        println!();
    }

    if USE_BALLAST {
        print_stats(&env)?;
        let mut percentiles = Percentiles::new();
        println!(
            "Inserting {BALLAST_VALUES_TO_INSERT} ballasts {BALLAST_VALUE_SIZE} bytes each..."
        );
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
                percentiles.add(elapsed.as_secs_f64());

                if key > 0 && key % (BALLAST_VALUES_TO_INSERT / 10) == 0 {
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
            print_percentiles(&percentiles);

            Ok(())
        })?;
        println!();

        print_stats(&env)?;
        let mut percentiles = Percentiles::new();
        println!("Deleting {BALLAST_VALUES_TO_USE} ballasts {BALLAST_VALUE_SIZE} bytes each...");
        with_txn(&env, |txn| {
            let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

            let mut total_duration = Duration::default();
            let mut log_duration = Duration::default();

            for key in 0..BALLAST_VALUES_TO_USE {
                let start = Instant::now();
                assert!(txn.del(dbi, ballast_key(key), None)?);
                let elapsed = start.elapsed();
                total_duration += elapsed;
                log_duration += elapsed;
                percentiles.add(elapsed.as_secs_f64());

                if key > 0 && key % (BALLAST_VALUES_TO_USE / 10) == 0 {
                    println!(
                        "  {:.1}%, time per del: {:?}",
                        key as f64 / BALLAST_VALUES_TO_USE as f64 * 100.0,
                        log_duration / (BALLAST_VALUES_TO_USE / 10) as u32
                    );
                    log_duration = Duration::default();
                }
            }

            println!(
                "  100.0%, time per del: {:?}",
                log_duration / (BALLAST_VALUES_TO_USE / 10) as u32
            );
            println!(
                "  Time per del: {:?}",
                total_duration / BALLAST_VALUES_TO_USE as u32
            );
            print_percentiles(&percentiles);

            Ok(())
        })?;
        println!();
    }

    print_stats(&env)?;
    println!("Inserting {LARGE_VALUES_TO_INSERT} records {LARGE_VALUE_SIZE} bytes each...");
    let mut percentiles = Percentiles::new();
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        let mut total_duration = Duration::default();
        let mut log_duration = Duration::default();

        for key in 0..LARGE_VALUES_TO_INSERT {
            let start = Instant::now();
            txn.put(
                dbi,
                large_value_key(key),
                [0; LARGE_VALUE_SIZE],
                WriteFlags::empty(),
            )?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            log_duration += elapsed;
            percentiles.add(elapsed.as_secs_f64());

            if key > 0 && key % (LARGE_VALUES_TO_INSERT / 10) == 0 {
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
        print_percentiles(&percentiles);

        Ok(())
    })?;
    println!();

    print_stats(&env)?;

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

fn print_stats<E: EnvironmentKind>(env: &Environment<E>) -> eyre::Result<()> {
    let freelist = env.freelist()?;
    let stat = env.stat()?;
    println!(
        "Freelist: {}, Depth: {}, Branch Pages: {}, Leaf Pages: {}, Overflow Pages: {}, Entries: {}",
        freelist,
        stat.depth(),
        stat.branch_pages(),
        stat.leaf_pages(),
        stat.overflow_pages(),
        stat.entries(),
    );

    Ok(())
}

fn print_percentiles(percentiles: &Percentiles<f64>) {
    println!(
        "{}",
        [0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0]
            .iter()
            .map(|percentile| format!(
                "{percentile}: {:?}",
                Duration::from_secs_f64(percentiles.percentile(percentile).unwrap().unwrap())
            ))
            .join(", ")
    );
}

fn small_value_key(key: usize) -> impl AsRef<[u8]> {
    [b"small", key.to_le_bytes().as_ref()].concat()
}

fn large_value_key(key: usize) -> impl AsRef<[u8]> {
    [b"large", key.to_le_bytes().as_ref()].concat()
}

fn ballast_key(key: usize) -> impl AsRef<[u8]> {
    [b"ballast", key.to_le_bytes().as_ref()].concat()
}
