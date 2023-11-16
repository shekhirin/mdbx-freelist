use crate::config::{SMALL_VALUES_TO_DELETE, SMALL_VALUES_TO_INSERT, SMALL_VALUE_SIZE, USE_LIFO};
use crate::duration::Durations;
use crate::print_stats;
use rand::prelude::*;
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, PageSize, Transaction, WriteFlags, RW,
};
use std::path::Path;

pub fn create_original_db(path: &Path) -> eyre::Result<()> {
    println!("Creating original database...");
    let env = create_env(path)?;
    with_txn(&env, |txn| {
        txn.create_db(Some(Table::Data.as_str()), DatabaseFlags::empty())?;
        txn.create_db(Some(Table::Ballast.as_str()), DatabaseFlags::empty())?;
        Ok(())
    })?;
    println!();

    print_stats(&env)?;
    println!("Inserting {SMALL_VALUES_TO_INSERT} records {SMALL_VALUE_SIZE} bytes each...");
    let mut durations = Durations::default();
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

        for key in 0..SMALL_VALUES_TO_INSERT {
            durations.measure_put(|| {
                txn.put(
                    dbi,
                    small_value_key(key),
                    [0; SMALL_VALUE_SIZE],
                    WriteFlags::empty(),
                )
            })?;

            if key > 0 && key % (SMALL_VALUES_TO_INSERT / 10) == 0 {
                println!(
                    "  {:.1}%. Put: {:?}.",
                    key as f64 / SMALL_VALUES_TO_INSERT as f64 * 100.0,
                    durations.finish_put_run()
                );
            }
        }

        println!("  100.0%");
        println!("    Put: {:?}", durations.finish_put_run());
        println!("  Put: {:?}", durations.finish().0);
        Ok(())
    })?;
    println!();

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

    Ok(())
}

pub fn create_env(path: impl AsRef<Path>) -> eyre::Result<Environment> {
    Ok(Environment::builder()
        .set_geometry(Geometry {
            size: Some(0..50 * 1024 * 1024 * 1024),   // max 50GB
            page_size: Some(PageSize::Set(4 * 1024)), // 4KB
            ..Default::default()
        })
        .set_max_dbs(2)
        .set_flags(EnvironmentFlags {
            liforeclaim: USE_LIFO,
            ..Default::default()
        })
        .open(path.as_ref())?)
}

pub fn with_txn(
    env: &Environment,
    f: impl FnOnce(&Transaction<RW>) -> eyre::Result<()>,
) -> eyre::Result<()> {
    let txn = env.begin_rw_txn()?;
    f(&txn)?;
    txn.commit()?;

    Ok(())
}

pub enum Table {
    Data,
    Ballast,
}

impl Table {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Ballast => "ballast",
        }
    }
}

pub fn small_value_key(key: usize) -> impl AsRef<[u8]> {
    [b"small", key.to_le_bytes().as_ref()].concat()
}

pub fn large_value_key(key: usize) -> impl AsRef<[u8]> {
    [b"large", key.to_le_bytes().as_ref()].concat()
}

pub fn ballast_key(key: usize) -> impl AsRef<[u8]> {
    [b"ballast", key.to_le_bytes().as_ref()].concat()
}
