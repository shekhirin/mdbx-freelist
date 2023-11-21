use crate::config::{SMALL_VALUES_TO_DELETE, SMALL_VALUES_TO_INSERT, SMALL_VALUE_SIZE, USE_LIFO};
use crate::duration::Durations;
use crate::print_stats;
use rand::prelude::*;
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, PageSize, Transaction, WriteFlags, RW,
};
use std::path::Path;
use tempfile::tempdir;

pub fn create_original_db(path: &Path) -> eyre::Result<()> {
    println!("Creating original database...");
    let temp = tempdir()?;
    let env = create_env(&temp)?;
    with_txn(&env, |txn| {
        txn.create_db(Some(Table::Small.as_str()), DatabaseFlags::empty())?;
        txn.create_db(Some(Table::Large.as_str()), DatabaseFlags::empty())?;
        txn.create_db(Some(Table::Ballast.as_str()), DatabaseFlags::empty())?;
        Ok(())
    })?;
    println!();

    print_stats(&env)?;
    println!("Appending {SMALL_VALUES_TO_INSERT} small records {SMALL_VALUE_SIZE} bytes each...");
    let mut durations = Durations::default();
    with_txn(&env, |txn| {
        let dbi = txn.open_db(Some(Table::Small.as_str()))?.dbi();
        for key in 0..SMALL_VALUES_TO_INSERT {
            durations.measure_put(|| {
                txn.put(
                    dbi,
                    format!("small-{}", key).as_bytes(),
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

        println!("  100.0%. Put: {:?}", durations.finish_put_run());
        println!("  Put: {:?}", durations.finish().0);
        Ok(())
    })?;
    println!();

    print_stats(&env)?;
    println!("Deleting {SMALL_VALUES_TO_DELETE} small records {SMALL_VALUE_SIZE} bytes each...");
    let mut keys: Vec<_> = (0..SMALL_VALUES_TO_INSERT).collect();
    keys.shuffle(&mut thread_rng());
    let mut keys = keys[..SMALL_VALUES_TO_DELETE].to_vec();
    keys.sort();

    for key_chunk in keys.chunks(10_000) {
        with_txn(&env, |txn| {
            let dbi = txn.open_db(Some(Table::Small.as_str()))?.dbi();

            for key in key_chunk.iter() {
                txn.del(dbi, format!("small-{}", key).as_bytes(), None)?;
            }
            Ok(())
        })?;
    }
    println!("  100.0%");
    println!();

    drop(env);

    std::fs::create_dir(path)?;
    for entry in std::fs::read_dir(&temp)? {
        let entry = entry?;
        assert!(entry.file_type()?.is_file());
        std::fs::rename(entry.path(), path.join(entry.file_name()))?;
    }

    Ok(())
}

pub fn create_env(path: impl AsRef<Path>) -> eyre::Result<Environment> {
    Ok(Environment::builder()
        .set_geometry(Geometry {
            size: Some(0..50 * 1024 * 1024 * 1024),   // max 30GB
            page_size: Some(PageSize::Set(4 * 1024)), // 4KB
            ..Default::default()
        })
        .set_max_dbs(3)
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
    Small,
    Large,
    Ballast,
}

impl Table {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Large => "large",
            Self::Ballast => "ballast",
        }
    }
}
