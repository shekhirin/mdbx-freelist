mod config;
mod db;
mod duration;

use crate::config::*;
use crate::db::{create_env, create_original_db, with_txn, Table};
use crate::duration::Durations;
use reth_libmdbx::{Environment, WriteFlags};
use std::borrow::Cow;
use std::path::Path;
use tempfile::tempdir;

fn main() -> eyre::Result<()> {
    print_config();

    let original_db_path = Path::new("db");
    if !original_db_path.exists() {
        create_original_db(original_db_path)?;
    }

    let dir = tempdir()?;

    println!("Copying original database...");
    for entry in std::fs::read_dir(original_db_path)? {
        let entry = entry?;
        assert!(entry.file_type()?.is_file());
        std::fs::copy(entry.path(), dir.path().join(entry.file_name()))?;
    }
    println!();

    let env = create_env(dir.path())?;

    if USE_BALLAST {
        print_stats(&env)?;
        let mut durations = Durations::default();
        println!("Appending {BALLASTS_TO_INSERT} ballasts {BALLAST_SIZE} bytes each...");
        with_txn(&env, |txn| {
            let dbi = txn.open_db(Some(Table::Ballast.as_str()))?.dbi();

            for key in 0..BALLASTS_TO_INSERT {
                durations.measure_put(|| {
                    txn.put(
                        dbi,
                        key.to_be_bytes(),
                        [0; BALLAST_SIZE],
                        WriteFlags::APPEND,
                    )
                })?;

                if key > 0 && key % (BALLASTS_TO_INSERT / 10) == 0 {
                    println!("  {:.1}%", key as f64 / BALLASTS_TO_INSERT as f64 * 100.0);
                    println!("    Put: {:?}", durations.finish_put_run());
                }
            }

            println!("  100.0%");
            println!("    Put: {:?}", durations.finish_put_run());

            Ok(())
        })?;
        println!("  Put: {:?}", durations.finish().0);
        println!();
    }

    print_stats(&env)?;
    println!(
        "Appending {LARGE_VALUES_TO_INSERT} large records {LARGE_VALUE_SIZE} bytes each{}...",
        if USE_BALLAST {
            " with ballasts deletion"
        } else {
            ""
        }
    );
    let mut durations = Durations::default();
    for key in 0..LARGE_VALUES_TO_INSERT {
        if USE_BALLAST {
            durations.measure_del(|| {
                with_txn(&env, |txn| {
                    let dbi = txn.open_db(Some(Table::Ballast.as_str()))?.dbi();

                    let mut ballast_cursor = txn.cursor_with_dbi(dbi)?;
                    assert!(ballast_cursor
                        .next::<Cow<'_, [u8]>, [u8; BALLAST_SIZE]>()?
                        .is_some());
                    ballast_cursor.del(WriteFlags::CURRENT)?;

                    Ok(())
                })
            })?;
        }

        durations.measure_put(|| {
            with_txn(&env, |txn| {
                let dbi = txn.open_db(Some(Table::Large.as_str()))?.dbi();

                txn.put(
                    dbi,
                    key.to_be_bytes(),
                    [0; LARGE_VALUE_SIZE],
                    WriteFlags::APPEND,
                )?;

                Ok(())
            })
        })?;

        if key > 0 && key % (LARGE_VALUES_TO_INSERT / 10) == 0 {
            println!(
                "  {:.1}%",
                key as f64 / LARGE_VALUES_TO_INSERT as f64 * 100.0,
            );
            println!("    Put: {:?}", durations.finish_put_run());
            if USE_BALLAST {
                println!("    Del: {:?}", durations.finish_del_run());
            }
        }
    }
    println!("  100.0%");
    println!("    Put: {:?}", durations.finish_put_run());
    if USE_BALLAST {
        println!("    Del: {:?}", durations.finish_del_run());
    }
    let (put, del) = durations.finish();
    println!("  Put: {:?}", put);
    if USE_BALLAST {
        println!("  Del: {:?}", del);
    }
    println!();

    print_stats(&env)?;

    Ok(())
}

fn print_stats(env: &Environment) -> eyre::Result<()> {
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
