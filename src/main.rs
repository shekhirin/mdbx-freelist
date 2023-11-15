mod config;
mod db;
mod duration;

use crate::config::*;
use crate::db::{ballast_key, create_env, create_original_db, large_value_key, with_txn, Table};
use crate::duration::Durations;
use reth_libmdbx::{Environment, EnvironmentKind, WriteFlags};
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
        let file = entry.path();
        std::fs::copy(&file, dir.path().join(file.as_path().file_name().unwrap()))?;
    }
    println!();

    let env = create_env(dir.path())?;

    if USE_BALLAST {
        print_stats(&env)?;
        let mut durations = Durations::default();
        println!(
            "Inserting {BALLAST_VALUES_TO_INSERT} ballasts {BALLAST_VALUE_SIZE} bytes each..."
        );
        with_txn(&env, |txn| {
            let dbi = txn.open_db(Some(Table::Ballast.as_str()))?.dbi();

            for key in 0..BALLAST_VALUES_TO_INSERT {
                durations.measure_put(|| {
                    txn.put(
                        dbi,
                        ballast_key(key),
                        [0; BALLAST_VALUE_SIZE],
                        WriteFlags::empty(),
                    )
                })?;

                if key > 0 && key % (BALLAST_VALUES_TO_INSERT / 10) == 0 {
                    println!(
                        "  {:.1}%",
                        key as f64 / BALLAST_VALUES_TO_INSERT as f64 * 100.0
                    );
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
        "Inserting {LARGE_VALUES_TO_INSERT} records {LARGE_VALUE_SIZE} bytes each{}...",
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
                        .set_range::<Cow<'_, [u8]>, [u8; BALLAST_VALUE_SIZE]>(
                            ballast_key(0).as_ref()
                        )?
                        .map_or(false, |(key, _)| key.starts_with(b"ballast")));
                    ballast_cursor.del(WriteFlags::CURRENT)?;

                    Ok(())
                })
            })?;
        }

        durations.measure_put(|| {
            with_txn(&env, |txn| {
                let dbi = txn.open_db(Some(Table::Data.as_str()))?.dbi();

                txn.put(
                    dbi,
                    large_value_key(key),
                    [0; LARGE_VALUE_SIZE],
                    WriteFlags::empty(),
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
