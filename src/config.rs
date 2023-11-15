pub const USE_LIFO: bool = true;
pub const USE_BALLAST: bool = true;

pub const SMALL_VALUES_TO_INSERT: usize = 5_000_000;
pub const SMALL_VALUE_SIZE: usize = 2 * 1024;
pub const SMALL_VALUES_TO_DELETE: usize = 4_000_000;

pub const LARGE_VALUES_TO_INSERT: usize = 100;
pub const LARGE_VALUE_SIZE: usize = 300 * 1024;

pub const BALLAST_VALUES_TO_LARGE_VALUES_RATIO: f64 = 1.0;
pub const BALLAST_VALUE_SIZE_TO_LARGE_VALUE_SIZE_RATIO: f64 = 1.0;
pub const BALLAST_VALUES_TO_INSERT: usize =
    (LARGE_VALUES_TO_INSERT as f64 * BALLAST_VALUES_TO_LARGE_VALUES_RATIO) as usize;
pub const BALLAST_VALUE_SIZE: usize =
    (LARGE_VALUE_SIZE as f64 * BALLAST_VALUE_SIZE_TO_LARGE_VALUE_SIZE_RATIO) as usize;

pub fn print_config() {
    println!("USE_LIFO = {USE_LIFO}, USE_BALLAST = {USE_BALLAST}");
}
