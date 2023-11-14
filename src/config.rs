pub const USE_LIFO: bool = true;
pub const DELETE_SMALL_VALUES: bool = true;
pub const USE_BALLAST: bool = true;

pub const SMALL_VALUES_TO_INSERT: usize = 5_000_000;
pub const SMALL_VALUE_SIZE: usize = 2 * 1024;
pub const SMALL_VALUES_TO_DELETE: usize = 4_000_000;

pub const LARGE_VALUES_TO_INSERT: usize = 1_000;
pub const LARGE_VALUE_SIZE: usize = 300 * 1024;

pub const BALLAST_VALUES_TO_INSERT: usize = 20_000;
pub const BALLAST_VALUES_TO_USE: usize = 10_000;
pub const BALLAST_VALUE_SIZE: usize = 300 * 1024;

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
