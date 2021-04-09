use crate::db_utils::SimpleDbSpec;

pub mod constants {
    pub const NETWORK_VERSION_SERIALIZED: Option<&[u8]> = None;
    pub const DB_PATH: &str = "src/db/db";
}

pub mod compute {
    use super::*;

    pub const REQUEST_LIST_KEY: &str = "RequestListKey";

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".compute",
        columns: &[],
    };
}
