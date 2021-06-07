use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchStatus {
    Contiguous(u64),
    NotContiguous(u64),
}

pub struct StorageFetch {
    /// Last block we have full history
    last_contiguous_block_num: Option<u64>,
}

impl StorageFetch {
    /// Initialize with database info
    pub fn new() -> Self {
        Self {
            last_contiguous_block_num: None,
        }
    }

    /// Set intial info loaded from db
    pub fn set_initial_last_contiguous_block_key(
        &mut self,
        last_contiguous_block_num: Option<u64>,
    ) {
        self.last_contiguous_block_num = last_contiguous_block_num;
    }

    /// Get next contiguous tracking
    pub fn check_contiguous_block_num(&self, block_num: u64) -> FetchStatus {
        let next_contiguous = self.last_contiguous_block_num.map(|v| v + 1);
        if block_num == next_contiguous.unwrap_or_default() {
            FetchStatus::Contiguous(block_num)
        } else {
            FetchStatus::NotContiguous(block_num)
        }
    }

    /// Update contiguous tracking
    pub fn update_contiguous_block_num(&mut self, status: FetchStatus) {
        if let FetchStatus::Contiguous(block_num) = status {
            self.last_contiguous_block_num = Some(block_num);
        }
    }
}

impl fmt::Debug for StorageFetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageFetch({:?})", self.last_contiguous_block_num,)
    }
}
