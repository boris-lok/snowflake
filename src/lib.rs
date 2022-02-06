use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A distributed unique ID generator inspired by Twitter's Snowflake.
/// I mimic the algorithm and translate it from Scala to rust.
/// The following url is the source. Thanks to Twitter Snowflake authors.
/// ref url: https://github.com/twitter-archive/snowflake
pub struct SnowflakeGenerator {
    /// Time cut-off
    last_time_millis: u128,

    /// Work Machine ID (0-31)
    worker_id: u8,

    /// Data Center ID (0-31)
    data_center_id: u8,

    /// Sequences in milliseconds (0 - 4095)
    sequence: u16,

    /// The time wanted to cut-off
    timestamp_offset: u128,
}

impl SnowflakeGenerator {
    /// Number of digits occupied by machine id
    const WORKER_ID_BITS: u8 = 5;

    /// Number of digits occupied by the data center identifier id
    const DATA_CENTER_BITS: u8 = 5;

    /// Number of digits occupied by sequence
    const SEQUENCE_BITS: u8 = 12;

    /// Supported maximum machine id, the result is 31.
    ///
    /// this shift algorithm can quickly calculate the maximum decimal number represented by
    /// serveral bits of binary number
    const MAX_WORK_ID: i8 = -1 ^ (-1 << SnowflakeGenerator::WORKER_ID_BITS);

    /// Supported maximum data identifier id, the result is 31.
    const MAX_DATA_CENTER_ID: i8 = -1 ^ (-1 << SnowflakeGenerator::DATA_CENTER_BITS);

    /// The mask of the generated sequence is 4095 (0b111111111111111111111 = 0xfff = 4095)
    const SEQUENCE_MASK: i16 = -1 ^ (-1 << SnowflakeGenerator::SEQUENCE_BITS);

    /// Time truncate moves 22 bits to the left (5 + 5 + 12)
    const TIMESTAMP_LEFT_SHIFT: u8 = SnowflakeGenerator::SEQUENCE_BITS
        + SnowflakeGenerator::WORKER_ID_BITS
        + SnowflakeGenerator::DATA_CENTER_BITS;

    /// Create SnowflakeGenerator
    /// Please make sure that worker_id and data_center_id is between 0 - 31.
    ///
    /// # Example
    /// ```
    /// use snowflake::SnowflakeGenerator;
    ///
    /// let mut generator = SnowflakeGenerator::new(0, 0, 0);
    /// ```
    pub fn new(worker_id: u8, data_center_id: u8, timestamp_offset: u128) -> Self {
        if worker_id as i8 > SnowflakeGenerator::MAX_WORK_ID {
            panic!(
                "worker id must be between 0 - {}",
                SnowflakeGenerator::MAX_WORK_ID
            );
        }

        if data_center_id as i8 > SnowflakeGenerator::MAX_DATA_CENTER_ID {
            panic!(
                "data center id must be between 0 - {}",
                SnowflakeGenerator::MAX_DATA_CENTER_ID
            );
        }

        Self {
            worker_id,
            data_center_id,
            sequence: 0,
            timestamp_offset,
            last_time_millis: SnowflakeGenerator::get_current_timestamp(timestamp_offset),
        }
    }

    /// Get the next id.
    /// This function will panic if the system time has changed and the time is less than generator
    /// last_time_millis
    ///
    /// # Example
    /// ```
    /// use snowflake::SnowflakeGenerator;
    ///
    /// let mut generator = SnowflakeGenerator::new(0,0,0);
    /// let id = generator.next_id();
    /// ```
    pub fn next_id(&mut self) -> u128 {
        let mut now = SnowflakeGenerator::get_current_timestamp(self.timestamp_offset);

        if now < self.last_time_millis {
            panic!(
                "Clock moved backwards, refusing to generate id for {} milliseconds.",
                self.last_time_millis - now
            );
        }

        if self.last_time_millis == now {
            self.sequence =
                (((self.sequence + 1) as i16) % SnowflakeGenerator::SEQUENCE_MASK) as u16;
            if self.sequence == 0 {
                now = SnowflakeGenerator::til_next_milliseconds(
                    self.last_time_millis,
                    self.timestamp_offset,
                );
            }
        } else {
            self.sequence = 0;
        }

        self.last_time_millis = now;

        (self.last_time_millis << SnowflakeGenerator::TIMESTAMP_LEFT_SHIFT) as u128
            | (self.data_center_id << SnowflakeGenerator::DATA_CENTER_BITS) as u128
            | (self.worker_id << SnowflakeGenerator::WORKER_ID_BITS) as u128
            | self.sequence as u128
    }

    /// Block to the next milliseconds until a new timestamp is obtained.
    fn til_next_milliseconds(last_time_millis: u128, offset: u128) -> u128 {
        loop {
            let now = SnowflakeGenerator::get_current_timestamp(offset);
            if now > last_time_millis {
                return now;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    /// Get the current timestamp in milliseconds.
    fn get_current_timestamp(offset: u128) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Can't get current timestamp")
            .as_millis()
            - offset
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let mut generator = super::SnowflakeGenerator::new(0, 0, 0);
        let id = generator.next_id();

        assert!(id > 0);
    }
}
