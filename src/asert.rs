use std::convert::TryInto;
use {
    crate::constants::{ASERT_HALF_LIFE, ASERT_TARGET_HASHES_PER_BLOCK},
    rug::{integer::ParseIntegerError, Integer},
    std::{
        cmp::Ordering,
        fmt,
        num::ParseIntError,
        ops::{Add, AddAssign, Sub},
        str::FromStr,
        time::Duration,
    },
    tw_chain::{crypto::sha3_256, primitives::block::BlockHeader},
};

/// Maps AIBlock PoW parameters to ASERT inputs then computes and returns the
/// PoW target for a given block.
pub fn calculate_asert_target(
    activation_height: u64,
    current_height: u64,
    winning_hashes_since_activation: u64,
) -> CompactTarget {
    assert!(
        current_height > activation_height,
        "ASERT has not activated yet"
    );

    map_asert_inputs(
        activation_height,
        current_height,
        winning_hashes_since_activation,
    )
    .calculate_target()
}

// this exists so that the map_asert_inputs function and the calculation are
// separate. this allows testing of the mapping function in isolation, and
// relatively trivial substition of the mapping function.
struct MappedAsertInputs {
    context: Asert,
    block_height: BlockHeight,
    block_timestamp: Timestamp,
}

impl MappedAsertInputs {
    pub fn calculate_target(&self) -> CompactTarget {
        self.context
            .calculate_target(self.block_height, self.block_timestamp)
    }
}

fn map_asert_inputs(
    activation_height: u64,
    current_height: u64,
    winning_hashes_since_activation: u64,
) -> MappedAsertInputs {
    assert!(
        current_height > activation_height,
        "ASERT has not activated yet"
    );

    // perform some input manipulation to convert hashes-per-block to timestamps.
    // in classic ASERT, there is a fixed number of winning hashes per block (i.e. 1),
    // and a variable timestamp. in this system, there is a fixed block interval,
    // and a variable number of hashes per block.
    // the target returned by ASERT tries to bound the variance of the time,
    // whereas we want to bound the number of winning hashes, and time is fixed.

    // the mapping function needs to convert number of hashes to block time
    // when the difficulty is too low:
    // - classic block time will be earlier than target
    // - our number of hashes will be too high
    // - map an excess in number of hashes to an earlier block time
    // when the difficulty is too high:
    // - classic block time will be later than target
    // - our number of hashes will be too low
    // - map a shortfall of hashes to a later block time

    // so first we need to invert the hash count difference difference, and second we
    // need some function to convert "n hashes too few/many" into "n seconds too long/short".
    // for simplicity we assume the anchor block "timestamp" is zero, and the
    // adjusted "timestamp" should increase by 11 per block.

    let expected_hashes = (current_height - activation_height) * ASERT_TARGET_HASHES_PER_BLOCK;

    let adjusted_asert_timestamp = match expected_hashes.cmp(&winning_hashes_since_activation) {
        Ordering::Less => {
            // we have received more hashes than we would like.
            // difficulty is too easy.
            // adjust the "timestamp" so that it appears "early".

            expected_hashes
                .checked_sub(winning_hashes_since_activation - expected_hashes)
                // this is a particularly low fallback value, and is deliberately not
                // taking TARGET_HASHES_PER_BLOCK into account.
                // if we have so many hashes that the checked sub yields none, we
                // need a fairly aggressive adjustment to the timestamp.
                .unwrap_or(current_height)
        }

        Ordering::Equal => {
            // we have exactly as many hashes as we expect
            expected_hashes
        }

        Ordering::Greater => {
            // we have received fewer hashes than we would like.
            // difficulty is too hard.
            // adjust the "timestamp" so that it appears "late".
            expected_hashes + (expected_hashes - winning_hashes_since_activation)
        }
    };

    let anchor_block_height = BlockHeight::from(activation_height);
    let current_block_height = BlockHeight::from(current_height);
    let current_block_adjusted_timestamp = Timestamp::from(adjusted_asert_timestamp as u32);

    const TARGET_BLOCK_TIME_D: Duration = Duration::from_secs(ASERT_TARGET_HASHES_PER_BLOCK);
    const HALF_LIFE_D: Duration = Duration::from_secs(ASERT_HALF_LIFE);

    let anchor_target = "0xc800ffff".parse().unwrap();

    let context = Asert::with_parameters(TARGET_BLOCK_TIME_D, HALF_LIFE_D)
        .with_anchor(anchor_block_height, anchor_target)
        .having_parent(Timestamp::ZERO);

    MappedAsertInputs {
        context,
        block_height: current_block_height,
        block_timestamp: current_block_adjusted_timestamp,
    }
}

/// Integer height of a block.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockHeight(u64);

impl BlockHeight {
    pub const ZERO: BlockHeight = BlockHeight(0);

    pub fn diff(&self, other: &Self) -> (Ordering, u64) {
        use std::cmp::Ordering::*;
        let ord = self.cmp(other);
        let diff = match ord {
            Less => other.0 - self.0,
            Equal => 0,
            Greater => self.0 - other.0,
        };
        (ord, diff)
    }
}

impl From<u64> for BlockHeight {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Debug for BlockHeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BlockHeight").field(&self.0).finish()
    }
}

impl fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for BlockHeight {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl PartialEq<u64> for BlockHeight {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<BlockHeight> for u64 {
    fn eq(&self, other: &BlockHeight) -> bool {
        other.0.eq(self)
    }
}

impl Sub for BlockHeight {
    type Output = Integer;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = Integer::from(self.0);
        let rhs = Integer::from(rhs.0);
        lhs - rhs
    }
}

/// UNIX timestamp for a block.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(u32);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);

    pub fn diff(&self, other: &Self) -> (Ordering, Duration) {
        use std::cmp::Ordering::*;
        let ord = self.cmp(other);
        let diff = match ord {
            Less => Duration::from_secs((other.0 - self.0) as u64),
            Equal => Duration::ZERO,
            Greater => Duration::from_secs((self.0 - other.0) as u64),
        };
        (ord, diff)
    }
}

impl From<u32> for Timestamp {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Timestamp").field(&self.0).finish()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for Timestamp {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_add(rhs.as_secs() as u32))
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl PartialEq<u32> for Timestamp {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<Timestamp> for u32 {
    fn eq(&self, other: &Timestamp) -> bool {
        other.0.eq(self)
    }
}

impl Sub for Timestamp {
    type Output = Integer;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = Integer::from(self.0);
        let rhs = Integer::from(rhs.0);
        lhs - rhs
    }
}

/// A 32-bit approximation of a 256-bit number that represents the target
/// a block hash must be lower than in order to meet PoW requirements.
///
/// This enconding is made up of an 8-bit exponent and a 24-bit mantissa.
///
/// Bitcoin calls this representation `nBits` in block headers.
///
/// This encoding originates from OpenSSL.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompactTarget(u32);

impl CompactTarget {
    /// This is the easiest difficulty possible. It's roughly equivalent to requiring a single
    /// leading zero byte.
    pub const MAX: CompactTarget = CompactTarget(u32::from_be_bytes([0x22, 0x00, 0x00, 0x01]));

    pub fn expand(&self) -> Target {
        let byte_len = self.0 >> 24;
        let value = self.0 & 0x007fffff;

        let target = if byte_len <= 3 {
            Integer::from(value) >> 8 * (3 - byte_len)
        } else {
            Integer::from(value) << 8 * (byte_len - 3)
        };

        // todo: some stuff around negative number stuffing

        Target(target)
    }

    pub fn expand_integer(&self) -> Integer {
        self.expand().0
    }

    pub fn into_array(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }

    pub fn from_array(array: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(array))
    }

    pub fn try_from_slice(slice: &[u8]) -> Option<Self> {
        // This requires that the slice's length is exactly 4
        Some(Self::from_array(slice.try_into().ok()?))
    }
}

impl FromStr for CompactTarget {
    type Err = ParseIntError;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        // we accept strings starting with a leading '0x'
        if s.starts_with("0x") {
            s = &s[2..];
        }

        u32::from_str_radix(s, 16).map(Self)
    }
}

impl fmt::Debug for CompactTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactTarget({:#010x})", &self.0)
    }
}

impl fmt::Display for CompactTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#010x}", &self.0)
    }
}

impl CompactTarget {
    pub fn _expand(&self) -> Target {
        let e0 = self.0 >> 24;
        let m0 = self.0 & 0xffffff;
        let (m, e) = if e0 <= 3 {
            let m = m0 >> (8 * 3u32 - e0);
            (m, 0)
        } else {
            let e = 8 * (e0 - 3);
            (m0, e)
        };

        let expanded = if m > 0x7fffff {
            Integer::ZERO
        } else {
            Integer::from(m) << e
        };

        Target(expanded)
    }
}

pub struct HeaderHash(sha3_256::Output<sha3::Sha3_256>);

impl HeaderHash {
    pub fn try_calculate(header: &BlockHeader) -> Option<Self> {
        let serialized = bincode::serialize(header).ok()?;
        let hashed = sha3_256::digest(&serialized);
        Some(Self(hashed))
    }

    pub fn is_below_target(&self, target: &Target) -> bool {
        let h_int = Integer::from_digits(self.0.as_slice(), rug::integer::Order::MsfBe);
        println!("h_int: {:?}", h_int);
        println!("target: {:?}", target.0);
        println!("h_int <= target.0: {:?}", h_int <= target.0);
        h_int <= target.0
    }

    pub fn is_below_compact_target(&self, target: &CompactTarget) -> bool {
        let target = target.expand();
        println!("target: {:?}", target);
        self.is_below_target(&target)
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl From<sha3_256::Output<sha3::Sha3_256>> for HeaderHash {
    fn from(value: sha3_256::Output<sha3::Sha3_256>) -> Self {
        Self(value)
    }
}

/// The expanded integer from of a `CompactTarget` that represents the target
/// a block hash must be lower than in order to meet PoW requirements.
///
/// Bitcoin refers to this as the _target_.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Target(Integer);

impl From<CompactTarget> for Target {
    fn from(value: CompactTarget) -> Self {
        value.expand()
    }
}

impl FromStr for Target {
    type Err = ParseIntegerError;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        // we accept strings starting with a leading '0x'
        if s.starts_with("0x") {
            s = &s[2..];
        }

        Integer::from_str_radix(s, 16).map(Self)
    }
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Target({:#034x})", &self.0)
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#034x}", &self.0)
    }
}

impl Target {
    /// Returns the expansion of `CompactTarget::MAX`.
    pub fn max() -> Self {
        CompactTarget::MAX.expand()
    }

    /// Returns the compacted form of `self`.
    ///
    /// Note that this is usually lossy as `Target` represents
    /// values with much greater precision than `CompactTarget`.
    pub fn compact(&self) -> CompactTarget {
        let mut e = (self.0.signed_bits() + 7) / 8;

        // we need E bytes to represent the number
        // we have 3 bytes to put the most significant bits into
        // we also have to do something about negative numbers.

        let mut m = if e <= 3 {
            let m0 = self
                .0
                .clone()
                .clamp(&0u32, &u32::MAX)
                .to_u64()
                // unwrap is safe because we've just clamped it
                .unwrap();
            (m0 << (8 * (3 - e))) as u32
        } else {
            let m0 = self.0.clone() >> (8 * (e - 3));
            m0.clamp(&0u32, &u32::MAX)
                .to_u32()
                // unwrap is safe because we've just clamped it
                .unwrap()
        };

        // targets are unsigned; ensure that the lead digit does not have
        // the signed bit set, as this may be expanded out into a
        // arbitrary-sized _signed_ integer.
        if (m & 0x00800000) != 0 {
            m >>= 8;
            e += 1;
        }

        let compact = m | (e << 24);

        CompactTarget(compact)
    }
}

/// ASERT3-2D calculation, as implemented around 2020 in Bitcoin ABC node.
///
/// * [Spec on GitHub](https://github.com/bitcoincashorg/bitcoincash.org/blob/master/spec/2020-11-15-asert.md).
/// * [Java implementation](https://github.com/pokkst/asert-java/blob/master/src/main/java/xyz/pokkst/asert/Asert.java).
/// * [Bitcoin ABC implementation](https://reviews.bitcoinabc.org/rABC5b4c8ede6a9bff82ff9dc2e54b62aa4b7ee4d67e).
/// * [Bitcoin Cash Node implementation](https://gitlab.com/bitcoin-cash-node/bitcoin-cash-node/-/blob/master/src/pow.cpp).
///
/// This implementation is based on the C++ implementation found in Bitcoin ABC and Bitcoin Cash Node, as this
/// is the implementation that has been used in production for several years, and is therefore the most
/// "battle tested" implementation.
fn calculate_next_target(
    anchor_height: BlockHeight,
    anchor_parent_time: Timestamp,
    anchor_target: CompactTarget,
    current_height: BlockHeight,
    current_time: Timestamp,
    target_block_time: Duration,
    halflife: Duration,
) -> CompactTarget {
    assert!(!halflife.is_zero(), "half-life cannot be zero");

    // spec doesn't make a comment on time travelling, but a test vector relies on it
    let time_diff = i64::from(current_time.0) - i64::from(anchor_parent_time.0);

    // bch node enforces this ordering, whereas spec says negative diffs are fine
    let height_diff = current_height
        .0
        .checked_sub(anchor_height.0)
        .expect("anchor after current");

    // todo: check pow limit on anchor block

    let exponent = ((i64::from(time_diff)
        - (target_block_time.as_secs() as i64) * ((height_diff + 1) as i64))
        * 65536)
        / (halflife.as_secs() as i64);

    assert_eq!(-1_i64 >> 1, -1_i64, "ASERT2 needs arithmetic shift support");

    let mut shifts = (exponent >> 16) as i64;
    let frac = exponent as u16;

    assert_eq!(
        ((shifts as i64) * 65536_i64) + i64::from(frac),
        exponent,
        "fracwang"
    );

    let frac = u64::from(frac);
    let factor = 65536u64
        + ((195766423245049_u64 * frac
            + 971821376_u64 * frac * frac
            + 5127_u64 * frac * frac * frac
            + (1_u64 << 47))
            >> 48);

    let mut next_target = anchor_target.expand().0 * factor;

    shifts -= 16;
    if shifts < 0 {
        next_target >>= shifts.abs() as usize;
    } else {
        let shifted_target = next_target.clone() << shifts as usize;
        if (shifted_target.clone() >> (shifts as usize)) != next_target {
            return CompactTarget::MAX;
        } else {
            next_target = shifted_target;
        }
    }

    if next_target.is_zero() {
        Target(Integer::from(1)).compact()
    } else if next_target > Target::max().0 {
        CompactTarget::MAX
    } else {
        Target(next_target).compact()
    }
}

#[doc(hidden)]
pub struct AsertParameters {
    target_block_time: Duration,
    halflife: Duration,
}

impl AsertParameters {
    pub fn with_anchor(self, height: BlockHeight, target: CompactTarget) -> RequiresParent {
        RequiresParent {
            target_block_time: self.target_block_time,
            halflife: self.halflife,
            reference_height: height,
            reference_target: target,
        }
    }
    pub fn with_genesis_anchor(self, target: CompactTarget, timestamp: Timestamp) -> Asert {
        Asert {
            target_block_time: self.target_block_time,
            halflife: self.halflife,
            reference_height: BlockHeight::ZERO,
            reference_target: target,
            reference_timestamp: timestamp,
        }
    }
}

#[doc(hidden)]
pub struct RequiresParent {
    target_block_time: Duration,
    halflife: Duration,
    reference_height: BlockHeight,
    reference_target: CompactTarget,
}

impl RequiresParent {
    pub fn having_parent(self, parent_timestamp: Timestamp) -> Asert {
        Asert {
            target_block_time: self.target_block_time,
            halflife: self.halflife,
            reference_height: self.reference_height,
            reference_target: self.reference_target,
            reference_timestamp: parent_timestamp,
        }
    }
}

/// A parameterised instance of the ASERTI3-2D algorithm.
///
/// # Usage
///
/// To use the ASERT algorithm to calculate a block's target:
///
/// 1. Create an ASERT context
/// 2. Supply static parameters
/// 3. Configure an anchor
/// 4. Calculate the target
///
/// Note that the context returned after (3) can be kept and
/// re-used; it is not necessary to construct a context for
/// every calculation.
///
/// ## Creating An ASERT Context
///
/// The ASERT algorithm requires three parts:
/// * Static parameters
/// * An anchor, representing the point in time at which
///   the ASERT DAA activated, comprising of either:
///   * For a non-genesis anchor:
///     * The height and target of the anchor block
///     * The timestamp of the block prior to the anchor block
///   * For anchoring at the genesis block:
///     * the height, target, and timestamp of the genesis block
/// * A block for which the target is to be calculated
///   * Block Height
///   * Timestamp
///
/// The `Asert` struct provides a builder-like interface for constructing
/// a calculation context. Once constructed, only the third part above
/// must be specified on a per-call basis.
///
/// ### Static parameters
///
/// The following static parameters are required:
///
/// * Block Interval, target number of seconds between blocks
/// * Half-Life, the period at which, given a doubling of the
///   number of blocks produced, the difficulty doubles (i.e.
///   the target halves)
///
/// #### BCH Static Parameters
///
/// These are the default parameters as ASERT is deployed on the BCH network.
///
/// This implementation contains default parameters:
/// * Target Block Interval of 30 seconds
/// * Half-Life of 2 days
///
/// ```
/// use aserti3_2d::*;
///
/// let asert = Asert::with_bch_parameters()
///     // configure anchor etc
/// # ;
/// ```
///
/// #### Supplying alternate static parameters
///
/// To use alternative parameters, use the following function:
///
/// ```
/// use aserti3_2d::*;
/// use std::time::Duration;
///
/// let target_block_time = Duration::from_secs(30);
/// let halflife = Duration::from_secs(2 * 24 * 60 * 60);
///
/// let asert = Asert::with_parameters(target_block_time, halflife)
///     // configure anchor etc
/// # ;
/// ```
///
/// ### Specifying An Anchor
///
/// The algorithm works by comparing some fixed point in the past, represented
/// by an anchor block, to some block after that fixed point in time.
///
/// The anchor may either be the genesis block, or some later block in the chain.
///
/// * For anchoring at the genesis block:
///   * the height, target, and timestamp of the genesis block
/// * For a non-genesis anchor:
///   * The height and target of the anchor block
///   * The timestamp of the block prior to the anchor block
///
/// #### Anchoring to the genesis block
///
/// According to the ASERT specification, when anchoring to genesis,
/// the timestamp of the genesis block is used rather than the timestamp
/// of the block prior to the anchor. This is because there is no block
/// prior to the genesis block.
///
/// ```
/// use aserti3_2d::*;
///
/// // let genesis_target = genesis.target;
/// // let genesis_timestamp = genesis.timestamp;
/// # let genesis_target = CompactTarget::MAX;
/// # let genesis_timestamp = Timestamp::UNIX_EPOCH;
///
/// let asert = Asert::with_bch_parameters()
///     .with_genesis_anchor(genesis_target, genesis_timestamp);
/// ```
///
/// #### Anchoring to a non-genesis block
///
/// When anchoring to a non-genesis block, the anchor block's
/// height and target are required, along with the timestamp of
/// the anchor block's parent.
///
/// ```
/// use aserti3_2d::*;
///
/// // let height = anchor.height;
/// // let target = anchor.target;
/// // let parent_timestamp = anchor.parent.timestamp;
/// # let height = BlockHeight::ZERO;
/// # let target = CompactTarget::MAX;
/// # let parent_timestamp = Timestamp::UNIX_EPOCH;
///
/// let asert = Asert::with_bch_parameters()
///     .with_anchor(height, target)
///     .having_parent(parent_timestamp);
/// ```
///
/// ## Calculating A Block Target
///
/// Once an ASERT context is established, the target for a given
/// block may be calculated by calling `Asert::calculate_target`,
/// passing the current block's height and timestamp.
///
/// ```
/// use aserti3_2d::*;
///
/// // create a context as previously demonstrated
/// // let asert = Asert::...
/// # let genesis_target = CompactTarget::MAX;
/// # let genesis_timestamp = Timestamp::UNIX_EPOCH;
/// # let asert = Asert::with_bch_parameters()
/// #    .with_genesis_anchor(genesis_target, genesis_timestamp);
///
/// // get block parameters
/// // let height = current_block.height;
/// // let timestamp = current_block.timestamp;
/// # let height = BlockHeight::from(10);
/// # let timestamp = Timestamp::from(1000);
///
/// let target = asert.calculate_target(height, timestamp);
/// println!("target for block {height} with timestamp {timestamp}: {target}");
/// ```
///
#[derive(Debug)]
pub struct Asert {
    target_block_time: Duration,
    halflife: Duration,
    reference_height: BlockHeight,
    reference_target: CompactTarget,
    reference_timestamp: Timestamp,
}

impl Asert {
    /// Begins context creation with default static parameters.
    ///
    /// See `Asert` struct documentation for details.
    pub fn with_bch_parameters() -> AsertParameters {
        AsertParameters {
            target_block_time: Duration::from_secs(30),
            halflife: Duration::from_secs(2 * 24 * 60 * 60),
        }
    }

    /// Begins context creation with custom static parameters.
    ///
    /// See `Asert` struct documentation for details.
    pub fn with_parameters(target_block_time: Duration, halflife: Duration) -> AsertParameters {
        AsertParameters {
            target_block_time,
            halflife,
        }
    }

    /// Calculates and returns the target for a block with given height and timestamp.
    ///
    /// See `Asert` struct documentation for details.
    pub fn calculate_target(&self, height: BlockHeight, timestamp: Timestamp) -> CompactTarget {
        calculate_next_target(
            self.reference_height,
            self.reference_timestamp,
            self.reference_target,
            height,
            timestamp,
            self.target_block_time,
            self.halflife,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod target_conversions {
        use super::*;

        fn test_target_expansion(
            compact_s: &'static str,
            expanded_s: &'static str,
            compacted_s: &'static str,
        ) {
            let target: Target = expanded_s.parse().expect("setup: parse expanded target");

            let compact: CompactTarget = compact_s.parse().expect("setup: parse compact target");

            let roundtrip: CompactTarget = compacted_s
                .parse()
                .expect("setup: parse compacted target after roundtrip");

            let expanded = compact.expand();

            assert_eq!(target, expanded, "expand {}", compact_s);

            let compacted = target.compact();

            assert_eq!(roundtrip, compacted, "compact {}", compact_s);
        }

        #[test]
        fn target_expansion_0x17073039() {
            // test case from https://bitcoin.stackexchange.com/a/117269
            test_target_expansion(
                "0x17073039",
                "0x730390000000000000000000000000000000000000000",
                "0x17073039",
            )
        }

        #[test]
        fn target_expansion_0x1b0404cb() {
            // test case from https://en.bitcoin.it/wiki/Difficulty
            // happens to be bitcoin block 100,800
            test_target_expansion(
                "0x1b0404cb",
                "0x00000000000404CB000000000000000000000000000000000000000000000000",
                "0x1b0404cb",
            )
        }

        #[test]
        fn target_expansion_0x1d00ffff() {
            // max possible value, according to bitcoin wiki
            // this is what was used in the bitcoin genesis block
            test_target_expansion(
                "0x1d00ffff",
                "0x00000000FFFF0000000000000000000000000000000000000000000000000000",
                "0x1d00ffff",
            )
        }

        // bitcoin block test cases extracted from https://learnmeabitcoin.com/technical/block/bits/

        #[test]
        fn target_expansion_0x170355f0() {
            // test case: bitcoin block 845,098
            test_target_expansion(
                "170355f0",
                "0000000000000000000355f00000000000000000000000000000000000000000",
                "170355f0",
            )
        }

        #[test]
        fn target_expansion_0x1824dbe9() {
            // test case: bitcoin block 320,544
            test_target_expansion(
                "1824dbe9",
                "000000000000000024dbe9000000000000000000000000000000000000000000",
                "1824dbe9",
            )
        }

        #[test]
        fn target_expansion_0x1b0e7256() {
            // test case: bitcoin block 90,720
            test_target_expansion(
                "1b0e7256",
                "00000000000e7256000000000000000000000000000000000000000000000000",
                "1b0e7256",
            )
        }

        // bitcoin core test cases
        // https://developer.bitcoin.org/reference/block_chain.html#target-nbits

        #[test]
        fn target_expansion_0x01003456() {
            test_target_expansion("0x01003456", "0x00", "0x01000000")
        }

        #[test]
        fn target_expansion_0x01123456() {
            test_target_expansion("0x01123456", "0x12", "0x01120000")
        }

        #[test]
        fn target_expansion_0x02008000() {
            test_target_expansion("0x02008000", "0x80", "0x02008000")
        }

        #[test]
        fn target_expansion_0x05009234() {
            test_target_expansion("0x05009234", "0x92340000", "0x05009234")
        }

        #[test]
        #[ignore = "we don't even handle negative numbers"]
        fn target_expansion_0x04923456_ve() {
            test_target_expansion("0x04923456", "-12345600", "0x04923456")
        }

        #[test]
        fn target_expansion_0x04123456() {
            test_target_expansion("0x04123456", "0x12345600", "0x04123456")
        }
    }

    #[test]
    fn max_compact_target() {
        assert_eq!(
            CompactTarget::from_str("0x1d00ffff").expect("setup: parse max"),
            CompactTarget::MAX
        );
    }

    mod aserti3_2d_bch_test_cases {
        use super::*;

        struct TestVector {
            description: &'static str,
            anchor_height: BlockHeight,
            anchor_ancestor_time: Timestamp,
            anchor_target: CompactTarget,
            start_height: BlockHeight,
            start_time: Timestamp,
            iterations: usize,
            items: Vec<TestVectorItem>,
        }

        struct TestVectorItem {
            iteration: usize,
            height: BlockHeight,
            time: Timestamp,
            target: CompactTarget,
        }

        fn parse(vector: &'static str) -> TestVector {
            #[derive(Default)]
            struct VectorBuilder {
                description: Option<&'static str>,
                anchor_height: Option<BlockHeight>,
                anchor_ancestor_time: Option<Timestamp>,
                anchor_target: Option<CompactTarget>,
                start_height: Option<BlockHeight>,
                start_time: Option<Timestamp>,
                iterations: Option<usize>,
                items: Vec<TestVectorItem>,
            }

            impl VectorBuilder {
                fn build(self) -> TestVector {
                    let v = TestVector {
                        description: self.description.expect("missing description"),
                        anchor_height: self.anchor_height.expect("missing anchor_height"),
                        anchor_ancestor_time: self
                            .anchor_ancestor_time
                            .expect("missing anchor_ancestor_time"),
                        anchor_target: self.anchor_target.expect("missing anchor_target"),
                        start_height: self.start_height.expect("missing start_height"),
                        start_time: self.start_time.expect("missing start_time"),
                        iterations: self.iterations.expect("missing iterations"),
                        items: self.items,
                    };

                    // there's duplicate data in the source files;
                    // check that it lines up. failures might indicate
                    // parser errors that would affect the correctness
                    // of the tests.

                    assert_eq!(v.iterations, v.items.len(), "wrong number of iterations",);

                    if let Some(first) = v.items.iter().nth(0) {
                        assert_eq!(v.start_height, first.height, "wrong start height");
                        assert_eq!(v.start_time, first.time, "wrong start time");
                    }

                    v
                }
            }

            #[derive(Default)]
            struct ItemBuilder {
                iteration: Option<usize>,
                height: Option<BlockHeight>,
                time: Option<Timestamp>,
                target: Option<CompactTarget>,
            }

            impl ItemBuilder {
                fn build(self) -> TestVectorItem {
                    TestVectorItem {
                        iteration: self.iteration.expect("missing iteration"),
                        height: self.height.expect("missing height"),
                        time: self.time.expect("missing time"),
                        target: self.target.expect("missing target"),
                    }
                }
            }

            let mut builder = VectorBuilder::default();
            for line in vector.lines() {
                if line.starts_with("##") {
                    let line = &line[2..];
                    let mut parts = line.splitn(2, ':');
                    let Some(k) = parts.next() else {
                        continue;
                    };
                    let Some(v) = parts.next() else {
                        continue;
                    };

                    let k = k.trim();
                    let v = v.trim();

                    match k {
                        "description" => builder.description = Some(v.trim()),
                        "anchor height" => builder.anchor_height = v.parse().ok(),
                        "anchor ancestor time" | "anchor parent time" => {
                            builder.anchor_ancestor_time = v.parse().ok()
                        }
                        "anchor nBits" => builder.anchor_target = v.parse().ok(),
                        "start height" => builder.start_height = v.parse().ok(),
                        "start time" => builder.start_time = v.parse().ok(),
                        "iterations" => builder.iterations = v.parse().ok(),
                        other => panic!("unexpected key: {}", other),
                    }
                } else if line.starts_with("#") {
                    assert_eq!(
                        "# iteration,height,time,target",
                        line.trim(),
                        "unexpected field layout"
                    );
                } else {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let mut parts = line.split_whitespace();
                    let mut item = ItemBuilder::default();
                    item.iteration = parts.next().map(|n| n.parse().ok()).flatten();
                    item.height = parts.next().map(|n| n.parse().ok()).flatten();
                    item.time = parts.next().map(|n| n.parse().ok()).flatten();
                    item.target = parts.next().map(|n| n.parse().ok()).flatten();
                    builder.items.push(item.build());
                }
            }

            builder.build()
        }

        fn execute(vector: TestVector, name: &'static str) {
            // bch selected constants
            const TARGET_BLOCK_TIME: Duration = Duration::from_secs(10 * 60);
            const HALFLIFE: Duration = Duration::from_secs(2 * 24 * 60 * 60);

            for item in vector.items.into_iter() {
                let actual = calculate_next_target(
                    vector.anchor_height,
                    vector.anchor_ancestor_time,
                    vector.anchor_target,
                    item.height,
                    item.time,
                    TARGET_BLOCK_TIME,
                    HALFLIFE,
                );

                // temporary: rule out compact conversion as the cause of test failures
                assert_eq!(
                    item.target, actual,
                    "{name} iteration {} ({})",
                    item.iteration, vector.description,
                )
            }
        }

        #[test]
        fn run01() {
            let vector = parse(include_str!("test_data/asert/run01"));
            execute(vector, "run01");
        }

        #[test]
        fn run02() {
            let vector = parse(include_str!("test_data/asert/run02"));
            execute(vector, "run02");
        }

        #[test]
        fn run03() {
            let vector = parse(include_str!("test_data/asert/run03"));
            execute(vector, "run03");
        }

        #[test]
        fn run04() {
            let vector = parse(include_str!("test_data/asert/run04"));
            execute(vector, "run04");
        }

        #[test]
        fn run05() {
            let vector = parse(include_str!("test_data/asert/run05"));
            execute(vector, "run05");
        }

        #[test]
        fn run06() {
            let vector = parse(include_str!("test_data/asert/run06"));
            execute(vector, "run06");
        }

        #[test]
        fn run07() {
            let vector = parse(include_str!("test_data/asert/run07"));
            execute(vector, "run07");
        }

        #[test]
        fn run08() {
            let vector = parse(include_str!("test_data/asert/run08"));
            execute(vector, "run08");
        }

        #[test]
        fn run09() {
            let vector = parse(include_str!("test_data/asert/run09"));
            execute(vector, "run09");
        }

        #[test]
        fn run10() {
            let vector = parse(include_str!("test_data/asert/run10"));
            execute(vector, "run10");
        }

        #[test]
        fn run11() {
            let vector = parse(include_str!("test_data/asert/run11"));
            execute(vector, "run11");
        }

        #[test]
        fn run12() {
            let vector = parse(include_str!("test_data/asert/run12"));
            execute(vector, "run12");
        }
    }
}
