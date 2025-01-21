use crate::miner_pow::vulkan::sha3_256_shader::SHA3_KECCAK_SPONGE_WORDS;
use crate::miner_pow::{
    MineError, MinerStatistics, PoWDifficulty, Sha3_256PoWMiner, BLOCK_HEADER_MAX_BYTES,
    SHA3_256_BYTES,
};
use crate::utils::split_range_into_blocks;
use crevice::std140::AsStd140;
use crevice::std430::AsStd430;
use debug_ignore::DebugIgnore;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;
use vulkano::buffer::{Buffer, BufferCreateInfo, BufferUsage, Subbuffer};
use vulkano::command_buffer::allocator::{
    StandardCommandBufferAllocator, StandardCommandBufferAllocatorCreateInfo,
};
use vulkano::command_buffer::{AutoCommandBufferBuilder, CommandBufferUsage};
use vulkano::descriptor_set::allocator::StandardDescriptorSetAllocator;
use vulkano::descriptor_set::layout::{
    DescriptorSetLayout, DescriptorSetLayoutBinding, DescriptorSetLayoutCreateInfo, DescriptorType,
};
use vulkano::descriptor_set::{PersistentDescriptorSet, WriteDescriptorSet};
use vulkano::device::{Device, DeviceCreateInfo, Features, Queue, QueueCreateInfo, QueueFlags};
use vulkano::instance::{Instance, InstanceCreateInfo};
use vulkano::memory::allocator::{AllocationCreateInfo, MemoryTypeFilter, StandardMemoryAllocator};
use vulkano::pipeline::compute::ComputePipelineCreateInfo;
use vulkano::pipeline::layout::{PipelineDescriptorSetLayoutCreateInfo, PipelineLayoutCreateInfo};
use vulkano::pipeline::{
    ComputePipeline, Pipeline, PipelineBindPoint, PipelineLayout, PipelineShaderStageCreateInfo,
};
use vulkano::shader::{ShaderModule, ShaderStages, SpecializationConstant};
use vulkano::sync::GpuFuture;
use vulkano::*;

// synced with vulkan_miner.glsl
const WORK_GROUP_SIZE: u32 = 256;

#[derive(Clone, Debug)]
pub enum VulkanMinerError {
    Load(String),
    Instance(Validated<VulkanError>),
    EnumerateDevices(VulkanError),
    NoDevices,
    NoComputeQueues,
    Device(Validated<VulkanError>),
}

impl std::error::Error for VulkanMinerError {}

impl fmt::Display for VulkanMinerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Load(cause) => write!(f, "Failed to load Vulkan library: {cause}"),
            Self::Instance(cause) => write!(f, "Failed to create Vulkan instance: {cause}"),
            Self::EnumerateDevices(cause) => {
                write!(f, "Failed to enumerate Vulkan devices: {cause}")
            }
            Self::NoDevices => f.write_str("No Vulkan devices found!"),
            Self::NoComputeQueues => f.write_str("No compute queue family found!"),
            Self::Device(cause) => write!(f, "Failed to create Vulkan device: {cause}"),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
struct VulkanMinerVariant {
    trailing_bytes_count: u32,
    digest_initial_byte_index: u32,
    digest_initial_word_index: u32,
    difficulty_function: u32,
    leading_zeroes_mining_difficulty: u32,
}

impl VulkanMinerVariant {
    pub fn specialization_constants(
        &self,
    ) -> impl IntoIterator<Item = (u32, SpecializationConstant)> {
        [
            (
                0,
                SpecializationConstant::U32(BLOCK_HEADER_MAX_BYTES as u32),
            ),
            (1, SpecializationConstant::U32(self.trailing_bytes_count)),
            (
                5,
                SpecializationConstant::U32(self.digest_initial_byte_index),
            ),
            (
                6,
                SpecializationConstant::U32(self.digest_initial_word_index),
            ),
            (10, SpecializationConstant::U32(self.difficulty_function)),
            (
                11,
                SpecializationConstant::U32(self.leading_zeroes_mining_difficulty),
            ),
        ]
    }
}

#[derive(Debug)]
pub struct VulkanMiner {
    uniform_buffer: Subbuffer<<MineUniforms as AsStd140>::Output>,
    response_buffer: Subbuffer<<MineResponse as AsStd430>::Output>,
    memory_allocator: Arc<StandardMemoryAllocator>,

    base_compute_shader: Arc<ShaderModule>,
    compute_pipelines: BTreeMap<VulkanMinerVariant, Arc<ComputePipeline>>,
    work_group_size: u32,

    descriptor_set_layout_index: usize,
    descriptor_set: DebugIgnore<Arc<PersistentDescriptorSet>>,

    queue: Arc<Queue>,
    device: Arc<Device>,

    instance: Arc<Instance>,
}

static VULKAN_MINER: OnceLock<Result<Arc<Mutex<VulkanMiner>>, VulkanMinerError>> = OnceLock::new();

impl VulkanMiner {
    pub fn get() -> Result<Arc<Mutex<VulkanMiner>>, VulkanMinerError> {
        VULKAN_MINER
            .get_or_init(|| Self::new().map(|miner| Arc::new(Mutex::new(miner))))
            .clone()
        //Self::new().map(|miner| Arc::new(Mutex::new(miner)))
    }

    fn new() -> Result<Self, VulkanMinerError> {
        let library =
            VulkanLibrary::new().map_err(|err| VulkanMinerError::Load(err.to_string()))?;
        let instance = Instance::new(library, InstanceCreateInfo::default())
            .map_err(VulkanMinerError::Instance)?;

        let physical_device = instance
            .enumerate_physical_devices()
            .map_err(VulkanMinerError::EnumerateDevices)?
            .filter(|d| {
                let features = d.supported_features();
                features.shader_int8 && features.shader_int64
            })
            .next()
            .ok_or_else(|| VulkanMinerError::NoDevices)?;

        let queue_family_index = physical_device
            .queue_family_properties()
            .iter()
            .enumerate()
            .position(|(_queue_family_index, queue_family_properties)| {
                queue_family_properties
                    .queue_flags
                    .contains(QueueFlags::COMPUTE)
            })
            .ok_or_else(|| VulkanMinerError::NoComputeQueues)?
            as u32;

        let (device, mut queues) = Device::new(
            physical_device,
            DeviceCreateInfo {
                // here we pass the desired queue family to use by index
                queue_create_infos: vec![QueueCreateInfo {
                    queue_family_index,
                    ..Default::default()
                }],
                enabled_features: Features {
                    shader_int8: true,
                    shader_int64: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .map_err(VulkanMinerError::Device)?;
        let queue = queues.next().unwrap();

        // create buffers
        let memory_allocator = Arc::new(StandardMemoryAllocator::new_default(device.clone()));

        let uniform_buffer = Buffer::from_data(
            memory_allocator.clone(),
            BufferCreateInfo {
                usage: BufferUsage::UNIFORM_BUFFER,
                ..Default::default()
            },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_DEVICE
                    | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            MineUniforms::default().as_std140(),
        )
        .unwrap();

        let response_buffer = Buffer::from_data(
            memory_allocator.clone(),
            BufferCreateInfo {
                usage: BufferUsage::STORAGE_BUFFER,
                ..Default::default()
            },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_DEVICE
                    | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            MineResponse::default().as_std430(),
        )
        .unwrap();

        let shader = mine_shader::load(device.clone()).unwrap();

        let descriptor_set_allocator =
            StandardDescriptorSetAllocator::new(device.clone(), Default::default());
        let pipeline_layout = PipelineLayout::new(
            device.clone(),
            PipelineLayoutCreateInfo {
                set_layouts: vec![DescriptorSetLayout::new(
                    device.clone(),
                    DescriptorSetLayoutCreateInfo {
                        bindings: BTreeMap::from([
                            (
                                0,
                                DescriptorSetLayoutBinding {
                                    stages: ShaderStages::COMPUTE,
                                    ..DescriptorSetLayoutBinding::descriptor_type(
                                        DescriptorType::UniformBuffer,
                                    )
                                },
                            ),
                            (
                                1,
                                DescriptorSetLayoutBinding {
                                    stages: ShaderStages::COMPUTE,
                                    ..DescriptorSetLayoutBinding::descriptor_type(
                                        DescriptorType::StorageBuffer,
                                    )
                                },
                            ),
                        ]),
                        ..Default::default()
                    },
                )
                .unwrap()],
                push_constant_ranges: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let descriptor_set_layouts = pipeline_layout.set_layouts();

        let descriptor_set_layout_index = 0;
        let descriptor_set_layout = descriptor_set_layouts
            .get(descriptor_set_layout_index)
            .unwrap();
        let descriptor_set = PersistentDescriptorSet::new(
            &descriptor_set_allocator,
            descriptor_set_layout.clone(),
            [
                WriteDescriptorSet::buffer(0, uniform_buffer.clone()),
                WriteDescriptorSet::buffer(1, response_buffer.clone()),
            ],
            [],
        )
        .unwrap();

        Ok(Self {
            uniform_buffer,
            response_buffer,
            memory_allocator,

            base_compute_shader: shader,
            compute_pipelines: Default::default(),
            work_group_size: WORK_GROUP_SIZE,

            descriptor_set_layout_index,
            descriptor_set: descriptor_set.into(),

            queue,
            device,

            instance,
        })
    }

    fn get_compute_pipeline(&mut self, variant: VulkanMinerVariant) -> Arc<ComputePipeline> {
        if let Some(compute_pipeline) = self.compute_pipelines.get(&variant) {
            // This pipeline variant is already cached!
            return compute_pipeline.clone();
        }

        let specialized_shader = self
            .base_compute_shader
            .specialize(variant.specialization_constants().into_iter().collect())
            .unwrap();
        let cs = specialized_shader.entry_point("main").unwrap();
        let stage = PipelineShaderStageCreateInfo::new(cs);
        let layout = PipelineLayout::new(
            self.device.clone(),
            PipelineDescriptorSetLayoutCreateInfo::from_stages([&stage])
                .into_pipeline_layout_create_info(self.device.clone())
                .unwrap(),
        )
        .unwrap();
        let compute_pipeline = ComputePipeline::new(
            self.device.clone(),
            None,
            ComputePipelineCreateInfo::stage_layout(stage, layout),
        )
        .unwrap();

        self.compute_pipelines
            .insert(variant, compute_pipeline.clone());
        compute_pipeline
    }
}

impl Sha3_256PoWMiner for VulkanMiner {
    fn is_hw_accelerated(&self) -> bool {
        true
    }

    fn min_nonce_count(&self) -> u32 {
        self.work_group_size
    }

    fn nonce_peel_amount(&self) -> u32 {
        // TODO: do runtime benchmarks to find an optimal value for this
        1 << 20
    }

    fn generate_pow_internal(
        &mut self,
        leading_bytes: &[u8],
        trailing_bytes: &[u8],
        difficulty: &PoWDifficulty,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, MineError> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let work_group_counts = [nonce_count.div_ceil(self.work_group_size), 1, 1];

        let max_work_group_count = self
            .device
            .physical_device()
            .properties()
            .max_compute_work_group_count[0];
        if work_group_counts[0] > max_work_group_count {
            warn!(
                "Vulkan miner dispatched with too high nonce_count {} (work group size={}, max \
                   work group count={})! Splitting into multiple dispatches.",
                nonce_count, self.work_group_size, max_work_group_count
            );

            for (first, count) in split_range_into_blocks(
                first_nonce,
                nonce_count,
                max_work_group_count * self.work_group_size,
            ) {
                match self.generate_pow_internal(
                    leading_bytes,
                    trailing_bytes,
                    difficulty,
                    first,
                    count,
                    statistics,
                )? {
                    Some(nonce) => return Ok(Some(nonce)),
                    None => (),
                };
            }
            return Ok(None);
        }

        let mut stats_updater = statistics.update_safe();

        let (difficulty_function, leading_zeroes_mining_difficulty, compact_target_expanded) =
            match difficulty {
                // The target value is higher than the largest possible SHA3-256 hash. Therefore,
                // every hash will meet the required difficulty threshold, so we can just return
                // an arbitrary nonce.
                PoWDifficulty::TargetHashAlwaysPass => return Ok(Some(first_nonce)),

                PoWDifficulty::LeadingZeroBytes { leading_zeroes } => (
                    // DIFFICULTY_FUNCTION_LEADING_ZEROES
                    1,
                    (*leading_zeroes).try_into().unwrap(),
                    Default::default(),
                ),
                PoWDifficulty::TargetHash { target_hash } => (
                    // DIFFICULTY_FUNCTION_COMPACT_TARGET
                    2,
                    Default::default(),
                    target_hash.map(|b| b as u32).into(),
                ),
            };

        // compute the initial digest state
        let mut digest = sha3_256_shader::Sha3_256Context::default();
        for byte in leading_bytes {
            digest.update(*byte);
        }

        // extend the block header with trailing zeroes
        let mut block_header_bytes = [0u8; BLOCK_HEADER_MAX_BYTES];
        block_header_bytes[..trailing_bytes.len()].copy_from_slice(trailing_bytes);

        //update the uniforms
        *self.uniform_buffer.write().unwrap() = MineUniforms {
            u_firstNonce: first_nonce,
            u_compactTarget_expanded: compact_target_expanded,
            u_digest_initialState: digest.state.into(),
            u_blockHeader_bytes: block_header_bytes.map(|b| b as u32).into(),
        }
        .as_std140();

        //reset the response buffer
        *self.response_buffer.write().unwrap() = MineResponse::default().as_std430();

        let command_buffer_allocator = StandardCommandBufferAllocator::new(
            self.device.clone(),
            StandardCommandBufferAllocatorCreateInfo::default(),
        );

        let mut command_buffer_builder = AutoCommandBufferBuilder::primary(
            &command_buffer_allocator,
            self.queue.queue_family_index(),
            CommandBufferUsage::OneTimeSubmit,
        )
        .unwrap();

        let compute_pipeline = self.get_compute_pipeline(VulkanMinerVariant {
            trailing_bytes_count: trailing_bytes.len().try_into().unwrap(),
            digest_initial_byte_index: digest.byte_index,
            digest_initial_word_index: digest.word_index,
            difficulty_function,
            leading_zeroes_mining_difficulty,
        });

        command_buffer_builder
            .bind_pipeline_compute(compute_pipeline.clone())
            .unwrap()
            .bind_descriptor_sets(
                PipelineBindPoint::Compute,
                compute_pipeline.layout().clone(),
                self.descriptor_set_layout_index as u32,
                (*self.descriptor_set).clone(),
            )
            .unwrap()
            .dispatch(work_group_counts)
            .unwrap();

        let command_buffer = command_buffer_builder.build().unwrap();

        let future = sync::now(self.device.clone())
            .then_execute(self.queue.clone(), command_buffer)
            .unwrap()
            .then_signal_fence_and_flush()
            .unwrap();

        future.wait(None).unwrap();

        let response = MineResponse::from_std430(*self.response_buffer.read().unwrap());

        // Now that the hashes have been computed, update the statistics
        stats_updater
            .computed_hashes((work_group_counts[0] as u128) * (self.work_group_size as u128));

        match response.status {
            // RESPONSE_STATUS_NONE
            0 => Ok(None),
            // RESPONSE_STATUS_SUCCESS
            1 => Ok(Some(response.success_nonce)),
            // RESPONSE_STATUS_ERROR_CODE
            2 => panic!(
                "compute shader returned error code 0x{:04x}: {}",
                response.error_code,
                match response.error_code {
                    1 => "INVALID_DIFFICULTY_FUNCTION",
                    _ => "(unknown)",
                }
            ),
            _ => panic!(
                "compute shader returned unknown response status 0x{:04x}",
                response.status
            ),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Clone, Copy, Debug, Default, AsStd140)]
struct MineUniforms {
    u_firstNonce: u32,
    u_compactTarget_expanded: crevice_utils::FixedArray<u32, SHA3_256_BYTES>,
    u_digest_initialState: crevice_utils::FixedArray<u64, SHA3_KECCAK_SPONGE_WORDS>,
    u_blockHeader_bytes: crevice_utils::FixedArray<u32, BLOCK_HEADER_MAX_BYTES>,
}

#[allow(non_snake_case)]
#[derive(Clone, Copy, Debug, AsStd430)]
struct MineResponse {
    status: u32,
    success_nonce: u32,
    error_code: u32,
}

impl Default for MineResponse {
    fn default() -> Self {
        Self {
            status: 0,
            success_nonce: u32::MAX,
            error_code: 0,
        }
    }
}

mod mine_shader {
    vulkano_shaders::shader! {
        ty: "compute",
        path: "src/miner_pow/vulkan_miner.glsl",
    }
}

mod crevice_utils {
    use super::*;

    // what follows is a gross hack since crevice doesn't support fixed arrays
    #[derive(Copy, Clone, Debug)]
    pub struct FixedArray<E: AsPadded, const N: usize>([E; N]);

    impl<E: AsPadded + 'static, const N: usize> From<[E; N]> for FixedArray<E, N> {
        fn from(value: [E; N]) -> Self {
            Self(value)
        }
    }

    impl<E: AsPadded + 'static, const N: usize> Default for FixedArray<E, N> {
        fn default() -> Self {
            Self([<E as Default>::default(); N])
        }
    }

    impl<E: AsPadded + 'static, const N: usize> AsStd140 for FixedArray<E, N> {
        type Output = Std140FixedArray<E, N>;

        fn as_std140(&self) -> Self::Output {
            Std140FixedArray::<E, N>(
                self.0
                    .map(|val| <E as AsPadded>::to_padded(val).as_std140()),
            )
        }

        fn from_std140(val: Self::Output) -> Self {
            Self(
                val.0.map(|padded| {
                    <E as AsPadded>::from_padded(<_ as AsStd140>::from_std140(padded))
                }),
            )
        }
    }

    #[derive(Clone, Copy)]
    pub struct Std140FixedArray<E: AsPadded, const N: usize>(
        [<<E as AsPadded>::Output as AsStd140>::Output; N],
    );

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Zeroable
        for Std140FixedArray<E, N>
    {
    }

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Pod
        for Std140FixedArray<E, N>
    {
    }

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::std140::Std140
        for Std140FixedArray<E, N>
    {
        const ALIGNMENT: usize = crevice::internal::max_arr([
            16usize,
            <<<E as AsPadded>::Output as AsStd140>::Output as crevice::std140::Std140>::ALIGNMENT,
        ]);
    }

    impl<E: AsPadded + 'static, const N: usize> fmt::Debug for Std140FixedArray<E, N> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            fmt::Debug::fmt(
                &<FixedArray<E, N> as AsStd140>::from_std140(self.clone()),
                f,
            )
        }
    }

    impl<E: AsPadded + 'static, const N: usize> AsStd430 for FixedArray<E, N> {
        type Output = Std430FixedArray<E, N>;

        fn as_std430(&self) -> Self::Output {
            Std430FixedArray::<E, N>(
                self.0
                    .map(|val| <E as AsPadded>::to_padded(val).as_std430()),
            )
        }

        fn from_std430(val: Self::Output) -> Self {
            Self(
                val.0.map(|padded| {
                    <E as AsPadded>::from_padded(<_ as AsStd430>::from_std430(padded))
                }),
            )
        }
    }

    #[derive(Clone, Copy)]
    pub struct Std430FixedArray<E: AsPadded, const N: usize>(
        [<<E as AsPadded>::Output as AsStd430>::Output; N],
    );

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Zeroable
        for Std430FixedArray<E, N>
    {
    }

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Pod
        for Std430FixedArray<E, N>
    {
    }

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::std430::Std430
        for Std430FixedArray<E, N>
    {
        const ALIGNMENT: usize = crevice::internal::max_arr([
            0usize,
            <<<E as AsPadded>::Output as AsStd430>::Output as crevice::std430::Std430>::ALIGNMENT,
        ]);
    }

    impl<E: AsPadded + 'static, const N: usize> fmt::Debug for Std430FixedArray<E, N> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            fmt::Debug::fmt(
                &<FixedArray<E, N> as AsStd430>::from_std430(self.clone()),
                f,
            )
        }
    }

    pub trait AsPadded: Copy + Default + fmt::Debug {
        type Output: Copy + AsStd140 + AsStd430;

        fn to_padded(self) -> Self::Output;

        fn from_padded(value: Self::Output) -> Self;
    }

    #[derive(Clone, Copy, Debug, AsStd140, AsStd430)]
    pub struct U32Padded {
        value: u32,
    }

    impl AsPadded for u32 {
        type Output = U32Padded;

        fn to_padded(self) -> Self::Output {
            U32Padded { value: self }
        }

        fn from_padded(value: Self::Output) -> Self {
            value.value
        }
    }

    #[derive(Clone, Copy, Debug, AsStd140, AsStd430)]
    pub struct F64Padded {
        value: f64,
    }

    impl AsPadded for f64 {
        type Output = F64Padded;

        fn to_padded(self) -> Self::Output {
            F64Padded { value: self }
        }

        fn from_padded(value: Self::Output) -> Self {
            value.value
        }
    }

    impl AsPadded for u64 {
        type Output = F64Padded;

        fn to_padded(self) -> Self::Output {
            F64Padded {
                value: f64::from_ne_bytes(self.to_ne_bytes()),
            }
        }

        fn from_padded(value: Self::Output) -> Self {
            Self::from_ne_bytes(value.value.to_ne_bytes())
        }
    }
}

/// Rust port of the SHA3-256 shader.
///
/// This enables us to precompute common parts of the digest state on the CPU.
mod sha3_256_shader {
    pub(super) const SHA3_KECCAK_SPONGE_WORDS: usize = ((1600)/8/*bits to byte*/)/8/*sizeof(uint64_t)*/; // 25
    const SHA3_256_BYTES: usize = 256 / 8; // 32
    const SHA3_256_CAPACITY_WORDS: usize = 2 * 256 / (8 * 8); // 8
    const KECCAK_ROUNDS: usize = 24;

    const KECCAKF_ROTC: &'static [u32; 24] = &[
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44,
    ];

    const KECCAKF_PILN: &'static [usize; 24] = &[
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1,
    ];

    const KECCAKF_RNDC: &'static [u64; KECCAK_ROUNDS] = &[
        0x0000000000000001u64,
        0x0000000000008082u64,
        0x800000000000808Au64,
        0x8000000080008000u64,
        0x000000000000808Bu64,
        0x0000000080000001u64,
        0x8000000080008081u64,
        0x8000000000008009u64,
        0x000000000000008Au64,
        0x0000000000000088u64,
        0x0000000080008009u64,
        0x000000008000000Au64,
        0x000000008000808Bu64,
        0x800000000000008Bu64,
        0x8000000000008089u64,
        0x8000000000008003u64,
        0x8000000000008002u64,
        0x8000000000000080u64,
        0x000000000000800Au64,
        0x800000008000000Au64,
        0x8000000080008081u64,
        0x8000000000008080u64,
        0x0000000080000001u64,
        0x8000000080008008u64,
    ];

    fn keccakf(s: &mut [u64; SHA3_KECCAK_SPONGE_WORDS]) {
        let mut bc = [0u64; 5];

        for round in 0..KECCAK_ROUNDS {
            /* Theta */
            for i in 0..5 {
                bc[i] = s[i] ^ s[i + 5] ^ s[i + 10] ^ s[i + 15] ^ s[i + 20];
            }

            for i in 0..5 {
                let t = bc[(i + 4) % 5] ^ bc[(i + 1) % 5].rotate_left(1);
                for j in 0..5 {
                    s[j * 5 + i] ^= t;
                }
            }

            /* Rho Pi */
            let mut t = s[1];
            for i in 0..24 {
                let j = KECCAKF_PILN[i];
                bc[0] = s[j];
                s[j] = t.rotate_left(KECCAKF_ROTC[i]);
                t = bc[0];
            }

            /* Chi */
            for j in 0..25 {
                for i in 0..5 {
                    bc[i] = s[j + i];
                }
                for i in 0..5 {
                    s[j + i] ^= (!bc[(i + 1) % 5]) & bc[(i + 2) % 5];
                }
            }

            /* Iota */
            s[0] ^= KECCAKF_RNDC[round];
        }
    }

    #[derive(Default)]
    pub(super) struct Sha3_256Context {
        pub state: [u64; SHA3_KECCAK_SPONGE_WORDS],
        pub byte_index: u32,
        pub word_index: u32,
    }

    impl Sha3_256Context {
        pub fn update(&mut self, byte: u8) {
            self.state[self.word_index as usize] ^= (byte as u64) << (self.byte_index * 8);
            self.byte_index += 1;
            if self.byte_index == 8 {
                self.byte_index = 0;
                self.word_index += 1;
                if self.word_index == (SHA3_KECCAK_SPONGE_WORDS - SHA3_256_CAPACITY_WORDS) as u32 {
                    self.word_index = 0;
                    keccakf(&mut self.state);
                }
            }
        }
    }
}
