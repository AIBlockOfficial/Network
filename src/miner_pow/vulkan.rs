use std::convert::TryInto;
use std::fmt;
use std::sync::Arc;
use crevice::std140::AsStd140;
use crevice::std430::AsStd430;
use vulkano::*;
use vulkano::buffer::{Buffer, BufferCreateInfo, BufferUsage, Subbuffer};
use vulkano::command_buffer::allocator::{StandardCommandBufferAllocator, StandardCommandBufferAllocatorCreateInfo};
use vulkano::command_buffer::{AutoCommandBufferBuilder, CommandBufferUsage};
use vulkano::descriptor_set::{PersistentDescriptorSet, WriteDescriptorSet};
use vulkano::descriptor_set::allocator::StandardDescriptorSetAllocator;
use vulkano::device::{Device, DeviceCreateInfo, Features, Queue, QueueCreateInfo, QueueFlags};
use vulkano::instance::{Instance, InstanceCreateInfo};
use vulkano::memory::allocator::{AllocationCreateInfo, MemoryTypeFilter, StandardMemoryAllocator};
use vulkano::pipeline::compute::ComputePipelineCreateInfo;
use vulkano::pipeline::{ComputePipeline, Pipeline, PipelineBindPoint, PipelineLayout, PipelineShaderStageCreateInfo};
use vulkano::pipeline::layout::PipelineDescriptorSetLayoutCreateInfo;
use vulkano::sync::GpuFuture;
use crate::constants::MINING_DIFFICULTY;
use crate::miner_pow::{BLOCK_HEADER_MAX_BYTES, MinerStatistics, PoWBlockMiner, PreparedBlockDifficulty, PreparedBlockHeader, SHA3_256_BYTES};

// synced with vulkan_miner.glsl
const WORK_GROUP_SIZE: u32 = 256;

#[derive(Debug)]
pub enum MinerError {
    Load(LoadingError),
    Instance(Validated<VulkanError>),
    EnumerateDevices(VulkanError),
    NoDevices,
    NoComputeQueues,
    Device(Validated<VulkanError>),
}

impl std::error::Error for MinerError {}

impl fmt::Display for MinerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Load(cause) => write!(f, "Failed to load Vulkan library: {cause}"),
            Self::Instance(cause) => write!(f, "Failed to create Vulkan instance: {cause}"),
            Self::EnumerateDevices(cause) => write!(f, "Failed to enumerate Vulkan devices: {cause}"),
            Self::NoDevices => f.write_str("No Vulkan devices found!"),
            Self::NoComputeQueues => f.write_str("No compute queue family found!"),
            Self::Device(cause) => write!(f, "Failed to create Vulkan device: {cause}"),
        }
    }
}

pub struct VulkanMiner {
    uniform_buffer: Subbuffer<<MineUniforms as AsStd140>::Output>,
    response_buffer: Subbuffer<<MineResponse as AsStd430>::Output>,
    memory_allocator: Arc<StandardMemoryAllocator>,

    compute_pipeline: Arc<ComputePipeline>,
    work_group_size: u32,

    descriptor_set_layout_index: usize,
    descriptor_set: Arc<PersistentDescriptorSet>,

    queue: Arc<Queue>,
    device: Arc<Device>,

    instance: Arc<Instance>,
}

impl VulkanMiner {
    pub fn new() -> Result<Self, MinerError> {
        let library = VulkanLibrary::new().map_err(MinerError::Load)?;
        let instance = Instance::new(library, InstanceCreateInfo::default())
            .map_err(MinerError::Instance)?;

        let physical_device = instance
            .enumerate_physical_devices()
            .map_err(MinerError::EnumerateDevices)?
            //.filter(|d| d.supported_features().shader_int64)
            .next()
            .ok_or_else(|| MinerError::NoDevices)?;

        let queue_family_index = physical_device
            .queue_family_properties()
            .iter()
            .enumerate()
            .position(|(_queue_family_index, queue_family_properties)| {
                queue_family_properties.queue_flags.contains(QueueFlags::COMPUTE)
            })
            .ok_or_else(|| MinerError::NoComputeQueues)? as u32;

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
        ).map_err(MinerError::Device)?;
        let queue = queues.next().unwrap();

        // create buffers
        let memory_allocator =
            Arc::new(StandardMemoryAllocator::new_default(device.clone()));

        let uniform_buffer = Buffer::from_data(
            memory_allocator.clone(),
            BufferCreateInfo {
                usage: BufferUsage::UNIFORM_BUFFER,
                ..Default::default()
            },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_DEVICE | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            MineUniforms::default().as_std140()
        ).unwrap();

        let response_buffer = Buffer::from_data(
            memory_allocator.clone(),
            BufferCreateInfo {
                usage: BufferUsage::STORAGE_BUFFER,
                ..Default::default()
            },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_DEVICE | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            MineResponse::default().as_std430()
        ).unwrap();

        let shader = mine_shader::load(device.clone()).unwrap();

        let cs = shader.entry_point("main").unwrap();
        let stage = PipelineShaderStageCreateInfo::new(cs);
        let layout = PipelineLayout::new(
            device.clone(),
            PipelineDescriptorSetLayoutCreateInfo::from_stages([&stage])
                .into_pipeline_layout_create_info(device.clone())
                .unwrap(),
        ).unwrap();
        let compute_pipeline = ComputePipeline::new(
            device.clone(),
            None,
            ComputePipelineCreateInfo::stage_layout(stage, layout)
        ).unwrap();

        let descriptor_set_allocator =
            StandardDescriptorSetAllocator::new(device.clone(), Default::default());
        let pipeline_layout = compute_pipeline.layout();
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
        ).unwrap();

        Ok(Self {
            uniform_buffer,
            response_buffer,
            memory_allocator,

            compute_pipeline,
            work_group_size: WORK_GROUP_SIZE,

            descriptor_set_layout_index,
            descriptor_set,

            queue,
            device,

            instance,
        })
    }
}

impl PoWBlockMiner for VulkanMiner {
    type Error = MinerError;

    fn min_nonce_count(&self) -> u32 {
        self.work_group_size
    }

    fn nonce_peel_amount(&self) -> u32 {
        // TODO: do runtime benchmarks to find an optimal value for this
        1 << 20
    }

    fn generate_pow_block_internal(
        &mut self,
        prepared_block_header: &PreparedBlockHeader,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, Self::Error> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let mut stats_updater = statistics.update_safe();

        let work_group_counts = [nonce_count.div_ceil(self.work_group_size), 1, 1];

        let block_header_nonce_offset = prepared_block_header.header_nonce_offset.try_into().unwrap();
        let block_header_length = prepared_block_header.header_bytes.len().try_into().unwrap();

        let (
            difficulty_function,
            leading_zeroes_mining_difficulty,
            compact_target_expanded,
        ) = match &prepared_block_header.difficulty {
            // The target value is higher than the largest possible SHA3-256 hash. Therefore,
            // every hash will meet the required difficulty threshold, so we can just return
            // an arbitrary nonce.
            PreparedBlockDifficulty::TargetHashAlwaysPass => return Ok(Some(first_nonce)),

            PreparedBlockDifficulty::LeadingZeroBytes => (
                // DIFFICULTY_FUNCTION_LEADING_ZEROES
                1,
                MINING_DIFFICULTY.try_into().unwrap(),
                Default::default(),
            ),
            PreparedBlockDifficulty::TargetHash { target_hash } => (
                // DIFFICULTY_FUNCTION_COMPACT_TARGET
                2,
                Default::default(),
                target_hash.map(|b| b as u32).into(),
            ),
        };

        // extend the block header with trailing zeroes
        let mut block_header_bytes = [0u8; BLOCK_HEADER_MAX_BYTES];
        block_header_bytes[..prepared_block_header.header_bytes.len()]
            .copy_from_slice(&prepared_block_header.header_bytes);

        //update the uniforms
        *self.uniform_buffer.write().unwrap() = MineUniforms {
            u_firstNonce: first_nonce,
            u_blockHeader_length: block_header_length,
            u_blockHeader_nonceOffset: block_header_nonce_offset,
            u_difficultyFunction: difficulty_function,
            u_leadingZeroes_miningDifficulty: leading_zeroes_mining_difficulty,
            u_compactTarget_expanded: compact_target_expanded,
            u_blockHeader_bytes: block_header_bytes.map(|b| b as u32).into(),
        }.as_std140();

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
        ).unwrap();

        command_buffer_builder
            .bind_pipeline_compute(self.compute_pipeline.clone())
            .unwrap()
            .bind_descriptor_sets(
                PipelineBindPoint::Compute,
                self.compute_pipeline.layout().clone(),
                self.descriptor_set_layout_index as u32,
                self.descriptor_set.clone(),
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
        stats_updater.computed_hashes(
            (work_group_counts[0] as u128) * (self.work_group_size as u128));

        match response.status {
            // RESPONSE_STATUS_NONE
            0 => Ok(None),
            // RESPONSE_STATUS_SUCCESS
            1 => Ok(Some(response.success_nonce)),
            // RESPONSE_STATUS_ERROR_CODE
            2 => panic!("compute shader returned error code 0x{:04x}: {}",
                        response.error_code,
                        match response.error_code {
                            1 => "INVALID_DIFFICULTY_FUNCTION",
                            _ => "(unknown)",
                        }),
            _ => panic!("compute shader returned unknown response status 0x{:04x}", response.status),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Clone, Copy, Debug, Default, AsStd140)]
struct MineUniforms {
    u_firstNonce : u32,
    u_blockHeader_length : u32,
    u_blockHeader_nonceOffset : u32,

    u_difficultyFunction : u32,
    u_leadingZeroes_miningDifficulty : u32,

    u_compactTarget_expanded : crevice_utils::FixedArray<u32, SHA3_256_BYTES>,

    u_blockHeader_bytes : crevice_utils::FixedArray<u32, BLOCK_HEADER_MAX_BYTES>,
}

#[allow(non_snake_case)]
#[derive(Clone, Copy, Debug, AsStd430)]
struct MineResponse {
    status : u32,
    success_nonce : u32,
    error_code : u32,
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
    vulkano_shaders::shader!{
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
                self.0.map(|val| <E as AsPadded>::to_padded(val).as_std140()))
        }

        fn from_std140(val: Self::Output) -> Self {
            Self(val.0.map(|padded| <E as AsPadded>::from_padded(<_ as AsStd140>::from_std140(padded))))
        }
    }

    #[derive(Clone, Copy)]
    pub struct Std140FixedArray<E: AsPadded, const N: usize>([<<E as AsPadded>::Output as AsStd140>::Output; N]);

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Zeroable for Std140FixedArray<E, N> {}

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Pod for Std140FixedArray<E, N> {}

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::std140::Std140 for Std140FixedArray<E, N> {
        const ALIGNMENT: usize = crevice::internal::max_arr([
            16usize,
            <<<E as AsPadded>::Output as AsStd140>::Output as crevice::std140::Std140>::ALIGNMENT,
        ]);
    }

    impl<E: AsPadded + 'static, const N: usize> fmt::Debug for Std140FixedArray<E, N> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            fmt::Debug::fmt(&<FixedArray<E, N> as AsStd140>::from_std140(self.clone()), f)
        }
    }

    impl<E: AsPadded + 'static, const N: usize> AsStd430 for FixedArray<E, N> {
        type Output = Std430FixedArray<E, N>;

        fn as_std430(&self) -> Self::Output {
            Std430FixedArray::<E, N>(
                self.0.map(|val| <E as AsPadded>::to_padded(val).as_std430()))
        }

        fn from_std430(val: Self::Output) -> Self {
            Self(val.0.map(|padded| <E as AsPadded>::from_padded(<_ as AsStd430>::from_std430(padded))))
        }
    }

    #[derive(Clone, Copy)]
    pub struct Std430FixedArray<E: AsPadded, const N: usize>([<<E as AsPadded>::Output as AsStd430>::Output; N]);

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Zeroable for Std430FixedArray<E, N> {}

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::internal::bytemuck::Pod for Std430FixedArray<E, N> {}

    unsafe impl<E: AsPadded + 'static, const N: usize> crevice::std430::Std430 for Std430FixedArray<E, N> {
        const ALIGNMENT: usize = crevice::internal::max_arr([
            0usize,
            <<<E as AsPadded>::Output as AsStd430>::Output as crevice::std430::Std430>::ALIGNMENT,
        ]);
    }

    impl<E: AsPadded + 'static, const N: usize> fmt::Debug for Std430FixedArray<E, N> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            fmt::Debug::fmt(&<FixedArray<E, N> as AsStd430>::from_std430(self.clone()), f)
        }
    }

    pub trait AsPadded : Copy + Default + fmt::Debug {
        type Output : Copy + AsStd140 + AsStd430;

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
}
