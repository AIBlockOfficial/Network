use std::convert::TryInto;
use std::ffi::CStr;
use std::{error, fmt};
use gl::types::*;
use glfw::{Glfw, GlfwReceiver, OpenGlProfileHint, PWindow, WindowEvent, WindowHint, WindowMode};
use tw_chain::crypto::sha3_256;
use tw_chain::primitives::block::BlockHeader;
use crate::asert::CompactTarget;
use crate::constants::{MINING_DIFFICULTY, POW_NONCE_LEN};
use crate::opengl_miner::gl_error::{AddContext, CompileShaderError, GlError, LinkProgramError};
use crate::opengl_miner::gl_wrapper::{Buffer, GetIntIndexedType, GetProgramIntType, GetStringType, ImmutableBuffer, IndexedBufferTarget, MemoryBarrierBit, Program, Shader, ShaderType, UniformLocation};

// libglfw3-dev libxrandr-dev libxinerama-dev libxcursor-dev libxi-dev

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum CreateMinerError {
    InitializeGlfw(glfw::InitError),
    CreateGlfwWindow,
    CompileShader(CompileShaderError),
    LinkProgram(LinkProgramError),
    GlError(GlError),
}

impl fmt::Display for CreateMinerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InitializeGlfw(cause) =>
                write!(f, "Failed to initialize GLFW: {cause}"),
            Self::CreateGlfwWindow => f.write_str("Failed to create GLFW window"),
            Self::CompileShader(cause) =>
                write!(f, "Failed to compile shader: {cause}"),
            Self::LinkProgram(cause) =>
                write!(f, "Failed to link shader program: {cause}"),
            Self::GlError(cause) =>
                write!(f, "An OpenGL error occurred while initializing the GPU miner: {cause}"),
        }
    }
}

impl error::Error for CreateMinerError {}

pub struct Miner {
    program: Program,
    program_first_nonce_uniform: UniformLocation,
    program_header_length_uniform: UniformLocation,
    program_header_nonce_offset_uniform: UniformLocation,
    program_difficulty_function_uniform: UniformLocation,
    program_leading_zeroes_mining_difficulty_uniform: UniformLocation,
    program_compact_target_expanded_uniform: UniformLocation,
    program_work_group_size: u32,
    max_work_group_count: u32,

    glfw_window: PWindow,
    glfw_events: GlfwReceiver<(f64, WindowEvent)>,
    glfw: Glfw,
}

impl Miner {
    pub fn new() -> Result<Self, CreateMinerError> {
        let mut glfw = glfw::init(glfw::fail_on_errors)
            .map_err(CreateMinerError::InitializeGlfw)?;

        glfw.window_hint(WindowHint::ContextVersion(4, 5));
        glfw.window_hint(WindowHint::OpenGlProfile(OpenGlProfileHint::Core));
        glfw.window_hint(WindowHint::Visible(false));
        let (mut window, events) = glfw.create_window(256, 256, "AIBlock Miner", WindowMode::Windowed)
            .ok_or(CreateMinerError::CreateGlfwWindow)?;

        gl::load_with(|name| window.get_proc_address(name) as *const _);

        let vendor = gl_wrapper::get_string(GetStringType::VENDOR).unwrap();
        let renderer = gl_wrapper::get_string(GetStringType::RENDERER).unwrap();
        let version = gl_wrapper::get_string(GetStringType::VERSION).unwrap();
        let glsl_version = gl_wrapper::get_string(GetStringType::SHADING_LANGUAGE_VERSION).unwrap();
        println!("Created OpenGL context:\n  Vendor: {vendor}\n  Renderer: {renderer}\n  Version: {version}\n  GLSL version: {glsl_version}");

        // compile and link the compute shader
        let shader = Shader::compile(
            ShaderType::Compute,
            //&[include_str!("opengl_miner.glsl")],
            &[ std::fs::read_to_string("src/opengl_miner.glsl").expect("failed to read source").as_str() ]
        ).map_err(CreateMinerError::CompileShader)?;

        let program = Program::link(&[&shader])
            .map_err(CreateMinerError::LinkProgram)?;

        // figure out the compute shader's work group size
        let work_group_size_arr = program.get_program_int::<3>(GetProgramIntType::ComputeWorkGroupSize)
            .map_err(CreateMinerError::GlError)?;
        assert_eq!(&work_group_size_arr[1..], &[1; 2],
                   "work group size should be 1 on y and z axes...");

        let max_work_group_count = gl_wrapper::get_int(GetIntIndexedType::MAX_COMPUTE_WORK_GROUP_COUNT, 0)
            .map_err(CreateMinerError::GlError)? as u32;

        Ok(Self {
            program_first_nonce_uniform: program.uniform_location("u_firstNonce").unwrap(),
            program_header_length_uniform: program.uniform_location("u_blockHeader_length").unwrap(),
            program_header_nonce_offset_uniform: program.uniform_location("u_blockHeader_nonceOffset").unwrap(),
            program_difficulty_function_uniform: program.uniform_location("u_difficultyFunction").unwrap(),
            program_leading_zeroes_mining_difficulty_uniform: program.uniform_location("u_leadingZeroes_miningDifficulty").unwrap(),
            program_compact_target_expanded_uniform: program.uniform_location("u_compactTarget_expanded").unwrap(),
            program_work_group_size: work_group_size_arr[0].try_into().unwrap(),
            max_work_group_count,
            program,

            glfw_window: window,
            glfw_events: events,
            glfw,
        })
    }

    fn find_block_header_nonce_location(block_header: &BlockHeader) -> (Vec<u8>, u32) {
        assert_eq!(POW_NONCE_LEN, 4, "POW_NONCE_LEN has changed?!?");

        let mut block_header = block_header.clone();

        block_header.nonce_and_mining_tx_hash.0 = vec![0x00u8; POW_NONCE_LEN];
        let serialized_00 = bincode::serialize(&block_header).unwrap();
        block_header.nonce_and_mining_tx_hash.0 = vec![0xFFu8; POW_NONCE_LEN];
        let serialized_ff = bincode::serialize(&block_header).unwrap();

        assert_ne!(serialized_00, serialized_ff,
                   "changing the nonce didn't affect the serialized block header?!?");
        assert_eq!(serialized_00.len(), serialized_ff.len(),
                   "changing the nonce affected the block header's serialized length?!?");

        // find the index at which the two headers differ
        let nonce_offset = (0..serialized_00.len())
            .find(|offset| serialized_00[*offset] != serialized_ff[*offset])
            .expect("the serialized block headers are not equal, but are equal at every index?!?");

        assert_eq!(serialized_00.as_slice()[nonce_offset..nonce_offset + 4], [0x00u8; 4],
                   "serialized block header with nonce 0x00000000 has different bytes at presumed nonce offset!");
        assert_eq!(serialized_ff.as_slice()[nonce_offset..nonce_offset + 4], [0xFFu8; 4],
                   "serialized block header with nonce 0xFFFFFFFF has different bytes at presumed nonce offset!");

        (serialized_00, nonce_offset.try_into().unwrap())
    }

    fn extract_difficulty_target(block_header: &BlockHeader) -> Result<Option<CompactTarget>, &'static str> {
        if block_header.difficulty.is_empty() {
            Ok(None)
        } else {
            match CompactTarget::try_from_slice(&block_header.difficulty) {
                Some(target) => Ok(Some(target)),
                None => Err("block header contains invalid difficulty"),
            }
        }
    }

    fn expand_target_to_bytes(compact_target: CompactTarget) -> Option<[u8; 32]> {
        use rug::Integer;

        let expanded_target = compact_target.expand_integer();

        let byte_digits: Vec<u8> = expanded_target.to_digits(rug::integer::Order::MsfBe);
        if byte_digits.len() > 32 {
            // The target value is higher than the largest possible SHA3-256 hash.
            None
        } else {
            let mut result = [0u8; 32];
            result[32 - byte_digits.len()..].copy_from_slice(&byte_digits);

            assert_eq!(expanded_target, Integer::from_digits(&result, rug::integer::Order::MsfBe));

            Some(result)
        }
    }

    /// Tries to generate a Proof-of-Work for a block.
    pub fn generate_pow_block_cpu(
        block_header: &BlockHeader,
        first_nonce: u32,
        nonce_count: u32,
    ) -> Result<Option<u32>, &'static str> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let (mut block_header_bytes, block_header_nonce_offset) =
            Self::find_block_header_nonce_location(block_header);
        let block_header_nonce_offset = block_header_nonce_offset as usize;

        let difficulty_target = match Self::extract_difficulty_target(block_header)? {
            None => None,
            Some(compact_target) => match Self::expand_target_to_bytes(compact_target) {
                // The target value is higher than the largest possible SHA3-256 hash. Therefore,
                // every hash will meet the required difficulty threshold, so we can just return
                // an arbitrary nonce.
                None => return Ok(Some(first_nonce)),
                Some(expanded_target) => Some(expanded_target),
            },
        };

        for nonce in first_nonce..=first_nonce.saturating_add(nonce_count - 1) {
            block_header_bytes.as_mut_slice()
                [block_header_nonce_offset..block_header_nonce_offset + 4]
                .copy_from_slice(&nonce.to_ne_bytes());

            let hash = sha3_256::digest(&block_header_bytes);

            match &difficulty_target {
                None => {
                    // There isn't a difficulty function, check if the first MINING_DIFFICULTY bytes
                    // of the hash are 0
                    let hash_prefix = hash.first_chunk::<MINING_DIFFICULTY>().unwrap();
                    if hash_prefix == &[0u8; MINING_DIFFICULTY] {
                        return Ok(Some(nonce));
                    }
                },
                Some(target) => {
                    if hash.as_slice() <= target.as_slice() {
                        return Ok(Some(nonce));
                    }
                },
            }
        }

        Ok(None)
    }

    /// Tries to generate a Proof-of-Work for a block.
    pub fn generate_pow_block_gpu(
        &mut self,
        block_header: &BlockHeader,
        first_nonce: u32,
        nonce_count: u32,
    ) -> Result<Option<u32>, GlError> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let (block_header_bytes, block_header_nonce_offset) =
            Self::find_block_header_nonce_location(block_header);
        let block_header_length: u32 = block_header_bytes.len().try_into().unwrap();

        let dispatch_count = nonce_count.div_ceil(self.program_work_group_size);
        //let hash_count = work_group_size * dispatch_count;
        //let hash_buffer_capacity_int32s = hash_count * 32;
        //let hash_buffer_capacity_bytes = hash_buffer_capacity_int32s as usize * 4;

        self.program.set_uniform_1ui(self.program_first_nonce_uniform, first_nonce)?;
        self.program.set_uniform_1ui(self.program_header_length_uniform, block_header_length)?;
        self.program.set_uniform_1ui(self.program_header_nonce_offset_uniform, block_header_nonce_offset)?;

        // TODO: don't unwrap here
        if let Some(compact_target) = Self::extract_difficulty_target(block_header).unwrap() {
            if let Some(expanded_target_hash) = Self::expand_target_to_bytes(compact_target) {
                // DIFFICULTY_FUNCTION_COMPACT_TARGET
                self.program.set_uniform_1ui(self.program_difficulty_function_uniform, 2)?;
                self.program.set_uniform_1uiv(self.program_compact_target_expanded_uniform,
                                              &expanded_target_hash.map(|b| b as u32))?;
            } else {
                // The target value is higher than the largest possible SHA3-256 hash. Therefore,
                // every hash will meet the required difficulty threshold, so we can just return
                // an arbitrary nonce.
                return Ok(Some(first_nonce))
            }
        } else {
            // DIFFICULTY_FUNCTION_LEADING_ZEROES
            self.program.set_uniform_1ui(self.program_difficulty_function_uniform, 1)?;
            self.program.set_uniform_1ui(self.program_leading_zeroes_mining_difficulty_uniform, MINING_DIFFICULTY.try_into().unwrap())?;
        }

        let block_header_buffer_data = block_header_bytes.iter()
            .map(|b| (*b as u32).to_ne_bytes())
            .collect::<Vec<_>>()
            .concat();
        let block_header_buffer = ImmutableBuffer::new_initialized(&block_header_buffer_data, 0)
            .add_context("BlockHeader buffer")?;

        //let mut hash_buffer = ImmutableBuffer::new_uninitialized(hash_buffer_capacity_bytes, 0)
        //    .add_msg("HashOutput buffer")?;

        const RESPONSE_BUFFER_CAPACITY: usize = 12;
        let response_buffer_data = [ 0u32, u32::MAX, 0u32 ];
        let mut response_buffer_data : [u8; RESPONSE_BUFFER_CAPACITY] = unsafe { std::mem::transmute(response_buffer_data.map(u32::to_ne_bytes)) };
        let response_buffer = ImmutableBuffer::new_initialized(&response_buffer_data, 0)
            .add_context("Response buffer")?;

        block_header_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 0).unwrap();
        //hash_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 1).unwrap();
        response_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 2).unwrap();

        //hash_buffer.invalidate().unwrap();
        self.program.bind().unwrap();

        gl_wrapper::dispatch_compute(dispatch_count, 1, 1)?;

        gl_wrapper::memory_barrier(&[ MemoryBarrierBit::BufferUpdate ])?;

        //let mut hash_buffer_data = vec![0u8; hash_buffer_capacity_bytes];
        //hash_buffer.download(0, &mut hash_buffer_data).unwrap();

        response_buffer.download_sub_data(0, &mut response_buffer_data).unwrap();

        let response_status = u32::from_ne_bytes(*response_buffer_data[0..].first_chunk().unwrap());
        let response_success_nonce = u32::from_ne_bytes(*response_buffer_data[4..].first_chunk().unwrap());
        let response_error_code = u32::from_ne_bytes(*response_buffer_data[8..].first_chunk().unwrap());

        match response_status {
            // RESPONSE_STATUS_NONE
            0 => Ok(None),
            // RESPONSE_STATUS_SUCCESS
            1 => Ok(Some(response_success_nonce)),
            // RESPONSE_STATUS_ERROR_CODE
            2 => panic!("compute shader returned error code 0x{:04x}: {}",
                        response_error_code,
                        match response_error_code {
                            1 => "INVALID_DIFFICULTY_FUNCTION",
                            _ => "(unknown)",
                        }),
            _ => panic!("compute shader returned unknown response status 0x{:04x}", response_status),
        }
    }
}

pub mod gl_error {
    use std::{error, fmt};
    use super::*;

    pub(super) trait ErrorContext: Sized {
        fn into_string(self) -> String;

        fn into_boxed_str(self) -> Box<str> {
            self.into_string().into_boxed_str()
        }
    }

    impl<'a> ErrorContext for &'a str {
        fn into_string(self) -> String {
            self.into()
        }
    }

    impl<'a> ErrorContext for fmt::Arguments<'a> {
        fn into_string(self) -> String {
            fmt::format(self)
        }
    }

    impl<F: FnOnce() -> String> ErrorContext for F {
        fn into_string(self) -> String {
            self()
        }
    }

    pub(super) trait AddContext {
        fn add_context(self, message: impl ErrorContext) -> Self;
    }

    impl<OK, ERR: AddContext> AddContext for Result<OK, ERR> {
        fn add_context(self, message: impl ErrorContext) -> Self {
            self.map_err(|err| err.add_context(message))
        }
    }

    /// An OpenGL error code
    #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub enum GlError {
        InvalidEnum,
        InvalidValue,
        InvalidOperation,
        InvalidFramebufferOperation,
        OutOfMemory,
        StackUnderflow,
        StackOverflow,
        Unknown(GLenum),
        WrappedWithMessage {
            cause: Box<GlError>,
            message: String,
        },
    }

    impl fmt::Display for GlError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Self::InvalidEnum => f.write_str("GL_INVALID_ENUM: An unacceptable value is specified for an enumerated argument. The offending command is ignored and has no other side effect than to set the error flag."),
                Self::InvalidValue => f.write_str("GL_INVALID_VALUE: A numeric argument is out of range. The offending command is ignored and has no other side effect than to set the error flag."),
                Self::InvalidOperation => f.write_str("GL_INVALID_OPERATION: The specified operation is not allowed in the current state. The offending command is ignored and has no other side effect than to set the error flag."),
                Self::InvalidFramebufferOperation => f.write_str("GL_INVALID_FRAMEBUFFER_OPERATION: The framebuffer object is not complete. The offending command is ignored and has no other side effect than to set the error flag."),
                Self::OutOfMemory => f.write_str("GL_OUT_OF_MEMORY: There is not enough memory left to execute the command. The state of the GL is undefined, except for the state of the error flags, after this error is recorded."),
                Self::StackUnderflow => f.write_str("GL_STACK_UNDERFLOW: An attempt has been made to perform an operation that would cause an internal stack to underflow."),
                Self::StackOverflow => f.write_str("GL_STACK_OVERFLOW: An attempt has been made to perform an operation that would cause an internal stack to overflow."),
                Self::Unknown(id) => write!(f, "Unknown OpenGL error: {:#04x}", id),
                Self::WrappedWithMessage { cause, message } => write!(f, "{}: {}", message, cause),
            }
        }
    }

    impl error::Error for GlError {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            match self {
                Self::WrappedWithMessage { cause, .. } => Some(&**cause),
                _ => None,
            }
        }
    }

    impl AddContext for GlError {
        fn add_context(self, message: impl ErrorContext) -> Self {
            Self::WrappedWithMessage { cause: Box::new(self), message: message.into_string() }
        }
    }

    impl GlError {
        /// Checks the OpenGL context for errors
        pub(super) fn check() -> Result<(), Self> {
            match unsafe { gl::GetError() } {
                gl::NO_ERROR => Ok(()),
                gl::INVALID_ENUM => Err(Self::InvalidEnum),
                gl::INVALID_VALUE => Err(Self::InvalidValue),
                gl::INVALID_OPERATION => Err(Self::InvalidOperation),
                gl::INVALID_FRAMEBUFFER_OPERATION => Err(Self::InvalidFramebufferOperation),
                gl::OUT_OF_MEMORY => Err(Self::OutOfMemory),
                gl::STACK_UNDERFLOW => Err(Self::StackUnderflow),
                gl::STACK_OVERFLOW => Err(Self::StackOverflow),
                id => Err(Self::Unknown(id)),
            }
        }

        /// Checks the OpenGL context for errors
        pub(super) fn check_msg(message: impl ErrorContext) -> Result<(), Self> {
            Self::check().add_context(message)
        }
    }

    /// An error which can occur while compiling an OpenGL shader
    #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub enum CompileShaderError {
        OpenGlError {
            shader_type: ShaderType,
            cause: GlError,
        },
        CompilationFailed {
            shader_type: ShaderType,
            info_log: Box<str>,
        },
    }

    impl fmt::Display for CompileShaderError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Self::OpenGlError { shader_type, cause } =>
                    write!(f, "OpenGL error occurred while compiling {shader_type:?}: {cause}"),
                Self::CompilationFailed { shader_type, info_log: message } =>
                    write!(f, "Failed to compile {shader_type:?}: {message}"),
            }
        }
    }

    impl error::Error for CompileShaderError {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            match self {
                Self::OpenGlError { cause, .. } => Some(&*cause),
                _ => None,
            }
        }
    }

    /// An error which can occur while linking an OpenGL program
    #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub enum LinkProgramError {
        OpenGlError(GlError),
        LinkFailed {
            info_log: Box<str>,
        },
    }

    impl fmt::Display for LinkProgramError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Self::OpenGlError(cause) =>
                    write!(f, "OpenGL error occurred while linking an OpenGL shader: {cause}"),
                Self::LinkFailed { info_log } =>
                    write!(f, "Failed to link an OpenGL shader program: {info_log}"),
            }
        }
    }

    impl error::Error for LinkProgramError {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            match self {
                Self::OpenGlError(cause) => Some(&*cause),
                _ => None,
            }
        }
    }
}

#[allow(non_camel_case_types)]
mod gl_wrapper {
    use std::convert::TryInto;
    use std::ffi::CString;
    use std::ptr::null;
    use gl_error::*;
    use super::*;

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum GetStringType {
        VENDOR = gl::VENDOR,
        RENDERER = gl::RENDERER,
        VERSION = gl::VERSION,
        SHADING_LANGUAGE_VERSION = gl::SHADING_LANGUAGE_VERSION,
    }

    /// Wrapper around `glGetString`
    pub fn get_string(t: GetStringType) -> Result<&'static str, GlError> {
        let cptr = unsafe { gl::GetString(t as GLenum) };
        GlError::check_msg(format_args!("glGetString(GL_{t:?})"))?;
        let cstr = unsafe { CStr::from_ptr(cptr as *const std::ffi::c_char) };
        Ok(cstr.to_str().unwrap())
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum GetIntIndexedType {
        MAX_COMPUTE_WORK_GROUP_COUNT = gl::MAX_COMPUTE_WORK_GROUP_COUNT,
    }

    /// Wrapper around `glGetIntegeri_v`
    pub fn get_int(t: GetIntIndexedType, index: GLuint) -> Result<i32, GlError> {
        let mut value = 0;
        unsafe { gl::GetIntegeri_v(t as GLenum, index, &mut value) };
        GlError::check_msg(format_args!("glGetIntegeri_v(GL_{t:?}, {index})"))?;
        Ok(value)
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum MemoryBarrierBit {
        BufferUpdate = gl::BUFFER_UPDATE_BARRIER_BIT,
    }

    /// Wrapper around `glMemoryBarrier`
    pub fn memory_barrier(bits: &[MemoryBarrierBit]) -> Result<(), GlError> {
        let mask = bits.iter()
            .map(|bit| *bit as GLenum)
            .reduce(<GLenum as std::ops::BitOr>::bitor)
            .unwrap_or(0);
        unsafe { gl::MemoryBarrier(mask) };
        GlError::check_msg(format_args!("glMemoryBarrier({bits:?}) -> glMemoryBarrier({mask:#08x})"))
    }

    /// Wrapper around `glDispatchCompute`
    pub fn dispatch_compute(
        num_groups_x: u32,
        num_groups_y: u32,
        num_groups_z: u32,
    ) -> Result<(), GlError> {
        unsafe { gl::DispatchCompute(num_groups_x, num_groups_y, num_groups_z) };
        GlError::check_msg(format_args!("glDispatchCompute({num_groups_x}, {num_groups_y}, {num_groups_z})"))
    }

    /// An OpenGL shader object
    pub struct Shader {
        pub id: GLuint,
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum ShaderType {
        Compute = gl::COMPUTE_SHADER,
    }

    impl Shader {
        /// Compiles an OpenGL shader.
        ///
        /// ### Arguments
        ///
        /// * `shader_type`  - the type of shader
        /// * `source`       - the GLSL source code to compile
        pub fn compile(shader_type: ShaderType, source: &[&str]) -> Result<Self, CompileShaderError> {
            let check_error = |message: &'static str| -> Result<(), CompileShaderError> {
                GlError::check_msg(message).map_err(|err| CompileShaderError::OpenGlError {
                    shader_type,
                    cause: err,
                })
            };

            let shader = Self { id: unsafe { gl::CreateShader(shader_type as GLenum) } };
            check_error("glCreateShader")?;

            let count = source.len().try_into().unwrap();
            let (pointers, lengths): (Vec<_>, Vec<_>) = source.iter()
                .map(|str| (str.as_ptr() as *const GLchar, <usize as TryInto<GLint>>::try_into(str.len()).unwrap()))
                .unzip();
            unsafe { gl::ShaderSource(shader.id, count, pointers.as_ptr(), lengths.as_ptr()) };
            check_error("glShaderSource")?;

            unsafe { gl::CompileShader(shader.id) };
            check_error("glCompileShader")?;

            let mut compile_status = 0;
            unsafe { gl::GetShaderiv(shader.id, gl::COMPILE_STATUS, &mut compile_status) };
            check_error("glGetShaderiv(GL_COMPILE_STATUS)")?;

            let info_log : Box<str> = {
                let mut info_log_length = 0;
                unsafe { gl::GetShaderiv(shader.id, gl::INFO_LOG_LENGTH, &mut info_log_length) };
                check_error("glGetShaderiv(GL_INFO_LOG_LENGTH)")?;

                if info_log_length != 0 {
                    let mut info_log = vec![0i8; info_log_length as usize];
                    let mut actual_info_log_length: GLsizei = 0;
                    unsafe { gl::GetShaderInfoLog(shader.id, info_log_length.into(), &mut actual_info_log_length, info_log.as_mut_ptr()) };
                    check_error("glGetShaderInfoLog")?;

                    let info_log_u8s = info_log.into_iter().map(|c| c as u8).collect::<Vec<_>>();
                    String::from_utf8_lossy(&info_log_u8s).as_ref().into()
                } else {
                    Default::default()
                }
            };

            if compile_status as GLboolean == gl::FALSE {
                return Err(CompileShaderError::CompilationFailed { shader_type, info_log });
            } else if !info_log.is_empty() {
                println!("Shader compile warnings: {info_log}");
            }

            Ok(shader)
        }
    }

    impl Drop for Shader {
        fn drop(&mut self) {
            unsafe { gl::DeleteShader(self.id) }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum GetProgramIntType {
        ComputeWorkGroupSize = gl::COMPUTE_WORK_GROUP_SIZE,
    }

    impl GetProgramIntType {
        const fn element_count(&self) -> usize {
            match self {
                Self::ComputeWorkGroupSize => 3,
            }
        }
    }

    /// The location of an OpenGL program uniform
    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub struct UniformLocation {
        location: GLint,
        name: &'static str,
    }

    /// An OpenGL program object
    pub struct Program {
        pub id: GLuint,
    }

    impl Program {
        /// Links an OpenGL program.
        ///
        /// ### Arguments
        ///
        /// * `shaders`  - the shaders to be linked
        /// * `source`       - the GLSL source code to compile
        pub fn link(shaders: &[&Shader]) -> Result<Self, LinkProgramError> {
            fn check_error(message: &'static str) -> Result<(), LinkProgramError> {
                GlError::check_msg(message).map_err(LinkProgramError::OpenGlError)
            }

            let program = Self { id: unsafe { gl::CreateProgram() } };
            check_error("glCreateProgram")?;

            // Attach the shaders to the program
            for shader in shaders {
                unsafe { gl::AttachShader(program.id, shader.id) };
                check_error("glAttachShader")?;
            }

            unsafe { gl::LinkProgram(program.id) };
            check_error("glLinkProgram")?;

            // Detach the shaders from the program again
            for shader in shaders {
                unsafe { gl::DetachShader(program.id, shader.id) };
                check_error("glDetachShader")?;
            }

            let mut link_status = 0;
            unsafe { gl::GetProgramiv(program.id, gl::LINK_STATUS, &mut link_status) };
            check_error("glGetProgramiv(GL_LINK_STATUS)")?;

            let info_log : Box<str> = {
                let mut info_log_length = 0;
                unsafe { gl::GetProgramiv(program.id, gl::INFO_LOG_LENGTH, &mut info_log_length) };
                check_error("glGetProgramiv(GL_INFO_LOG_LENGTH)")?;

                if info_log_length != 0 {
                    let mut info_log = vec![0i8; info_log_length as usize];
                    let mut actual_info_log_length: GLsizei = 0;
                    unsafe { gl::GetProgramInfoLog(program.id, info_log_length.into(), &mut actual_info_log_length, info_log.as_mut_ptr()) };
                    check_error("glGetProgramInfoLog")?;

                    let info_log_u8s = info_log.into_iter().map(|c| c as u8).collect::<Vec<_>>();
                    String::from_utf8_lossy(&info_log_u8s).as_ref().into()
                } else {
                    Default::default()
                }
            };

            if link_status as GLboolean == gl::FALSE {
                return Err(LinkProgramError::LinkFailed { info_log });
            } else if !info_log.is_empty() {
                println!("Program link warnings: {info_log}");
            }

            Ok(program)
        }

        pub fn bind(&self) -> Result<(), GlError> {
            unsafe { gl::UseProgram(self.id) };
            GlError::check_msg("glUseProgram")
        }

        pub fn get_program_int<const N: usize>(&self, t: GetProgramIntType) -> Result<[i32; N], GlError> {
            assert_eq!(N, t.element_count(), "{:?} returns {} elements, not {}", t, t.element_count(), N);
            let mut res = [0 as GLint; N];
            unsafe { gl::GetProgramiv(self.id, t as GLenum, res.as_mut_ptr()) };
            GlError::check_msg(format_args!("glGetProgramiv({}, {t:?})", self.id))?;
            Ok(res)
        }

        pub fn uniform_location(&self, uniform_name: &'static str) -> Result<UniformLocation, GlError> {
            let uniform_name_cstr = CString::new(uniform_name).expect(uniform_name);
            let location = unsafe { gl::GetUniformLocation(self.id, uniform_name_cstr.as_ptr()) };
            GlError::check_msg(format_args!("glGetUniformLocation({}, \"{uniform_name}\")", self.id))?;
            Ok(UniformLocation { location, name: uniform_name })
        }

        pub fn set_uniform_1i(
            &mut self, location: UniformLocation,
            value: i32,
        ) -> Result<(), GlError> {
            unsafe { gl::ProgramUniform1i(self.id, location.location, value) };
            GlError::check_msg(format_args!("glProgramUniform1i({}, {location:?})", self.id))
        }

        pub fn set_uniform_1ui(
            &mut self, location: UniformLocation,
            value: u32,
        ) -> Result<(), GlError> {
            unsafe { gl::ProgramUniform1ui(self.id, location.location, value) };
            GlError::check_msg(format_args!("glProgramUniform1ui({}, {location:?})", self.id))
        }

        pub fn set_uniform_1uiv(
            &mut self, location: UniformLocation,
            value: &[u32],
        ) -> Result<(), GlError> {
            unsafe { gl::ProgramUniform1uiv(self.id, location.location, value.len().try_into().unwrap(), value.as_ptr()) };
            GlError::check_msg(format_args!("glProgramUniform1uiv({}, {location:?})", self.id))
        }
    }

    impl Drop for Program {
        fn drop(&mut self) {
            unsafe { gl::DeleteProgram(self.id) }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum BufferTarget {
        ArrayBuffer = gl::ARRAY_BUFFER,
        ElementArrayBuffer = gl::ELEMENT_ARRAY_BUFFER,
        CopyReadBuffer = gl::COPY_READ_BUFFER,
        CopyWriteBuffer = gl::COPY_WRITE_BUFFER,
        UniformBuffer = gl::UNIFORM_BUFFER,
        ShaderStorageBuffer = gl::SHADER_STORAGE_BUFFER,
        AtomicCounterBuffer = gl::ATOMIC_COUNTER_BUFFER,
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum IndexedBufferTarget {
        UniformBuffer = gl::UNIFORM_BUFFER,
        ShaderStorageBuffer = gl::SHADER_STORAGE_BUFFER,
        AtomicCounterBuffer = gl::ATOMIC_COUNTER_BUFFER,
    }

    /// Shared trait providing functions accessible to both kinds of OpenGL buffers.
    pub trait Buffer {
        fn invalidate(&mut self) -> Result<(), GlError>;

        fn upload_sub_data(&mut self, offset: usize, data: &[u8]) -> Result<(), GlError>;

        fn download_sub_data(&self, offset: usize, data: &mut [u8]) -> Result<(), GlError>;

        fn bind_base(&self, target: IndexedBufferTarget, index: GLuint) -> Result<(), GlError>;
    }

    impl<T: AsRef<BaseBuffer> + AsMut<BaseBuffer>> Buffer for T {
        fn invalidate(&mut self) -> Result<(), GlError> {
            self.as_mut().invalidate()
        }

        fn upload_sub_data(&mut self, offset: usize, data: &[u8]) -> Result<(), GlError> {
            self.as_mut().upload_sub_data(offset, data)
        }

        fn download_sub_data(&self, offset: usize, data: &mut [u8]) -> Result<(), GlError> {
            <Self as AsRef<BaseBuffer>>::as_ref(self).download_sub_data(offset, data)
        }

        fn bind_base(&self, target: IndexedBufferTarget, index: GLuint) -> Result<(), GlError> {
            <Self as AsRef<BaseBuffer>>::as_ref(self).bind_base(target, index)
        }
    }

    /// An OpenGL buffer
    struct BaseBuffer {
        id: GLuint,
        size: usize,
    }

    impl BaseBuffer {
        fn new(size: usize) -> Result<Self, GlError> {
            let mut id: GLuint = 0;
            unsafe { gl::CreateBuffers(1, &mut id) };
            GlError::check_msg("glCreateBuffers")?;
            Ok(Self { id, size })
        }
    }

    impl Buffer for BaseBuffer {
        fn invalidate(&mut self) -> Result<(), GlError> {
            unsafe { gl::InvalidateBufferData(self.id) };
            GlError::check_msg("glInvalidateBufferData")
        }

        fn upload_sub_data(&mut self, offset: usize, data: &[u8]) -> Result<(), GlError> {
            unsafe {
                gl::NamedBufferSubData(
                    self.id,
                    offset.try_into().expect("offset"),
                    data.len().try_into().expect("data.len()"),
                    data.as_ptr() as *const _,
                )
            };
            GlError::check_msg("glNamedBufferSubData")
        }

        fn download_sub_data(&self, offset: usize, data: &mut [u8]) -> Result<(), GlError> {
            unsafe {
                gl::GetNamedBufferSubData(
                    self.id,
                    offset.try_into().expect("offset"),
                    data.len().try_into().expect("data.len()"),
                    data.as_mut_ptr() as *mut _,
                )
            };
            GlError::check_msg("glGetNamedBufferSubData")
        }

        fn bind_base(&self, target: IndexedBufferTarget, index: GLuint) -> Result<(), GlError> {
            unsafe { gl::BindBufferBase(target as GLenum, index, self.id) };
            GlError::check_msg("glBindBufferBase")
        }
    }

    impl Drop for BaseBuffer {
        fn drop(&mut self) {
            unsafe { gl::DeleteBuffers(1, &self.id) };
        }
    }

    /// An OpenGL immutable buffer
    pub struct ImmutableBuffer {
        internal_buffer: BaseBuffer,
        flags: GLbitfield,
    }

    impl ImmutableBuffer {
        fn new_internal(size: usize, flags: GLbitfield) -> Result<Self, GlError> {
            let internal_buffer = BaseBuffer::new(size)
                .add_context(format_args!("size={}", size))?;
            Ok(Self { internal_buffer, flags })
        }

        pub fn new_uninitialized(size: usize, flags: GLbitfield) -> Result<Self, GlError> {
            let buffer = Self::new_internal(size, flags)?;

            unsafe { gl::NamedBufferStorage(buffer.internal_buffer.id, size.try_into().unwrap(), null(), flags) };
            GlError::check_msg("glNamedBufferStorage(NULL)")?;

            Ok(buffer)
        }

        pub fn new_initialized(data: &[u8], flags: GLbitfield) -> Result<Self, GlError> {
            let buffer = Self::new_internal(data.len(), flags)?;

            unsafe { gl::NamedBufferStorage(buffer.internal_buffer.id, data.len().try_into().unwrap(), data.as_ptr() as *const _, flags) };
            GlError::check_msg("glNamedBufferStorage(data.as_ptr())")?;

            Ok(buffer)
        }
    }

    impl AsRef<BaseBuffer> for ImmutableBuffer {
        fn as_ref(&self) -> &BaseBuffer {
            &self.internal_buffer
        }
    }

    impl AsMut<BaseBuffer> for ImmutableBuffer {
        fn as_mut(&mut self) -> &mut BaseBuffer {
            &mut self.internal_buffer
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_big_integer_behaves_as_expected() {
        use rug::Integer;
        use std::cmp::Ordering::{self, *};
        type DigestArr = [u8; 32];

        fn to_int(digits: &[u8]) -> Integer {
            let res = Integer::from_digits(digits, rug::integer::Order::MsfBe);
            assert_eq!(res, Integer::from_digits(digits, rug::integer::Order::MsfLe));
            res
        }

        fn test(hash: &[u8], target: &[u8], order: Ordering) {
            let (hash, target) = (to_int(hash), to_int(target));
            assert_eq!(PartialOrd::partial_cmp(&hash, &target), Some(order),
                       "hash: {hash}, target: {target}");
        }

        test(b"\x00", b"\x00", Equal);
        test(b"\x02", b"\x00", Greater);
        test(b"\x02", b"\x80", Less);

        test(b"\x0201234567", b"\x8001234567", Less);
        test(b"\x8001234567", b"\x8001234567", Equal);
        test(b"01234567890abcde01234567890abcde", b"01234567890abcde01234567890abcde", Equal);
        test(b"01234567890abcde01234567890abcde", b"91234567890abcde01234567890abcde", Less);
        test(b"01234567890abcde01234567890abcde", b"01234567890abcde91234567890abcde", Less);
    }

    #[test]
    fn test_expand_target_hash() {
        fn test_hex(compact_target: u32, target_hash_hex: Option<&str>) {
            let target_hash = Miner::expand_target_to_bytes(CompactTarget::from_array(compact_target.to_be_bytes()))
                .map(|h| hex::encode_upper(&h));
            assert_eq!(target_hash.as_ref().map(|s| s.as_str()), target_hash_hex,
                       "compact_target=0x{:08x}", compact_target)
        }

        test_hex(0x00000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));
        test_hex(0x03000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));
        test_hex(0xFF000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0x03000001, Some("0000000000000000000000000000000000000000000000000000000000000001"));
        test_hex(0x037FFFFF, Some("00000000000000000000000000000000000000000000000000000000007FFFFF"));
        test_hex(0x03FFFFFF, Some("00000000000000000000000000000000000000000000000000000000007FFFFF"));
        test_hex(0x02FFFFFF, Some("0000000000000000000000000000000000000000000000000000000000007FFF"));
        test_hex(0x01FFFFFF, Some("000000000000000000000000000000000000000000000000000000000000007F"));
        test_hex(0x00FFFFFF, Some("0000000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0x22000001, Some("0100000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0xFFFFFFFF, None);
    }

    const TEST_MINING_DIFFICULTY: &'static [u8] = b"\x22\x00\x00\x01";

    fn test_block_header(difficulty: &[u8]) -> BlockHeader {
        BlockHeader {
            version: 1337,
            bits: 10973,
            nonce_and_mining_tx_hash: (vec![], "abcde".to_string()),
            b_num: 2398927,
            timestamp: 29837637,
            difficulty: difficulty.to_vec(),
            seed_value: b"2983zuifsigezd".to_vec(),
            previous_hash: Some("jeff".to_string()),
            txs_merkle_root_and_hash: ("merkle_root".to_string(), "hash".to_string()),
        }
    }

    #[test]
    fn verify_cpu() {
        let header = test_block_header(TEST_MINING_DIFFICULTY);
        assert_eq!(Miner::generate_pow_block_cpu(&header, 0, 1024),
                   Ok(Some(28)));

        let header = test_block_header(&[]);
        assert_eq!(Miner::generate_pow_block_cpu(&header, 0, 1024),
                   Ok(Some(455)));
    }

    #[test]
    fn verify_gpu() {
        let mut miner = Miner::new().unwrap();

        let header = test_block_header(TEST_MINING_DIFFICULTY);
        assert_eq!(miner.generate_pow_block_gpu(&header, 0, 1024),
                   Ok(Some(28)));

        let header = test_block_header(&[]);
        assert_eq!(miner.generate_pow_block_gpu(&header, 0, 1024),
                   Ok(Some(455)));
    }
}