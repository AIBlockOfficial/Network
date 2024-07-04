use std::convert::TryInto;
use std::ffi::CStr;
use std::{error, fmt};
use std::sync::{Mutex, MutexGuard, OnceLock};
use gl::types::*;
use glfw::{Context, Glfw, GlfwReceiver, OpenGlProfileHint, PWindow, WindowEvent, WindowHint, WindowMode};
use tracing::{debug, info, warn};
use crate::miner_pow::{MinerStatistics, SHA3_256PoWMiner, PoWDifficulty, MineError};
use crate::miner_pow::opengl::gl_error::{AddContext, CompileShaderError, GlError, LinkProgramError};
use crate::miner_pow::opengl::gl_wrapper::{Buffer, GetIntIndexedType, GetProgramIntType, GetStringType, ImmutableBuffer, IndexedBufferTarget, MemoryBarrierBit, Program, Shader, ShaderType, UniformLocation};
use crate::utils::split_range_into_blocks;

// libglfw3-dev libxrandr-dev libxinerama-dev libxcursor-dev libxi-dev

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum OpenGlMinerError {
    LockFailed,
    InitializeGlfw(glfw::InitError),
    CreateGlfwWindow,
    CompileShader(CompileShaderError),
    LinkProgram(LinkProgramError),
    GlError(GlError),
}

impl fmt::Display for OpenGlMinerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::LockFailed =>
                f.write_str("couldn't acquire global lock for using GLFW context"),
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

impl error::Error for OpenGlMinerError {}

#[derive(Debug)]
struct GlfwContext<Ctx: fmt::Debug> {
    mutex: Mutex<Ctx>,

    glfw_window: PWindow,
    glfw_events: GlfwReceiver<(f64, WindowEvent)>,
    glfw: Glfw,
    glfw_mutex_guard: MutexGuard<'static, ()>,
}

impl<Ctx: fmt::Debug> Drop for GlfwContext<Ctx> {
    fn drop(&mut self) {
        debug!("Thread {} is destroying the GLFW context", std::thread::current().name().unwrap_or("<unnamed thread>"));
    }
}

#[derive(Debug)]
pub struct GlfwContextGuard<'glfw, Ctx: fmt::Debug> {
    mutex_guard: MutexGuard<'glfw, Ctx>,
}

impl<'glfw, Ctx: fmt::Debug> std::ops::Deref for GlfwContextGuard<'glfw, Ctx> {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &*self.mutex_guard
    }
}

impl<'glfw, Ctx: fmt::Debug> std::ops::DerefMut for GlfwContextGuard<'glfw, Ctx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.mutex_guard
    }
}

impl<'glfw, Ctx: fmt::Debug> Drop for GlfwContextGuard<'glfw, Ctx> {
    fn drop(&mut self) {
        // un-bind the context before releasing the context's mutex
        debug!("Thread {} is releasing the GLFW context", std::thread::current().name().unwrap_or("<unnamed thread>"));
        glfw::make_context_current(None);
    }
}

static GLFW_MUTEX: Mutex<()> = Mutex::new(());

impl<Ctx: fmt::Debug> GlfwContext<Ctx> {
    fn new(f: impl FnOnce() -> Result<Ctx, OpenGlMinerError>) -> Result<Self, OpenGlMinerError> {
        debug!("Thread {} is creating the GLFW context", std::thread::current().name().unwrap_or("<unnamed thread>"));

        let glfw_mutex_guard = GLFW_MUTEX.lock()
            .map_err(|_| OpenGlMinerError::LockFailed)?;

        let mut glfw = glfw::init(glfw::fail_on_errors)
            .map_err(OpenGlMinerError::InitializeGlfw)?;

        glfw.window_hint(WindowHint::ContextVersion(4, 5));
        glfw.window_hint(WindowHint::OpenGlProfile(OpenGlProfileHint::Core));
        glfw.window_hint(WindowHint::Visible(false));
        let (mut window, events) = glfw.create_window(256, 256, "AIBlock Miner", WindowMode::Windowed)
            .ok_or(OpenGlMinerError::CreateGlfwWindow)?;

        gl::load_with(|name| window.get_proc_address(name) as *const _);

        let vendor = gl_wrapper::get_string(GetStringType::VENDOR).unwrap();
        let renderer = gl_wrapper::get_string(GetStringType::RENDERER).unwrap();
        let version = gl_wrapper::get_string(GetStringType::VERSION).unwrap();
        let glsl_version = gl_wrapper::get_string(GetStringType::SHADING_LANGUAGE_VERSION).unwrap();
        info!("Created OpenGL context:\n  Vendor: {vendor}\n  Renderer: {renderer}\n  Version: {version}\n  GLSL version: {glsl_version}");

        let ctx = f()?;

        glfw::make_context_current(None);

        Ok(Self {
            mutex: Mutex::new(ctx),
            glfw_window: window,
            glfw_events: events,
            glfw,
            glfw_mutex_guard,
        })
    }

    fn make_current(&mut self) -> GlfwContextGuard<Ctx> {
        let guard = self.mutex.lock().unwrap();
        debug!("Thread {} is acquiring the GLFW context", std::thread::current().name().unwrap_or("<unnamed thread>"));
        self.glfw_window.make_current();
        GlfwContextGuard { mutex_guard: guard }
    }
}

#[derive(Debug)]
pub struct OpenGlMiner {
    program: Program,
    program_first_nonce_uniform: UniformLocation,
    program_header_length_uniform: UniformLocation,
    program_header_nonce_offset_uniform: UniformLocation,
    program_difficulty_function_uniform: UniformLocation,
    program_leading_zeroes_mining_difficulty_uniform: UniformLocation,
    program_compact_target_expanded_uniform: UniformLocation,
    program_work_group_size: u32,
    max_work_group_count: u32,

    glfw_context: GlfwContext<()>,
}

static mut GLOBAL_MINER: OnceLock<GlfwContext<OpenGlMiner>> = OnceLock::new();

impl OpenGlMiner {
    pub fn new() -> Result<Self, OpenGlMinerError> {
        let mut glfw_context = GlfwContext::new(|| Ok(()))?;
        let guard = glfw_context.make_current();

        debug!("compiling shader...");

        // compile and link the compute shader
        let shader = Shader::compile(
            ShaderType::Compute,
            &[include_str!("opengl_miner.glsl")],
            //&[std::fs::read_to_string("src/miner_pow/opengl_miner.glsl").expect("failed to read source").as_str()]
        ).map_err(OpenGlMinerError::CompileShader)?;

        let program = Program::link(&[&shader])
            .map_err(OpenGlMinerError::LinkProgram)?;

        // figure out the compute shader's work group size
        let work_group_size_arr = program.get_program_int::<3>(GetProgramIntType::ComputeWorkGroupSize)
            .map_err(OpenGlMinerError::GlError)?;
        assert_eq!(&work_group_size_arr[1..], &[1; 2],
                   "work group size should be 1 on y and z axes...");

        let max_work_group_count = gl_wrapper::get_int(GetIntIndexedType::MAX_COMPUTE_WORK_GROUP_COUNT, 0)
            .map_err(OpenGlMinerError::GlError)? as u32;

        let program_first_nonce_uniform = program.uniform_location("u_firstNonce").unwrap();
        let program_header_length_uniform = program.uniform_location("u_blockHeader_length").unwrap();
        let program_header_nonce_offset_uniform = program.uniform_location("u_blockHeader_nonceOffset").unwrap();
        let program_difficulty_function_uniform = program.uniform_location("u_difficultyFunction").unwrap();
        let program_leading_zeroes_mining_difficulty_uniform = program.uniform_location("u_leadingZeroes_miningDifficulty").unwrap();
        let program_compact_target_expanded_uniform = program.uniform_location("u_compactTarget_expanded").unwrap();

        drop(guard);

        Ok(Self {
            program_first_nonce_uniform,
            program_header_length_uniform,
            program_header_nonce_offset_uniform,
            program_difficulty_function_uniform,
            program_leading_zeroes_mining_difficulty_uniform,
            program_compact_target_expanded_uniform,
            program_work_group_size: work_group_size_arr[0].try_into().unwrap(),
            max_work_group_count,
            program,

            glfw_context,
        })
    }
}

impl SHA3_256PoWMiner for OpenGlMiner {
    fn is_hw_accelerated(&self) -> bool {
        true
    }

    fn min_nonce_count(&self) -> u32 {
        self.program_work_group_size
    }

    fn nonce_peel_amount(&self) -> u32 {
        // TODO: Auto-detect this based on device speed
        1u32 << 20
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

        let dispatch_count = nonce_count.div_ceil(self.program_work_group_size);

        if dispatch_count > self.max_work_group_count {
            warn!("OpenGL miner dispatched with too high nonce_count {} (work group size={}, max \
                   work group count={})! Splitting into multiple dispatches.",
                  nonce_count, self.program_work_group_size, self.max_work_group_count);

            for (first, count) in split_range_into_blocks(
                first_nonce,
                nonce_count,
                self.max_work_group_count * self.program_work_group_size
            ) {
                match self.generate_pow_internal(leading_bytes, trailing_bytes, difficulty, first, count, statistics)? {
                    Some(nonce) => return Ok(Some(nonce)),
                    None => (),
                };
            }
            return Ok(None);
        }

        let mut stats_updater = statistics.update_safe();

        let block_header_nonce_offset = leading_bytes.len().try_into().unwrap();
        let header_bytes = [ leading_bytes, &0u32.to_le_bytes(), trailing_bytes ].concat();
        let block_header_length = header_bytes.len().try_into().unwrap();

        let _guard = self.glfw_context.make_current();

        self.program.set_uniform_1ui(self.program_first_nonce_uniform, first_nonce)
            .map_err(MineError::wrap)?;
        self.program.set_uniform_1ui(self.program_header_length_uniform, block_header_length)
            .map_err(MineError::wrap)?;
        self.program.set_uniform_1ui(self.program_header_nonce_offset_uniform, block_header_nonce_offset)
            .map_err(MineError::wrap)?;

        match difficulty {
            // The target value is higher than the largest possible SHA3-256 hash. Therefore,
            // every hash will meet the required difficulty threshold, so we can just return
            // an arbitrary nonce.
            PoWDifficulty::TargetHashAlwaysPass => return Ok(Some(first_nonce)),

            PoWDifficulty::LeadingZeroBytes { leading_zeroes } => {
                // DIFFICULTY_FUNCTION_LEADING_ZEROES
                self.program.set_uniform_1ui(self.program_difficulty_function_uniform, 1)
                    .map_err(MineError::wrap)?;
                self.program.set_uniform_1ui(self.program_leading_zeroes_mining_difficulty_uniform, (*leading_zeroes).try_into().unwrap())
                    .map_err(MineError::wrap)?;
            },
            PoWDifficulty::TargetHash { target_hash } => {
                // DIFFICULTY_FUNCTION_COMPACT_TARGET
                self.program.set_uniform_1ui(self.program_difficulty_function_uniform, 2)
                    .map_err(MineError::wrap)?;
                self.program.set_uniform_1uiv(self.program_compact_target_expanded_uniform,
                                              &target_hash.map(|b| b as u32))
                    .map_err(MineError::wrap)?;
            },
        }

        let block_header_buffer_data = header_bytes.iter()
            .map(|b| (*b as u32).to_ne_bytes())
            .collect::<Vec<_>>()
            .concat();
        let block_header_buffer = ImmutableBuffer::new_initialized(&block_header_buffer_data, 0)
            .add_context("BlockHeader buffer")
            .map_err(MineError::wrap)?;

        const RESPONSE_BUFFER_CAPACITY: usize = 12;
        let response_buffer_data = [ 0u32, u32::MAX, 0u32 ];
        let mut response_buffer_data : [u8; RESPONSE_BUFFER_CAPACITY] = unsafe { std::mem::transmute(response_buffer_data.map(u32::to_ne_bytes)) };
        let response_buffer = ImmutableBuffer::new_initialized(&response_buffer_data, 0)
            .add_context("Response buffer")
            .map_err(MineError::wrap)?;

        block_header_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 0).unwrap();
        response_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 2).unwrap();

        self.program.bind().unwrap();

        gl_wrapper::dispatch_compute(dispatch_count, 1, 1)
            .map_err(MineError::wrap)?;
        gl_wrapper::memory_barrier(&[ MemoryBarrierBit::BufferUpdate ])
            .map_err(MineError::wrap)?;

        response_buffer.download_sub_data(0, &mut response_buffer_data).unwrap();

        // Now that the hashes have been computed, update the statistics
        stats_updater.computed_hashes(
            (dispatch_count as u128) * (self.program_work_group_size as u128));

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
    #[derive(Debug)]
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
                warn!("Shader compile warnings: {info_log}");
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
    #[derive(Debug)]
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
                warn!("Program link warnings: {info_log}");
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
    pub trait Buffer: fmt::Debug {
        fn invalidate(&mut self) -> Result<(), GlError>;

        fn upload_sub_data(&mut self, offset: usize, data: &[u8]) -> Result<(), GlError>;

        fn download_sub_data(&self, offset: usize, data: &mut [u8]) -> Result<(), GlError>;

        fn bind_base(&self, target: IndexedBufferTarget, index: GLuint) -> Result<(), GlError>;
    }

    impl<T: AsRef<BaseBuffer> + AsMut<BaseBuffer> + fmt::Debug> Buffer for T {
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
    #[derive(Debug)]
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
    #[derive(Debug)]
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
