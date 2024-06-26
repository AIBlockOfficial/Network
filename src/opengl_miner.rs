use std::convert::TryInto;
use std::ffi::CStr;
//use std::ops::Deref;
use gl::types::*;
use glfw::{Glfw, GlfwReceiver, OpenGlProfileHint, PWindow, WindowEvent, WindowHint, WindowMode};
use tw_chain::crypto::sha3_256;
use tw_chain::primitives::block::BlockHeader;
use crate::constants::POW_NONCE_LEN;
use crate::opengl_miner::gl_wrapper::{AddContext, GlError, ImmutableBuffer, IndexedBufferTarget, Program, Shader, ShaderType, UniformLocation};

// libglfw3-dev libxrandr-dev libxinerama-dev libxcursor-dev libxi-dev

pub struct Miner {
    mine_program: Program,
    mine_program_header_length_uniform: UniformLocation,
    mine_program_header_nonce_offset_uniform: UniformLocation,

    glfw_window: PWindow,
    glfw_events: GlfwReceiver<(f64, WindowEvent)>,
    glfw: Glfw,
}

impl Miner {
    pub fn new() -> Result<Self, &'static str> {
        let mut glfw = glfw::init(glfw::fail_on_errors)
            .map_err(|_| "Failed to initialize GLFW library")?;

        glfw.window_hint(WindowHint::ContextVersion(4, 5));
        glfw.window_hint(WindowHint::OpenGlProfile(OpenGlProfileHint::Core));
        glfw.window_hint(WindowHint::Visible(false));
        let (mut window, events) = glfw.create_window(256, 256, "AIBlock Miner", WindowMode::Windowed)
            .ok_or("Failed to create GLFW window")?;

        gl::load_with(|name| window.get_proc_address(name) as *const _);

        let vendor = unsafe { CStr::from_ptr(gl::GetString(gl::VENDOR) as *const std::ffi::c_char) }.to_str().unwrap();
        let renderer = unsafe { CStr::from_ptr(gl::GetString(gl::RENDERER) as *const std::ffi::c_char) }.to_str().unwrap();
        let version = unsafe { CStr::from_ptr(gl::GetString(gl::VERSION) as *const std::ffi::c_char) }.to_str().unwrap();
        let glsl_version = unsafe { CStr::from_ptr(gl::GetString(gl::SHADING_LANGUAGE_VERSION) as *const std::ffi::c_char) }.to_str().unwrap();
        println!("Created OpenGL context:\n  Vendor: {vendor}\n  Renderer: {renderer}\n  Version: {version}\n  GLSL version: {glsl_version}");

        let source = std::fs::read_to_string("src/opengl_miner.glsl").expect("failed to read source");

        let shader = Shader::compile(
            ShaderType::Compute,
            //&[include_str!("opengl_miner.glsl")],
            &[ source.as_str() ]
        ).expect("Shader compilation failed?!?");

        let program = Program::link(&[&shader])
            .expect("Shader linking failed?!?");

        Ok(Self {
            mine_program_header_length_uniform: program.uniform_location("u_blockHeader_length").unwrap(),
            mine_program_header_nonce_offset_uniform: program.uniform_location("u_blockHeader_nonceOffset").unwrap(),
            mine_program: program,

            glfw_window: window,
            glfw_events: events,
            glfw,
        })
    }

    #[allow(non_snake_case)]
    fn mine(&mut self, block_header_length: u32) -> Result<(), GlError> {
        const INSERT_NONCE : bool = false;

        //let block_header_length = 172u32;
        //let block_header_length = 8u32;
        let block_header_nonceOffset = 7u32;
        let block_header_bytes = (0..block_header_length).map(|n| n as u8).collect::<Vec<_>>();
        println!("block header: length={block_header_length}, nonce offset={block_header_nonceOffset}");

        let mut work_group_size_arr = [0 as GLint; 3];
        unsafe { gl::GetProgramiv(self.mine_program.id, gl::COMPUTE_WORK_GROUP_SIZE, work_group_size_arr.as_mut_ptr()) };
        GlError::check_msg("glGetProgramiv(COMPUTE_WORK_GROUP_SIZE)")?;
        let work_group_size = work_group_size_arr.iter().cloned().reduce(<GLint as std::ops::Mul>::mul).unwrap() as u32;
        println!("compute shader work group size: {work_group_size_arr:?} -> {work_group_size}");

        let dispatch_count = 2u32;
        let hash_count = work_group_size * dispatch_count;
        let hash_buffer_capacity_int32s = hash_count * 32;
        let hash_buffer_capacity_bytes = hash_buffer_capacity_int32s as usize * 4;

        self.mine_program.set_uniform_1ui(self.mine_program_header_length_uniform, block_header_length)?;
        self.mine_program.set_uniform_1ui(self.mine_program_header_nonce_offset_uniform, block_header_nonceOffset)?;

        let mut block_header_buffer_data = Vec::new();
        for byte in &block_header_bytes {
            block_header_buffer_data.extend_from_slice(&(*byte as u32).to_ne_bytes());
        }
        block_header_buffer_data.push(0); //ensure the buffer isn't empty
        let block_header_buffer = ImmutableBuffer::new_initialized(&block_header_buffer_data, 0)
            .add_msg("BlockHeader buffer")?;

        let mut hash_buffer = ImmutableBuffer::new_uninitialized(hash_buffer_capacity_bytes, 0)
            .add_msg("HashOutput buffer")?;

        block_header_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 0).unwrap();
        hash_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 1).unwrap();

        hash_buffer.invalidate().unwrap();
        self.mine_program.bind().unwrap();

        unsafe { gl::DispatchCompute(dispatch_count, 1, 1) };
        GlError::check_msg("glDispatchCompute").add_msg("Dispatch miner")?;

        //unsafe { gl::Finish() };
        unsafe { gl::MemoryBarrier(gl::BUFFER_UPDATE_BARRIER_BIT) };

        let mut hash_buffer_data = vec![0u8; hash_buffer_capacity_bytes];
        hash_buffer.download(0, &mut hash_buffer_data).unwrap();

        //println!("hash_buffer: {}", hex::encode(&hash_buffer_data));

        let mut hash_buffer_idx = 0usize;
        for nonce in 0u32..hash_count {
            let expected_block_header = if INSERT_NONCE {
                let mut expected_block_header = block_header_bytes.clone();
                expected_block_header.as_mut_slice()
                    [(block_header_nonceOffset as usize)..((block_header_nonceOffset + 4) as usize)]
                    .copy_from_slice(&nonce.to_ne_bytes());
                expected_block_header
            } else {
                // hash the block header in one go, without inserting the nonce
                block_header_bytes.clone()
            };

            let expected_hash : [u8; 32] = sha3_256::digest(&expected_block_header).try_into().unwrap();

            let mut constructed_hash = [0u8; 32];
            for i in 0..32 {
                let byte_u32 = u32::from_ne_bytes(*hash_buffer_data.as_slice()[hash_buffer_idx..].first_chunk::<4>().unwrap());
                assert!(byte_u32 < 256, "{}", byte_u32);

                constructed_hash[i] = byte_u32.try_into().unwrap();

                hash_buffer_idx += 4;
            }

            // incrementing counter
            //let expected_hash : [u8; 32] = std::array::from_fn(|n| (nonce as u8).wrapping_add(n as u8));

            assert_eq!(expected_hash, constructed_hash, "block_header_length={block_header_length}, nonce={nonce}");

            //println!("nonce {nonce} => expected hash=\"{expected_hex}\", gpu hash=\"{constructed_hex}\"");
        }

        Ok(())
    }
}

mod gl_wrapper {
    use std::{error, fmt};
    use std::convert::TryInto;
    use std::ffi::CString;
    use std::ptr::null;
    use super::*;

    pub trait AddContext {
        fn add_msg(self, message: &'static str) -> Self;
    }

    impl<OK, ERR: AddContext> AddContext for Result<OK, ERR> {
        fn add_msg(self, message: &'static str) -> Self {
            self.map_err(|err| err.add_msg(message))
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
            message: Box<str>,
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
        fn add_msg(self, message: &'static str) -> Self {
            Self::WrappedWithMessage { cause: Box::new(self), message: message.into() }
        }
    }

    impl GlError {
        /// Checks the OpenGL context for errors
        pub fn check() -> Result<(), Self> {
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
        pub fn check_msg(message: &'static str) -> Result<(), Self> {
            Self::check().add_msg(message)
        }
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

    /// The location of an OpenGL program uniform
    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub struct UniformLocation {
        location: GLint,
    }

    /// An OpenGL program object
    pub struct Program {
        pub id: GLuint,
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

        pub fn uniform_location(&self, uniform_name: &'static str) -> Result<UniformLocation, GlError> {
            let uniform_name_cstr = CString::new(uniform_name).expect(uniform_name);
            let location = unsafe { gl::GetUniformLocation(self.id, uniform_name_cstr.as_ptr()) };
            GlError::check_msg("glGetUniformLocation").add_msg(uniform_name)?;
            Ok(UniformLocation { location })
        }

        pub fn set_uniform_1i(
            &mut self, location: UniformLocation,
            value: i32,
        ) -> Result<(), GlError> {
            unsafe { gl::ProgramUniform1i(self.id, location.location, value) };
            GlError::check_msg("glProgramUniform1i")
        }

        pub fn set_uniform_1ui(
            &mut self, location: UniformLocation,
            value: u32,
        ) -> Result<(), GlError> {
            unsafe { gl::ProgramUniform1ui(self.id, location.location, value) };
            GlError::check_msg("glProgramUniform1ui")
        }
    }

    impl Drop for Program {
        fn drop(&mut self) {
            unsafe { gl::DeleteProgram(self.id) }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    #[repr(u32)]
    pub enum IndexedBufferTarget {
        UniformBuffer = gl::UNIFORM_BUFFER,
        ShaderStorageBuffer = gl::SHADER_STORAGE_BUFFER,
        AtomicCounterBuffer = gl::ATOMIC_COUNTER_BUFFER,
    }

    /// An OpenGL immutable buffer
    pub struct ImmutableBuffer {
        pub id: GLuint,
        size: usize,
        flags: GLbitfield,
    }

    impl ImmutableBuffer {
        fn new_internal(size: usize, flags: GLbitfield) -> Result<Self, GlError> {
            let mut id: GLuint = 0;
            unsafe { gl::CreateBuffers(1, &mut id) };
            GlError::check_msg("glCreateBuffers")?;

            Ok(Self { id, size, flags })
        }

        pub fn new_uninitialized(size: usize, flags: GLbitfield) -> Result<Self, GlError> {
            let buffer = Self::new_internal(size, flags)?;

            unsafe { gl::NamedBufferStorage(buffer.id, size.try_into().unwrap(), null(), flags) };
            GlError::check_msg("glNamedBufferStorage(NULL)")?;

            Ok(buffer)
        }

        pub fn new_initialized(data: &[u8], flags: GLbitfield) -> Result<Self, GlError> {
            let buffer = Self::new_internal(data.len(), flags)?;

            unsafe { gl::NamedBufferStorage(buffer.id, data.len().try_into().unwrap(), data.as_ptr() as *const _, flags) };
            GlError::check_msg("glNamedBufferStorage(data.as_ptr())")?;

            Ok(buffer)
        }

        pub fn invalidate(&mut self) -> Result<(), GlError> {
            unsafe { gl::InvalidateBufferData(self.id) };
            GlError::check_msg("glInvalidateBufferData")
        }

        pub fn upload(&mut self, offset: usize, data: &[u8]) -> Result<(), GlError> {
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

        pub fn download(&self, offset: usize, data: &mut [u8]) -> Result<(), GlError> {
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

        pub fn bind_base(&self, target: IndexedBufferTarget, index: GLuint) -> Result<(), GlError> {
            unsafe { gl::BindBufferBase(target as GLenum, index, self.id) };
            GlError::check_msg("glBindBufferBase")
        }
    }

    impl Drop for ImmutableBuffer {
        fn drop(&mut self) {
            unsafe { gl::DeleteBuffers(1, &self.id) };
        }
    }
}

#[cfg(test)]
#[test]
fn test() {
    let mut miner = Miner::new().unwrap();
    for block_header_length in 0..=1024 {
        println!("block_header_length={block_header_length}");
        miner.mine(block_header_length).unwrap();
    }
}
