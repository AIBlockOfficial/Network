use std::convert::TryInto;
use std::ffi::CStr;
//use std::ops::Deref;
use gl::types::*;
use glfw::{Glfw, GlfwReceiver, OpenGlProfileHint, PWindow, WindowEvent, WindowHint, WindowMode};
use tw_chain::crypto::sha3_256;
use tw_chain::primitives::block::BlockHeader;
use crate::constants::{MINING_DIFFICULTY, POW_NONCE_LEN};
use crate::opengl_miner::gl_wrapper::{AddContext, GlError, ImmutableBuffer, IndexedBufferTarget, Program, Shader, ShaderType, UniformLocation};

// libglfw3-dev libxrandr-dev libxinerama-dev libxcursor-dev libxi-dev

pub struct Miner {
    mine_program: Program,
    mine_program_first_nonce_uniform: UniformLocation,
    mine_program_header_length_uniform: UniformLocation,
    mine_program_header_nonce_offset_uniform: UniformLocation,
    mine_program_difficulty_function_uniform: UniformLocation,
    mine_program_leading_zeroes_mining_difficulty_uniform: UniformLocation,

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
            mine_program_first_nonce_uniform: program.uniform_location("u_firstNonce").unwrap(),
            mine_program_header_length_uniform: program.uniform_location("u_blockHeader_length").unwrap(),
            mine_program_header_nonce_offset_uniform: program.uniform_location("u_blockHeader_nonceOffset").unwrap(),
            mine_program_difficulty_function_uniform: program.uniform_location("u_difficultyFunction").unwrap(),
            mine_program_leading_zeroes_mining_difficulty_uniform: program.uniform_location("u_leadingZeroes_miningDifficulty").unwrap(),
            mine_program: program,

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

    /// Tries to generate a Proof-of-Work for a block.
    fn generate_pow_block_cpu(
        block_header: &BlockHeader,
        first_nonce: u32,
        nonce_count: u32,
    ) -> Result<Option<u32>, ()> {
        assert!(block_header.difficulty.is_empty(), "non-empty difficulty function not supported");

        let (mut block_header_bytes, block_header_nonce_offset) =
            Self::find_block_header_nonce_location(block_header);
        let block_header_nonce_offset = block_header_nonce_offset as usize;

        // we accumulate the result into an Option rather than immediately returning the first
        // result so that we still have to compute every hash, allowing a fair hash rate comparison
        // with the GPU implementation.
        let mut result = None;

        for nonce in 0..nonce_count {
            let nonce = nonce.wrapping_add(first_nonce);

            block_header_bytes.as_mut_slice()
                [block_header_nonce_offset..block_header_nonce_offset + 4]
                .copy_from_slice(&nonce.to_ne_bytes());

            let hash = sha3_256::digest(&block_header_bytes);

            let hash_prefix = hash.first_chunk::<MINING_DIFFICULTY>().unwrap();
            if hash_prefix == &[0u8; MINING_DIFFICULTY] && result.is_none() {
                result = Some(nonce);
            }

            std::hint::black_box(hash);
        }

        Ok(result)
    }

    /// Tries to generate a Proof-of-Work for a block.
    #[allow(non_snake_case)]
    fn generate_pow_block(
        &mut self,
        block_header: &BlockHeader,
        first_nonce: u32,
        nonce_count: u32,
    ) -> Result<Option<u32>, GlError> {
        assert!(block_header.difficulty.is_empty(), "non-empty difficulty function not supported");

        let (block_header_bytes, block_header_nonce_offset) =
            Self::find_block_header_nonce_location(block_header);
        let block_header_length: u32 = block_header_bytes.len().try_into().unwrap();

        let mut work_group_size_arr = [0 as GLint; 3];
        unsafe { gl::GetProgramiv(self.mine_program.id, gl::COMPUTE_WORK_GROUP_SIZE, work_group_size_arr.as_mut_ptr()) };
        GlError::check_msg("glGetProgramiv(COMPUTE_WORK_GROUP_SIZE)")?;
        let work_group_size = work_group_size_arr.iter().cloned().reduce(<GLint as std::ops::Mul>::mul).unwrap() as u32;

        let dispatch_count = nonce_count.div_ceil(work_group_size);
        //let hash_count = work_group_size * dispatch_count;
        //let hash_buffer_capacity_int32s = hash_count * 32;
        //let hash_buffer_capacity_bytes = hash_buffer_capacity_int32s as usize * 4;

        self.mine_program.set_uniform_1ui(self.mine_program_first_nonce_uniform, first_nonce)?;
        self.mine_program.set_uniform_1ui(self.mine_program_header_length_uniform, block_header_length)?;
        self.mine_program.set_uniform_1ui(self.mine_program_header_nonce_offset_uniform, block_header_nonce_offset)?;

        if block_header.difficulty.is_empty() {
            // DIFFICULTY_FUNCTION_LEADING_ZEROES
            self.mine_program.set_uniform_1ui(self.mine_program_difficulty_function_uniform, 1)?;
            self.mine_program.set_uniform_1ui(self.mine_program_leading_zeroes_mining_difficulty_uniform, MINING_DIFFICULTY.try_into().unwrap())?;
        } else {
            assert!(false, "non-empty difficulty function not supported");
        }

        let block_header_buffer_data = block_header_bytes.iter()
            .map(|b| (*b as u32).to_ne_bytes())
            .collect::<Vec<_>>()
            .concat();
        let block_header_buffer = ImmutableBuffer::new_initialized(&block_header_buffer_data, 0)
            .add_msg("BlockHeader buffer")?;

        //let mut hash_buffer = ImmutableBuffer::new_uninitialized(hash_buffer_capacity_bytes, 0)
        //    .add_msg("HashOutput buffer")?;

        const RESPONSE_BUFFER_CAPACITY: usize = 12;
        let response_buffer_data = [ 0u32, u32::MAX, 0u32 ];
        //let mut response_buffer_data = [0u8; RESPONSE_BUFFER_CAPACITY];
        let mut response_buffer_data : [u8; RESPONSE_BUFFER_CAPACITY] = unsafe { std::mem::transmute(response_buffer_data.map(u32::to_ne_bytes)) };
        let response_buffer = ImmutableBuffer::new_initialized(&response_buffer_data, 0)
            .add_msg("Response buffer")?;

        block_header_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 0).unwrap();
        //hash_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 1).unwrap();
        response_buffer.bind_base(IndexedBufferTarget::ShaderStorageBuffer, 2).unwrap();

        //hash_buffer.invalidate().unwrap();
        self.mine_program.bind().unwrap();

        unsafe { gl::DispatchCompute(dispatch_count, 1, 1) };
        GlError::check_msg("glDispatchCompute").add_msg("Dispatch miner")?;

        //unsafe { gl::Finish() };
        unsafe { gl::MemoryBarrier(gl::BUFFER_UPDATE_BARRIER_BIT) };

        //let mut hash_buffer_data = vec![0u8; hash_buffer_capacity_bytes];
        //hash_buffer.download(0, &mut hash_buffer_data).unwrap();
        //std::hint::black_box(&hash_buffer_data);

        response_buffer.download(0, &mut response_buffer_data).unwrap();

        let response_status = u32::from_ne_bytes(*response_buffer_data[0..].first_chunk().unwrap());
        let response_success_nonce = u32::from_ne_bytes(*response_buffer_data[4..].first_chunk().unwrap());
        let response_error_code = u32::from_ne_bytes(*response_buffer_data[8..].first_chunk().unwrap());

        match response_status {
            // RESPONSE_STATUS_NONE
            0 => return Ok(None),
            // RESPONSE_STATUS_SUCCESS
            1 => return Ok(Some(response_success_nonce)),
            // RESPONSE_STATUS_ERROR_CODE
            2 => panic!("compute shader returned error code 0x{:04x}: {}",
                        response_error_code,
                        match response_error_code {
                            1 => "INVALID_DIFFICULTY_FUNCTION",
                            _ => "(unknown)",
                        }),
            _ => panic!("compute shader returned unknown response status 0x{:04x}", response_status),
        }

        /*if true {
            return Ok(None);
        }

        //println!("hash_buffer: {}", hex::encode(&hash_buffer_data));

        let mut hash_buffer_idx = 0usize;
        let mut expected_block_header = block_header_bytes.clone();
        for nonce in 0u32..hash_count {
            if true {
                expected_block_header.as_mut_slice()
                    [(block_header_nonce_offset as usize)..((block_header_nonce_offset + 4) as usize)]
                    .copy_from_slice(&nonce.to_ne_bytes());
            } else if true {
                expected_block_header[block_header_nonce_offset as usize] = nonce as u8;
            }

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

            //println!("nonce {nonce} => expected hash=\"{}\", gpu hash=\"{}\"", hex::encode(&expected_hash), hex::encode(&constructed_hash));
            assert_eq!(expected_hash, constructed_hash, "block_header_length={block_header_length}, nonce={nonce}");
        }

        Ok(None)*/
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
mod test {
    use std::time::{Duration, Instant};
    use super::*;

    fn test_block_header() -> BlockHeader {
        BlockHeader {
            version: 1337,
            bits: 10973,
            nonce_and_mining_tx_hash: (vec![], "abcde".to_string()),
            b_num: 2398927,
            timestamp: 29837637,
            difficulty: vec![],
            seed_value: b"2983zuifsigezd".to_vec(),
            previous_hash: Some("jeff".to_string()),
            txs_merkle_root_and_hash: ("merkle_root".to_string(), "hash".to_string()),
        }
    }

    fn fmt_per_second(qty: f64, elapsed: Duration) -> String {
        let qty_per_second = qty / elapsed.as_secs_f64();

        let (mut divisor, mut prefix) = (1f64, "");
        for (next_divisor, next_prefix) in [
            (1000f64, "k"),
            (1000000f64, "M"),
            (1000000000f64, "G"),
        ] {
            if qty_per_second > next_divisor {
                (divisor, prefix) = (next_divisor, next_prefix);
            }
        }
        format!("{:.2}{}", qty_per_second / divisor, prefix)
    }

    #[test]
    fn test_cpu() {
        let header = test_block_header();

        for nonce_count in (0..15).map(|i| 1u32 << i) {
            let start_time = Instant::now();
            let nonce = Miner::generate_pow_block_cpu(&header, 0, nonce_count).unwrap();
            let elapsed = start_time.elapsed();
            println!("calculated {} hashes in {:?} ({}H/s) -> nonce={:?}", nonce_count, elapsed, fmt_per_second(nonce_count as f64, elapsed), nonce);
        }
    }

    #[test]
    fn test_gpu() {
        let header = test_block_header();

        let mut miner = Miner::new().unwrap();
        for nonce_count in (0..21).map(|i| 1u32 << i) {
            let start_time = Instant::now();
            let nonce = miner.generate_pow_block(&header, 0, nonce_count).unwrap();
            let elapsed = start_time.elapsed();
            println!("calculated {} hashes in {:?} ({}H/s) -> nonce={:?}", nonce_count, elapsed, fmt_per_second(nonce_count as f64, elapsed), nonce);
        }
    }
}
