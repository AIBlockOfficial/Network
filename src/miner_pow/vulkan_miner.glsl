#version 460 core
#extension GL_ARB_compute_shader : require

#extension GL_EXT_shader_explicit_arithmetic_types_int8  : require
#extension GL_EXT_shader_explicit_arithmetic_types_int32 : require
#extension GL_ARB_gpu_shader_int64 : require

layout(local_size_x = 256) in;

#include "vulkan_miner_sha3_256.glsl"

layout(constant_id = 0) const uint BLOCK_HEADER_MAX_BYTES = 1;
layout(constant_id = 1) const uint u_blockHeader_leadingBytesCount = 1;
layout(constant_id = 2) const uint u_blockHeader_trailingBytesCount = 1;

layout(constant_id = 10) const uint u_difficultyFunction = 0;
layout(constant_id = 11) const uint u_leadingZeroes_miningDifficulty = 0;

layout(std140, set = 0, binding = 0) uniform Uniforms {
    uint u_firstNonce;

    uint u_compactTarget_expanded[SHA3_256_BYTES];
    uint u_blockHeader_bytes[BLOCK_HEADER_MAX_BYTES];
};

const uint DIFFICULTY_FUNCTION_LEADING_ZEROES = 1;
const uint DIFFICULTY_FUNCTION_COMPACT_TARGET = 2;

layout(std430, set = 0, binding = 1) volatile restrict buffer Response {
    uint status;        // initialized to 0
    uint success_nonce; // initialized to u32::MAX
    uint error_code;    // initialized to 0
} b_response;

const uint RESPONSE_STATUS_NONE = 0;
const uint RESPONSE_STATUS_SUCCESS = 1;
const uint RESPONSE_STATUS_ERROR = 2;

const uint ERROR_CODE_INVALID_DIFFICULTY_FUNCTION = 1;

void respond_success(uint32_t nonce) {
    // race to change the response status from NONE to SUCCESS
    atomicCompSwap(b_response.status, RESPONSE_STATUS_NONE, RESPONSE_STATUS_SUCCESS);
    // set the response nonce to the min() of the current response nonce and this thread's nonce.
    //   this ensures that we always return the first valid nonce to the CPU, and works because
    //   b_response.success_nonce should be initialized to the maximum uint32_t value.
    atomicMin(b_response.success_nonce, nonce);
}

void respond_error(uint error_code) {
    // set the response status to ERROR, regardless of what the current value is
    // (this will hide any future threads which try to respond with success)
    atomicMax(b_response.status, RESPONSE_STATUS_ERROR);
    b_response.error_code = error_code;
}

void main() {
    uint32_t nonce = u_firstNonce + gl_GlobalInvocationID.x;

    if (b_response.success_nonce < nonce) {
        // If another thread has already successfully found a nonce lower than this thread, exit immediately.
        return;
    }

    // hash the block header with the nonce inserted in the correct location
    sha3_256_context ctx;
    sha3_256_Init(ctx);

    // hash the block header, inserting the nonce in the correct location
    for (uint i = 0; i < u_blockHeader_leadingBytesCount; i++)
        sha3_256_Update(ctx, uint8_t(u_blockHeader_bytes[i] & 0xFFu));
    sha3_256_Update_int32le(ctx, nonce);
    for (uint i = 0; i < u_blockHeader_trailingBytesCount; i++)
        sha3_256_Update(ctx, uint8_t(u_blockHeader_bytes[u_blockHeader_leadingBytesCount + i] & 0xFFu));

    uint8_t[SHA3_256_BYTES] hash = sha3_256_Finalize(ctx);

    switch (u_difficultyFunction) {
        default: {
            respond_error(ERROR_CODE_INVALID_DIFFICULTY_FUNCTION);
            return;
        }
        case DIFFICULTY_FUNCTION_LEADING_ZEROES: {
            // check that the first u_leadingZeroes_miningDifficulty bytes of the hash are 0
            for (uint i = 0; i < u_leadingZeroes_miningDifficulty; i++)
                if (hash[i] != 0)
                    return;

            respond_success(nonce);
            return;
        }
        case DIFFICULTY_FUNCTION_COMPACT_TARGET: {
            // check that the hash is lexicographically less than or equal to the expanded target hash
            for (uint i = 0; i < SHA3_256_BYTES; i++) {
                if (uint32_t(hash[i]) > uint32_t(u_compactTarget_expanded[i])) {
                    return;
                } else if (uint32_t(hash[i]) < uint32_t(u_compactTarget_expanded[i])) {
                    break;
                }
            }

            respond_success(nonce);
            return;
        }
    }
}
