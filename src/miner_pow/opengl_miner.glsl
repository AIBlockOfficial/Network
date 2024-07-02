#version 450 core

//#extension GL_EXT_shader_explicit_arithmetic_types_int8  : enable
//#extension GL_EXT_shader_explicit_arithmetic_types_int32 : enable
#extension GL_ARB_gpu_shader_int64 : require

#define int32_t int
#define uint32_t uint

#define uint8_t uint32_t

layout(local_size_x = 256) in;

//
// SHA-3 implementation
// This is ported from https://github.com/brainhub/SHA3IUF
//

/* 'Words' here refers to uint64_t */
#define SHA3_KECCAK_SPONGE_WORDS uint(((1600)/8/*bits to byte*/)/8/*sizeof(uint64_t)*/)
#define SHA3_256_BYTES uint(32)

#define KECCAK_ROUNDS uint(24)

/*
 * This flag is used to configure "pure" Keccak, as opposed to NIST SHA3.
 */
#define SHA3_USE_KECCAK 0

#define SHA3_ROTL64(x, y) (((x) << (y)) | ((x) >> (64/*(sizeof(uint64_t)*8)*/ - (y))))

struct sha3_context {
    uint64_t saved;             /* the portion of the input message that we
                                 * didn't consume yet */
    uint64_t s[SHA3_KECCAK_SPONGE_WORDS]; /* Keccak's state */
    uint byteIndex;         /* 0..7--the next byte after the set one
                                 * (starts from 0; 0--none are buffered) */
    uint wordIndex;         /* 0..24--the next word to integrate input
                                 * (starts from 0) */
    uint capacityWords;     /* the double size of the hash output in
                                 * words (e.g. 16 for Keccak 512) */
};

const uint keccakf_rotc[24] = {
    1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62,
    18, 39, 61, 20, 44
};

const uint keccakf_piln[24] = {
    10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20,
    14, 22, 9, 6, 1
};

const uint64_t keccakf_rndc[24] = {
	0x0000000000000001UL, 0x0000000000008082UL, 0x800000000000808AUL, 0x8000000080008000UL,
	0x000000000000808BUL, 0x0000000080000001UL, 0x8000000080008081UL, 0x8000000000008009UL,
	0x000000000000008AUL, 0x0000000000000088UL, 0x0000000080008009UL, 0x000000008000000AUL,
	0x000000008000808BUL, 0x800000000000008BUL, 0x8000000000008089UL, 0x8000000000008003UL,
	0x8000000000008002UL, 0x8000000000000080UL, 0x000000000000800AUL, 0x800000008000000AUL,
	0x8000000080008081UL, 0x8000000000008080UL, 0x0000000080000001UL, 0x8000000080008008UL
};

/* generally called after SHA3_KECCAK_SPONGE_WORDS-ctx->capacityWords words
 * are XORed into the state s
 */
void keccakf(inout uint64_t s[SHA3_KECCAK_SPONGE_WORDS]) {
    uint i, j, round;
    uint64_t t, bc[5];

    for(round = 0; round < KECCAK_ROUNDS; round++) {

        /* Theta */
        for(i = 0; i < 5; i++)
            bc[i] = s[i] ^ s[i + 5] ^ s[i + 10] ^ s[i + 15] ^ s[i + 20];

        for(i = 0; i < 5; i++) {
            t = bc[(i + 4) % 5] ^ SHA3_ROTL64(bc[(i + 1) % 5], 1);
            for(j = 0; j < 25; j += 5)
                s[j + i] ^= t;
        }

        /* Rho Pi */
        t = s[1];
        for(i = 0; i < 24; i++) {
            j = keccakf_piln[i];
            bc[0] = s[j];
            s[j] = SHA3_ROTL64(t, keccakf_rotc[i]);
            t = bc[0];
        }

        /* Chi */
        for(j = 0; j < 25; j += 5) {
            for(i = 0; i < 5; i++)
                bc[i] = s[j + i];
            for(i = 0; i < 5; i++)
                s[j + i] ^= (~bc[(i + 1) % 5]) & bc[(i + 2) % 5];
        }

        /* Iota */
        s[0] ^= keccakf_rndc[round];
    }
}

void sha3_Init_256(inout sha3_context ctx) {
    uint bitSize = 256u;

    ctx.saved = 0ul;
    for (uint i = 0; i < SHA3_KECCAK_SPONGE_WORDS; i++) ctx.s[i] = 0ul;
    ctx.byteIndex = 0u;
    ctx.wordIndex = 0u;
    ctx.capacityWords = 2u * bitSize / (8u * 8u/*sizeof(uint64_t)*/);
}

void sha3_Update(inout sha3_context ctx, uint8_t byteIn) {
    ctx.saved |= ((uint64_t(byteIn) & uint64_t(0xFFu)) << ((ctx.byteIndex++) * 8u));
    if (ctx.byteIndex == 8u) {
        ctx.s[ctx.wordIndex] ^= ctx.saved;
        ctx.saved = 0u;
        ctx.byteIndex = 0u;
        if (++ctx.wordIndex == (SHA3_KECCAK_SPONGE_WORDS - ctx.capacityWords)) {
            ctx.wordIndex = 0u;
            keccakf(ctx.s);
        }
    }
}

uint8_t[SHA3_256_BYTES] sha3_Finalize(inout sha3_context ctx) {
    /* Append 2-bit suffix 01, per SHA-3 spec. Instead of 1 for padding we
     * use 1<<2 below. The 0x02 below corresponds to the suffix 01.
     * Overall, we feed 0, then 1, and finally 1 to start padding. Without
     * M || 01, we would simply use 1 to start padding. */

    uint64_t t;
#if SHA3_USE_KECCAK
    /* Keccak version */
    t = uint64_t((uint64_t(1)) << (ctx.byteIndex * 8u));
#else
    /* SHA3 version */
    t = uint64_t((uint64_t(0x02 | (1 << 2))) << ((ctx.byteIndex) * 8u));
#endif

    ctx.s[ctx.wordIndex] ^= ctx.saved ^ t;

    ctx.s[uint(SHA3_KECCAK_SPONGE_WORDS) - ctx.capacityWords - 1u] ^= 0x8000000000000000UL;
    keccakf(ctx.s);

    uint8_t result[SHA3_256_BYTES];
    for (uint i = 0; i < SHA3_256_BYTES / 8u; i++) {
        t = ctx.s[i];
        result[i * 8 + 0] = uint8_t((t) & uint64_t(0xFFu));
        result[i * 8 + 1] = uint8_t((t >> 8u) & uint64_t(0xFFu));
        result[i * 8 + 2] = uint8_t((t >> 16u) & uint64_t(0xFFu));
        result[i * 8 + 3] = uint8_t((t >> 24u) & uint64_t(0xFFu));
        result[i * 8 + 4] = uint8_t((t >> 32u) & uint64_t(0xFFu));
        result[i * 8 + 5] = uint8_t((t >> 40u) & uint64_t(0xFFu));
        result[i * 8 + 6] = uint8_t((t >> 48u) & uint64_t(0xFFu));
        result[i * 8 + 7] = uint8_t((t >> 56u) & uint64_t(0xFFu));
    }
    return result;
}

//
// Miner implementation
//

uniform uint u_firstNonce;
uniform uint u_blockHeader_length;
uniform uint u_blockHeader_nonceOffset;

uniform uint u_difficultyFunction;
const uint DIFFICULTY_FUNCTION_LEADING_ZEROES = 1;
const uint DIFFICULTY_FUNCTION_COMPACT_TARGET = 2;

uniform uint u_leadingZeroes_miningDifficulty;

uniform uint32_t u_compactTarget_expanded[SHA3_256_BYTES];

layout(std430, binding = 0) readonly restrict buffer BlockHeader {
    uint32_t bytes[];
} b_blockHeader;

/*layout(std430, binding = 1) writeonly restrict buffer HashOutput {
    uint32_t hashes[][SHA3_256_BYTES];
} b_hashOutput;*/

layout(std430, binding = 2) volatile restrict buffer Response {
    uint32_t status;        // initialized to 0
    uint32_t success_nonce; // initialized to u32::MAX
    uint32_t error_code;    // initialized to 0
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
    uint header_length = u_blockHeader_length;
    uint header_nonceOffset = u_blockHeader_nonceOffset;
    sha3_context ctx;
    sha3_Init_256(ctx);

    // hash the block header, inserting the nonce in the correct location
    for (uint i = 0; i < header_nonceOffset; i++) sha3_Update(ctx, uint8_t(b_blockHeader.bytes[i] & 0xFFu));
    for (uint i = 0; i < 4; i++) sha3_Update(ctx, uint8_t((nonce >> (i * 8u)) & 0xFFu));
    for (uint i = header_nonceOffset + 4u; i < header_length; i++) sha3_Update(ctx, uint8_t(b_blockHeader.bytes[i] & 0xFFu));

    uint8_t[SHA3_256_BYTES] hash = sha3_Finalize(ctx);

    //for (uint i = 0; i < SHA3_256_BYTES; i++) b_hashOutput.hashes[uint(nonce)][i] = uint32_t(hash[i]);

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
