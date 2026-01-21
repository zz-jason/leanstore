#ifndef LEANSTORE_COMMON_PORTABLE_H
#define LEANSTORE_COMMON_PORTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

/// Portable packed attribute definition.
///
/// Packed structs are used to ensure that the structure's memory layout is
/// tightly packed without any padding bytes between the members.  Useful for
/// serialization, network protocols, or when interfacing with hardware where
/// the exact memory layout is crucial.
///
/// Drawback: may lead to performance issues on some architectures due to
/// misaligned accesses, USE WITH CAUTION.
#if defined(__GNUC__) || defined(__clang__)
#define PACKED __attribute__((packed))
#else
#define PACKED
#endif

/// Portable alignment attribute definition.
///
/// Alignment is used to ensure that a type is aligned in memory according to
/// the specified alignment value. This is important for performance and
/// correctness, especially when dealing with SIMD instructions or hardware
/// that requires specific alignment.
///
/// NOTE:
/// 1. The alignment value must be a power of two and greater than zero.
/// 2. Must not exceed the maximum alignment supported by the platform.
#if defined(__GNUC__) || defined(__clang__)
#define ALIGNAS(x) alignas(x)
#else
#define ALIGNAS(x)
#endif

#ifdef __cplusplus
}
#endif

#endif