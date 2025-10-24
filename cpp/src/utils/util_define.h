/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef UTILS_UTIL_DEFINE_H
#define UTILS_UTIL_DEFINE_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

/* ======== 跨平台兼容性定义 ======== */
#if defined(_WIN32)
#define PLATFORM_WINDOWS 1
#elif defined(__APPLE__)
#define PLATFORM_APPLE 1
#elif defined(__linux__)
#define PLATFORM_LINUX 1
#else
#define PLATFORM_UNKNOWN 1
#endif

/* ======== unused ======== */
#define UNUSED(v) ((void)(v))

/* ======== inline ======== */
#if defined(_MSC_VER)
#define FORCE_INLINE __forceinline
#elif defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE inline
#endif

#ifdef BUILD_FOR_SMALL_BINARY
#define INLINE FORCE_INLINE
#else
#define INLINE
#endif

/* ======== likely/unlikely ======== */
#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#elif defined(_MSC_VER)
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

/* ======== nullptr ======== */
#if __cplusplus < 201103L && !defined(_MSC_VER)
#ifndef nullptr
#define nullptr NULL
#endif
#define OVERRIDE
#else
#define OVERRIDE override
#endif

/* ======== cache line ======== */
#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

/* ======== assert ======== */
#ifdef NDEBUG
#define ASSERT(condition) ((void)0)
#else
#define ASSERT(condition) assert((condition))
#endif

/* ======== static assert ======== */
#if __cplusplus < 201103L && !defined(_MSC_VER)
#define STATIC_ASSERT(cond, msg) \
    typedef char static_assertion_##msg[(cond) ? 1 : -1] __attribute__((unused))
#else
#define STATIC_ASSERT(cond, msg) static_assert((cond), #msg)
#endif

/* ======== atomic operations ======== */
#if defined(__GNUC__) || defined(__clang__)
#define ATOMIC_FAA(val_addr, addv) \
    __atomic_fetch_add((val_addr), (addv), __ATOMIC_SEQ_CST)
#define ATOMIC_AAF(val_addr, addv) \
    __atomic_add_fetch((val_addr), (addv), __ATOMIC_SEQ_CST)
#define ATOMIC_CAS(val_addr, expected, desired)                            \
    __atomic_compare_exchange_n((val_addr), (expected), (desired),         \
                                /* weak = */ false,                        \
                                /* success_memorder = */ __ATOMIC_SEQ_CST, \
                                /* failure_memorder = */ __ATOMIC_SEQ_CST)
#define ATOMIC_LOAD(val_addr) __atomic_load_n((val_addr), __ATOMIC_SEQ_CST)
#define ATOMIC_STORE(val_addr, val) \
    __atomic_store_n((val_addr), (val), __ATOMIC_SEQ_CST)
#elif defined(_MSC_VER)
#include <intrin.h>
#define ATOMIC_FAA(val_addr, addv) _InterlockedExchangeAdd(val_addr, addv)
#ifdef _WIN64
#define ATOMIC_AAF(val_addr, addv) \
(_InterlockedExchangeAdd64(reinterpret_cast<volatile __int64*>(val_addr), static_cast<__int64>(addv)) + (addv))
#else
#define ATOMIC_AAF(val_addr, addv) \
(_InterlockedExchangeAdd(reinterpret_cast<volatile long*>(val_addr), static_cast<long>(addv)) + (addv))
#endif

#define ATOMIC_CAS(val_addr, expected, desired) \
    (_InterlockedCompareExchange(val_addr, desired, *expected) == *expected)
#define ATOMIC_LOAD(val_addr) (*val_addr)
#define ATOMIC_STORE(val_addr, val) (*val_addr = val)
#else
#error "Atomic operations not supported on this platform"
#endif

/* ======== alignment ======== */
#if defined(_MSC_VER)
#define ALIGNED(a) __declspec(align(a))
#elif defined(__GNUC__)
#define ALIGNED(a) __attribute__((aligned(a)))
#else
#define ALIGNED(a)
#endif

#define ALIGNED_4 ALIGNED(4)
#define ALIGNED_8 ALIGNED(8)

/* ======== disallow copy and assign ======== */
#if __cplusplus < 201103L && !defined(_MSC_VER)
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)
#else
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;    \
    TypeName& operator=(const TypeName&) = delete;
#endif

/* ======== return value check ======== */
#define RET_FAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RFAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RET_SUCC(expr) LIKELY(common::E_OK == (ret = (expr)))
#define RSUCC(expr) LIKELY(common::E_OK == (ret = (expr)))
#define IS_SUCC(ret) LIKELY(common::E_OK == (ret))
#define IS_FAIL(ret) UNLIKELY(common::E_OK != (ret))

#define IS_NULL(ptr) UNLIKELY((ptr) == nullptr)

/* ======== min/max ======== */
#define UTIL_MAX(a, b) ((a) > (b) ? (a) : (b))
#define UTIL_MIN(a, b) ((a) > (b) ? (b) : (a))

/*
 * int64_max < 10^20
 * consider +/- and the '\0' tail. 24 is enough
 */
#define INT64_TO_BASE10_MAX_LEN 24

#endif  // UTILS_UTIL_DEFINE_H