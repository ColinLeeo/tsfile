#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]
# Shared-runtime profile for Python extension integration tests. The legacy
# ENABLE_ASAN profile remains available for standalone C++ executables.
set(TSFILE_SANITIZERS "" CACHE STRING "Python integration sanitizers: address or address,undefined (Linux GCC/Clang)")
set(_tsfile_sanitizer_json "[]")
set(_tsfile_legacy_asan "false")
if (ENABLE_ASAN)
    set(_tsfile_legacy_asan "true")
endif ()
if (NOT TSFILE_SANITIZERS STREQUAL "")
    if (ENABLE_ASAN)
        message(FATAL_ERROR "Use TSFILE_SANITIZERS or ENABLE_ASAN, not both")
    endif ()
    if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
        message(FATAL_ERROR "TSFILE_SANITIZERS currently requires Linux and GCC or Clang")
    endif ()
    if (NOT TSFILE_BUILD_SHARED)
        message(FATAL_ERROR "TSFILE_SANITIZERS requires TSFILE_BUILD_SHARED=ON")
    endif ()
    if (NOT TSFILE_SANITIZERS MATCHES "^address(,undefined)?$")
        message(FATAL_ERROR "TSFILE_SANITIZERS must be address or address,undefined")
    endif ()
    set(_tsfile_sanitizer_compile_options
            "-fsanitize=${TSFILE_SANITIZERS}" -O1 -g
            -fno-omit-frame-pointer -fno-optimize-sibling-calls
            -fno-sanitize-recover=all)
    set(_tsfile_sanitizer_json "[\"address\"]")
    if (TSFILE_SANITIZERS STREQUAL "address,undefined")
        set(_tsfile_sanitizer_json "[\"address\",\"undefined\"]")
    endif ()
endif ()

# Python setup.py checks this before copying/linking the C++ library, preventing
# accidental mixtures of sanitized libraries and ordinary extension builds.
file(WRITE "${PROJECT_BINARY_DIR}/sanitizers.json"
        "{\"legacy_asan\":${_tsfile_legacy_asan},\"sanitizers\":${_tsfile_sanitizer_json},\"compiler_id\":\"${CMAKE_CXX_COMPILER_ID}\"}\n")

function(tsfile_instrument_target target)
    if (TSFILE_SANITIZERS STREQUAL "")
        return()
    endif ()
    target_compile_options(${target} PRIVATE ${_tsfile_sanitizer_compile_options})
    get_target_property(_type ${target} TYPE)
    if (NOT _type STREQUAL "OBJECT_LIBRARY")
        # LINK_FLAGS supports the project's CMake 3.11 baseline.
        set_property(TARGET ${target} APPEND_STRING PROPERTY LINK_FLAGS
                " -fsanitize=${TSFILE_SANITIZERS}")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            set_property(TARGET ${target} APPEND_STRING PROPERTY LINK_FLAGS " -shared-libasan")
        endif ()
    endif ()
endfunction()
