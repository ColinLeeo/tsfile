/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <vector>

static int fail(const char* operation) {
    std::perror(operation);
    return 1;
}

static long long resident(void* address, size_t size, size_t page_size) {
    size_t pages = (size + page_size - 1) / page_size;
#ifdef __APPLE__
    std::vector<char> status(pages);
#else
    std::vector<unsigned char> status(pages);
#endif
    if (mincore(address, size, status.data()) != 0) return -1;
    long long count = 0;
    for (size_t i = 0; i < pages; ++i) count += (status[i] & 1) != 0;
    return count;
}

int main(int argc, char** argv) {
    if (argc != 3 || (std::strcmp(argv[1], "inspect") != 0 &&
                      std::strcmp(argv[1], "evict") != 0)) {
        std::fprintf(stderr, "usage: file_cache inspect|evict FILE\n");
        return 1;
    }
    bool evict = std::strcmp(argv[1], "evict") == 0;
    int fd = open(argv[2], O_RDONLY);
    if (fd < 0) return fail("open");
    struct stat info;
    if (fstat(fd, &info) != 0) return fail("fstat");
    if (!S_ISREG(info.st_mode) || info.st_size <= 0) return 1;
    size_t size = static_cast<size_t>(info.st_size);
    size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    void* address = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) return fail("mmap");
    long long before = resident(address, size, page_size);
    if (before < 0) return fail("mincore before");
    const char* method = "mincore_only";
    auto start = std::chrono::steady_clock::now();
    if (evict) {
#ifdef __APPLE__
        method = "msync_MS_SYNC_MS_INVALIDATE";
        if (msync(address, size, MS_SYNC | MS_INVALIDATE) != 0)
            return fail("msync invalidate");
#else
        method = "fsync_posix_fadvise_DONTNEED";
        if (fsync(fd) != 0) return fail("fsync");
        int status = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        if (status != 0) {
            std::fprintf(stderr, "posix_fadvise: %s\n", std::strerror(status));
            return 1;
        }
#endif
    }
    double elapsed = std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - start).count();
    // Drop our mapping and create a fresh, untouched one for the post-check.
    if (munmap(address, size) != 0) return fail("munmap");
    address = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) return fail("mmap after");
    long long after = resident(address, size, page_size);
    if (after < 0) return fail("mincore after");
    if (munmap(address, size) != 0) return fail("munmap after");
    if (close(fd) != 0) return fail("close");
    std::printf("{\"method\":\"%s\",\"file_bytes\":%zu,\"page_size\":%zu,"
                "\"total_pages\":%zu,\"resident_pages_before\":%lld,"
                "\"resident_pages_after\":%lld,\"eviction_s\":%.9f,"
                "\"eviction_requested\":%s,\"cold_verified\":%s}\n",
                method, size, page_size, (size + page_size - 1) / page_size,
                before, after, elapsed, evict ? "true" : "false",
                after == 0 ? "true" : "false");
    return evict && after != 0 ? 2 : 0;
}
