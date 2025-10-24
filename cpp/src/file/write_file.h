#ifndef STORAGE_WRITE_FILE_H_
#define STORAGE_WRITE_FILE_H_

#include <string>

#include "utils/util_define.h"
#ifndef LIBTSFILE_SDK
#include "utils/storage_utils.h"
#endif

// Windows 兼容性处理
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#define mode_t int
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IROTH 0
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#endif

namespace storage {

class WriteFile {
public:
#ifndef LIBTSFILE_SDK
    WriteFile() : path_(), file_id_(), fd_(-1) {}
    FORCE_INLINE common::FileID get_file_id() const { return file_id_; }
    FORCE_INLINE int get_fd() const { return fd_; }
    int create(const common::FileID &file_id, int flags, mode_t mode);
#else
    WriteFile() : path_(), fd_(-1) {}
#endif
    int create(const std::string &file_name, int flags, mode_t mode);
    bool file_opened() const { return fd_ > 0; }
    int write(const char *buf, uint32_t len);
    int sync();
    int close();
    FORCE_INLINE std::string get_file_path() { return path_; }

private:
    int do_create(int flags, mode_t mode);

private:
    std::string path_;
#ifndef LIBTSFILE_SDK
    common::FileID file_id_;
#endif
    int fd_;
};

}  // end namespace storage

#endif  // STORAGE_WRITE_FILE_H_