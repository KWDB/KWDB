// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#ifdef _WIN32

#include "platform.h"
#include <windows.h>
#include <io.h>
#include <errno.h>
#include <map>
#include <mutex>

// Global map to track file mappings and their alignment info
struct MappingInfo {
    HANDLE mapping_handle;   // CreateFileMapping handle
    void* view_base;         // Original MapViewOfFile returned pointer
    size_t view_size;        // Size passed to MapViewOfFile
    size_t alignment_offset; // Offset adjustment (for alignment)
};
static std::mutex g_mapping_mutex;
static std::map<void*, MappingInfo> g_file_mappings;

int fsync(int fd) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }
    if (!FlushFileBuffers(h)) {
        errno = EIO;
        return -1;
    }
    return 0;
}

int fallocate(int fd, int mode, off_t offset, off_t len) {
    // Windows doesn't have a direct equivalent to fallocate.
    // Extend the file to offset+len and ensure physical allocation.
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    // Get current file size
    LARGE_INTEGER current_size;
    if (!GetFileSizeEx(h, &current_size)) {
        errno = EIO;
        return -1;
    }

    off_t target_size = offset + len;
    if (target_size <= current_size.QuadPart) {
        return 0;  // File is already large enough
    }

    // Extend the file to the target size
    LARGE_INTEGER li;
    li.QuadPart = target_size;

    // Set file pointer to target position
    if (!SetFilePointerEx(h, li, NULL, FILE_BEGIN)) {
        errno = EIO;
        return -1;
    }

    // Set end of file to extend
    if (!SetEndOfFile(h)) {
        errno = EIO;
        return -1;
    }

    // On Windows, SetEndOfFile creates a sparse file extension.
    // For mmap to work reliably, we need to ensure the pages are committed.
    // SetFileValidData requires SE_MANAGE_VOLUME_NAME privilege, so we
    // can't use it. Instead, we rely on the mmap writing to commit pages.
    // The file size has been extended and mmap should succeed.

    return 0;
}

void* mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset) {
    if (fd == -1 && (flags & MAP_ANONYMOUS)) {
        DWORD flProtect = (prot & PROT_WRITE) ? PAGE_READWRITE : PAGE_READONLY;
        return VirtualAlloc(addr, length, MEM_COMMIT | MEM_RESERVE, flProtect);
    }

    DWORD protect = 0;
    DWORD access = 0;

    if (prot & PROT_WRITE) {
        protect = PAGE_READWRITE;
        access = FILE_MAP_WRITE;
    } else if (prot & PROT_READ) {
        protect = PAGE_READONLY;
        access = FILE_MAP_READ;
    }

    HANDLE h = INVALID_HANDLE_VALUE;
    HANDLE h_delete = INVALID_HANDLE_VALUE;  // handle with FILE_SHARE_DELETE
    if (fd != -1) {
        h = (HANDLE)_get_osfhandle(fd);
        if (h == INVALID_HANDLE_VALUE) {
            errno = EBADF;
            return MAP_FAILED;
        }
        // Re-open the file with FILE_SHARE_DELETE so the file can be
        // deleted while memory-mapped (matching Linux mmap behavior).
        char path_buf[MAX_PATH + 32];
        DWORD path_len = GetFinalPathNameByHandleA(h, path_buf, sizeof(path_buf), FILE_NAME_OPENED);
        if (path_len > 0 && path_len < sizeof(path_buf)) {
            // Strip \\?\ prefix if present for CreateFileA compatibility
            const char* fname = path_buf;
            if (strncmp(fname, "\\\\?\\", 4) == 0) {
                fname += 4;
            }
            DWORD desired_access = (prot & PROT_WRITE)
                ? (GENERIC_READ | GENERIC_WRITE)
                : GENERIC_READ;
            h_delete = CreateFileA(
                fname,
                desired_access,
                FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                NULL
            );
        }
        // Use the FILE_SHARE_DELETE handle if available, otherwise fall back
        if (h_delete != INVALID_HANDLE_VALUE) {
            h = h_delete;
        }
    }

    // On Windows, MapViewOfFile requires the file offset to be a multiple
    // of the system allocation granularity (typically 64KB), NOT the page size.
    DWORD alloc_gran = get_allocation_granularity();
    off_t aligned_offset = (offset / alloc_gran) * alloc_gran;
    size_t offset_align_diff = offset - aligned_offset;
    size_t mapping_size = length + offset_align_diff;

    // Get the actual file size to validate bounds
    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(h, &file_size)) {
        errno = EIO;
        if (h_delete != INVALID_HANDLE_VALUE) CloseHandle(h_delete);
        return MAP_FAILED;
    }
    // Cap mapping size to file size from aligned offset
    if ((off_t)(aligned_offset + mapping_size) > file_size.QuadPart) {
        mapping_size = (size_t)(file_size.QuadPart - aligned_offset);
    }

    // CreateFileMapping size: max size of the file mapping object
    // Use the full file size to allow mapping any part of the file
    HANDLE mapping = CreateFileMapping(
        h,
        NULL,
        protect,
        (DWORD)(file_size.QuadPart >> 32),
        (DWORD)file_size.QuadPart,
        NULL
    );

    // Close the FILE_SHARE_DELETE handle now that CreateFileMapping
    // has its own reference to the file.
    if (h_delete != INVALID_HANDLE_VALUE) {
        CloseHandle(h_delete);
        h_delete = INVALID_HANDLE_VALUE;
    }

    if (mapping == NULL) {
        DWORD err = GetLastError();
        if (err == ERROR_DISK_FULL || err == ERROR_HANDLE_DISK_FULL) {
            errno = ENOSPC;
        } else {
            errno = ENOMEM;
        }
        return MAP_FAILED;
    }

    // Map view of file at aligned offset
    void* view_base = MapViewOfFile(
        mapping,
        access,
        (DWORD)((ULONGLONG)aligned_offset >> 32),
        (DWORD)aligned_offset,
        mapping_size
    );

    if (view_base == NULL) {
        CloseHandle(mapping);
        DWORD err = GetLastError();
        if (err == ERROR_DISK_FULL || err == ERROR_HANDLE_DISK_FULL) {
            errno = ENOSPC;
        } else {
            errno = ENOMEM;
        }
        return MAP_FAILED;
    }

    // Return pointer adjusted by the alignment difference
    void* result = (char*)view_base + offset_align_diff;

    // Store mapping info for cleanup
    MappingInfo info;
    info.mapping_handle = mapping;
    info.view_base = view_base;
    info.view_size = mapping_size;
    info.alignment_offset = offset_align_diff;

    std::lock_guard<std::mutex> lock(g_mapping_mutex);
    g_file_mappings[result] = info;

    return result;
}

int munmap(void* addr, size_t length) {
    // Check file-backed mappings first to avoid VirtualFree
    // incorrectly handling a MapViewOfFile address.
    {
        std::lock_guard<std::mutex> lock(g_mapping_mutex);
        auto it = g_file_mappings.find(addr);
        if (it != g_file_mappings.end()) {
            MappingInfo& info = it->second;
            // Unmap using the original view base (aligned address)
            if (!UnmapViewOfFile(info.view_base)) {
                errno = EINVAL;
                return -1;
            }
            CloseHandle(info.mapping_handle);
            g_file_mappings.erase(it);
            return 0;
        }
    }

    // Anonymous allocations via VirtualAlloc
    if (VirtualFree(addr, 0, MEM_RELEASE)) {
        return 0;
    }

    // Not found in our map - try UnmapViewOfFile directly
    if (!UnmapViewOfFile(addr)) {
        errno = EINVAL;
        return -1;
    }

    return 0;
}

void* mremap(void* old_address, size_t old_size, size_t new_size, int flags) {
    // Windows doesn't support mremap directly.
    // Allocate a new anonymous mapping, copy data, then free the old one.
    void* new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (new_addr == MAP_FAILED) {
        return MAP_FAILED;
    }
    memcpy(new_addr, old_address, old_size < new_size ? old_size : new_size);
    munmap(old_address, old_size);
    return new_addr;
}

int msync(void* addr, size_t length, int flags) {
    if (!FlushViewOfFile(addr, length)) {
        errno = EIO;
        return -1;
    }
    return 0;
}

int flock(int fd, int operation) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    if (operation & LOCK_UN) {
        OVERLAPPED ov = {};
        ov.Offset = 0;
        ov.OffsetHigh = 0;
        if (!UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov)) {
            errno = EIO;
            return -1;
        }
        return 0;
    }

    DWORD flags = LOCKFILE_EXCLUSIVE_LOCK;
    if (operation & LOCK_NB) {
        flags |= LOCKFILE_FAIL_IMMEDIATELY;
    }

    OVERLAPPED ov = {};
    ov.Offset = 0;
    ov.OffsetHigh = 0;
    if (!LockFileEx(h, flags, 0, MAXDWORD, MAXDWORD, &ov)) {
        if (GetLastError() == ERROR_LOCK_VIOLATION) {
            errno = EWOULDBLOCK;
        } else {
            errno = EIO;
        }
        return -1;
    }
    return 0;
}

int symlink(const char* target, const char* linkpath) {
    // Try to create a symbolic link first (requires admin or developer mode on older Windows).
    // Fallback to a copy if symbolic link creation fails.
    DWORD attr = GetFileAttributesA(target);
    DWORD flags = (attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY))
                      ? SYMBOLIC_LINK_FLAG_DIRECTORY
                      : 0;
    if (CreateSymbolicLinkA(linkpath, target, flags)) {
        return 0;
    }
    if (!CopyFileA(target, linkpath, FALSE)) {
        errno = EIO;
        return -1;
    }
    return 0;
}

int mkstemp(char* template_name) {
    if (_mktemp_s(template_name, strlen(template_name) + 1) != 0) {
        return -1;
    }
    return _open(template_name, _O_RDWR | _O_CREAT | _O_EXCL | _O_BINARY, _S_IREAD | _S_IWRITE);
}

int kwdb_rename(const char* oldpath, const char* newpath) {
    // On Windows, rename() fails if the target file already exists (unlike POSIX
    // where it atomically replaces the target). Use MoveFileExA with
    // MOVEFILE_REPLACE_EXISTING to get POSIX-compatible behavior.
    if (MoveFileExA(oldpath, newpath, MOVEFILE_REPLACE_EXISTING)) {
        return 0;
    }
    // Map Windows error to errno
    DWORD err = GetLastError();
    if (err == ERROR_ACCESS_DENIED) {
        errno = EACCES;
    } else if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
        errno = ENOENT;
    } else if (err == ERROR_NOT_SAME_DEVICE) {
        errno = EXDEV;
    } else if (err == ERROR_DISK_FULL) {
        errno = ENOSPC;
    } else {
        errno = EIO;
    }
    return -1;
}

#endif // _WIN32
