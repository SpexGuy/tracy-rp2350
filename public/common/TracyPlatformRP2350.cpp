#ifndef _TRACY_PLATFORM_RP2350_INCLUDED
#define _TRACY_PLATFORM_RP2350_INCLUDED

#define TRACY_RP2350
#define TRACY_NO_THREADS
#define TRACY_NO_CODE_TRANSFER
#define TRACY_NO_BROADCAST
#define TRACY_NO_FRAME_IMAGE

// For some reason the RP2350 compiler sets this define,
// even though it doesn't have an OS
#ifdef __linux__
#undef __linux__
#endif

#define TRACY_HAS_CUSTOM_THREAD_ID
uint32_t PlatformGetThreadId() {
    // TODO return core number
    return 0;
}

#define TRACY_HAS_CUSTOM_USER_INFO
const char *PlatformGetUserLogin() {
    return "root";
}
const char *PlatformGetUserFullName() {
    return "root";
}
void PlatformGetHostname(char* buf, int len) {
    strncpy(buf, "rp2350", len);
}

#define TRACY_HAS_CUSTOM_ALLOCATOR
void PlatformAllocatorInit() {}
void PlatformAllocatorFinalize() {}
void PlatformAllocatorThreadInit() {}
void PlatformAllocatorThreadFinalize() {}
void *PlatformMalloc(size_t size) {
    return nullptr; // TODO
}
void PlatformFree(void *ptr) {
    // TODO
}
void *PlatformRealloc(void* ptr, size_t size) {
    // TODO
    return ptr;
}

#define TRACY_HAS_CUSTOM_SAFE_COPY
bool PlatformSafeMemcpy(void* buf, const void* data, size_t size) {
    // TODO
    return false;
}

#endif
