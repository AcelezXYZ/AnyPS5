#include "SceTypes.hpp"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <thread>

extern "C" {
int APS5_VABI scePthreadCreate(Pthread* thread, const PthreadAttr* attr, PthreadEntry entry, void* arg, const char* name);
int APS5_VABI scePthreadJoin(Pthread thread, void** retval);
int APS5_VABI scePthreadDetach(Pthread thread);
void APS5_VABI scePthreadExit(void* retval);
Pthread APS5_VABI scePthreadSelf();
void APS5_VABI scePthreadTestcancel();
void APS5_VABI pthread_testcancel_nid_postfix(void);
int APS5_VABI scePthreadSetcancelstate(int state, int* old_state);
int APS5_VABI scePthreadSetcanceltype(int type, int* old_type);
int APS5_VABI scePthreadCancel(Pthread thread);
int APS5_VABI scePthreadMutexattrInit(PthreadMutexattr* attr);
int APS5_VABI scePthreadMutexattrDestroy(PthreadMutexattr* attr);
int APS5_VABI scePthreadMutexattrSettype(PthreadMutexattr* attr, int type);
int APS5_VABI scePthreadMutexInit(PthreadMutex* mutex, const PthreadMutexattr* attr, const char* name);
int APS5_VABI scePthreadMutexDestroy(PthreadMutex* mutex);
int APS5_VABI scePthreadMutexLock(PthreadMutex* mutex);
int APS5_VABI scePthreadMutexUnlock(PthreadMutex* mutex);
int APS5_VABI scePthreadAttrInit(PthreadAttr* attr);
int APS5_VABI scePthreadAttrDestroy(PthreadAttr* attr);
int APS5_VABI scePthreadAttrGet(Pthread thread, PthreadAttr* attr);
int APS5_VABI scePthreadAttrGetstackaddr(const PthreadAttr* attr, void** stack_addr);
int APS5_VABI scePthreadAttrGetstacksize(const PthreadAttr* attr, size_t* stack_size);
}

static bool StackContains(Pthread thread, const void* address) {
    PthreadAttr attr = nullptr;
    if (scePthreadAttrInit(&attr) != 0 || scePthreadAttrGet(thread, &attr) != 0) return false;
    void* stack = nullptr;
    size_t size = 0;
    const bool queried = scePthreadAttrGetstackaddr(&attr, &stack) == 0 && scePthreadAttrGetstacksize(&attr, &size) == 0;
    scePthreadAttrDestroy(&attr);
    const auto begin = reinterpret_cast<std::uintptr_t>(stack);
    const auto value = reinterpret_cast<std::uintptr_t>(address);
    return queried && stack != nullptr && size != 0 && value >= begin && value - begin < size;
}

static constexpr int SCE_OK = 0;
static constexpr int SCE_KERNEL_ERROR_EINVAL = 0x80020016;
static constexpr int SCE_KERNEL_ERROR_EPERM = 0x80020001;
static constexpr int MUTEX_TYPE_RECURSIVE = 2;
static constexpr int SCE_KERNEL_ERROR_ESRCH = 0x80020003;
static constexpr int CANCEL_STATE_ENABLE = 0;
static constexpr int CANCEL_STATE_DISABLE = 1;
static constexpr int CANCEL_TYPE_DEFERRED = 0;
static constexpr int CANCEL_TYPE_ASYNCHRONOUS = 2;
static constexpr std::intptr_t WorkerRetval = 0x1234;
static constexpr std::intptr_t CancelWorkerRetval = 0x55;
static void* const PthreadCanceled = reinterpret_cast<void*>(std::uintptr_t{1});

static void Require(bool value) { if (!value) std::abort(); }

struct CancelContext {
    Pthread thread = nullptr;
    std::atomic<bool> ready{false};
    std::atomic<bool> go{false};
    std::atomic<bool> beforeCancelPoint{false};
    std::atomic<bool> afterCancelPoint{false};
    int setupResult = -1;
    int oldValue = -1;
};

static void WaitFor(const std::atomic<bool>& flag) {
    while (!flag.load(std::memory_order_acquire)) std::this_thread::yield();
}

static void* APS5_VABI DeferredCancelWorker(void* arg) {
    auto& context = *static_cast<CancelContext*>(arg);
    context.ready.store(true, std::memory_order_release);
    WaitFor(context.go);
    context.beforeCancelPoint.store(true, std::memory_order_release);
    scePthreadTestcancel();
    context.afterCancelPoint.store(true, std::memory_order_release);
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* APS5_VABI DisabledCancelWorker(void* arg) {
    auto& context = *static_cast<CancelContext*>(arg);
    context.setupResult = scePthreadSetcancelstate(CANCEL_STATE_DISABLE, &context.oldValue);
    context.ready.store(true, std::memory_order_release);
    WaitFor(context.go);
    scePthreadTestcancel();
    context.beforeCancelPoint.store(true, std::memory_order_release);
    scePthreadSetcancelstate(CANCEL_STATE_ENABLE, nullptr);
    context.afterCancelPoint.store(true, std::memory_order_release);
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* APS5_VABI SelfAsyncCancelWorker(void* arg) {
    auto& context = *static_cast<CancelContext*>(arg);
    context.setupResult = scePthreadSetcanceltype(CANCEL_TYPE_ASYNCHRONOUS, &context.oldValue);
    context.beforeCancelPoint.store(true, std::memory_order_release);
    scePthreadCancel(scePthreadSelf());
    context.afterCancelPoint.store(true, std::memory_order_release);
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* APS5_VABI SelfDeferredCancelWorker(void* arg) {
    auto& context = *static_cast<CancelContext*>(arg);
    context.setupResult = scePthreadCancel(scePthreadSelf());
    context.beforeCancelPoint.store(true, std::memory_order_release);
    scePthreadTestcancel();
    context.afterCancelPoint.store(true, std::memory_order_release);
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* APS5_VABI AsyncTargetWorker(void* arg) {
    auto& context = *static_cast<CancelContext*>(arg);
    context.setupResult = scePthreadSetcanceltype(CANCEL_TYPE_ASYNCHRONOUS, nullptr);
    context.ready.store(true, std::memory_order_release);
    WaitFor(context.go);
    scePthreadSetcanceltype(CANCEL_TYPE_DEFERRED, nullptr);
    scePthreadTestcancel();
    context.afterCancelPoint.store(true, std::memory_order_release);
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* APS5_VABI FinishingWorker(void*) {
    return reinterpret_cast<void*>(CancelWorkerRetval);
}

static void* RunCancelWorker(PthreadEntry entry, CancelContext& context, bool cancelFromMain) {
    Require(scePthreadCreate(&context.thread, nullptr, entry, &context, nullptr) == SCE_OK);
    if (cancelFromMain) {
        WaitFor(context.ready);
        Require(scePthreadCancel(context.thread) == SCE_OK);
        context.go.store(true, std::memory_order_release);
    }
    void* result = nullptr;
    Require(scePthreadJoin(context.thread, &result) == SCE_OK);
    return result;
}

static void CheckCancellation() {
    {
        CancelContext context;
        Require(RunCancelWorker(DeferredCancelWorker, context, true) == PthreadCanceled);
        Require(context.beforeCancelPoint.load() && !context.afterCancelPoint.load());
    }
    {
        CancelContext context;
        Require(RunCancelWorker(DisabledCancelWorker, context, true) == PthreadCanceled);
        Require(context.setupResult == SCE_OK && context.oldValue == CANCEL_STATE_ENABLE);
        Require(context.beforeCancelPoint.load() && !context.afterCancelPoint.load());
    }
    {
        CancelContext context;
        Require(RunCancelWorker(SelfAsyncCancelWorker, context, false) == PthreadCanceled);
        Require(context.setupResult == SCE_OK && context.oldValue == CANCEL_TYPE_DEFERRED);
        Require(context.beforeCancelPoint.load() && !context.afterCancelPoint.load());
    }
    {
        CancelContext context;
        Require(RunCancelWorker(SelfDeferredCancelWorker, context, false) == PthreadCanceled);
        Require(context.setupResult == SCE_OK);
        Require(context.beforeCancelPoint.load() && !context.afterCancelPoint.load());
    }
    {
        CancelContext context;
        Require(scePthreadCreate(&context.thread, nullptr, AsyncTargetWorker, &context, nullptr) == SCE_OK);
        WaitFor(context.ready);
        bool threw = false;
        try { scePthreadCancel(context.thread); } catch (const std::runtime_error&) { threw = true; }
        Require(threw);
        context.go.store(true, std::memory_order_release);
        void* result = nullptr;
        Require(scePthreadJoin(context.thread, &result) == SCE_OK);
        Require(result == reinterpret_cast<void*>(CancelWorkerRetval));
        Require(context.setupResult == SCE_OK && context.afterCancelPoint.load());
    }
    {
        Pthread finishing = nullptr;
        Require(scePthreadCreate(&finishing, nullptr, FinishingWorker, nullptr, nullptr) == SCE_OK);
        while (scePthreadCancel(finishing) != SCE_KERNEL_ERROR_ESRCH) std::this_thread::yield();
        void* result = nullptr;
        Require(scePthreadJoin(finishing, &result) == SCE_OK);
        Require(result == reinterpret_cast<void*>(CancelWorkerRetval));
    }
    int unchanged = 7;
    Require(scePthreadSetcancelstate(2, &unchanged) == SCE_KERNEL_ERROR_EINVAL && unchanged == 7);
    Require(scePthreadSetcanceltype(1, &unchanged) == SCE_KERNEL_ERROR_EINVAL && unchanged == 7);
}

struct WorkerContext {
    Pthread thread = nullptr;
    Pthread selfFromWorker = nullptr;
    bool workerStackReported = false;
    PthreadMutex* mutex = nullptr;
    int unlockResult = 0;
    bool testcancelReturned = false;
};

static void* APS5_VABI Worker(void* arg) {
    auto& context = *static_cast<WorkerContext*>(arg);
    context.selfFromWorker = scePthreadSelf();
    int local = 0;
    context.workerStackReported = StackContains(context.selfFromWorker, &local);
    context.unlockResult = scePthreadMutexUnlock(context.mutex);
    int oldState = -1;
    int oldType = -1;
    context.testcancelReturned = scePthreadSetcancelstate(CANCEL_STATE_ENABLE, &oldState) == SCE_OK &&
        scePthreadSetcanceltype(CANCEL_TYPE_ASYNCHRONOUS, &oldType) == SCE_OK;
    scePthreadTestcancel();
    pthread_testcancel_nid_postfix();
    context.testcancelReturned = context.testcancelReturned && oldState == CANCEL_STATE_ENABLE;
    scePthreadExit(reinterpret_cast<void*>(WorkerRetval));
    return nullptr;
}

int main() {
    const Pthread mainSelf = scePthreadSelf();
    Require(mainSelf != nullptr);
    Require(scePthreadSelf() == mainSelf);
    int local = 0;
    Require(StackContains(mainSelf, &local));
    Require(scePthreadJoin(mainSelf, nullptr) == SCE_KERNEL_ERROR_EINVAL);
    Require(scePthreadDetach(mainSelf) == SCE_KERNEL_ERROR_EINVAL);

    PthreadMutexattr attr = nullptr;
    Require(scePthreadMutexattrInit(&attr) == SCE_OK);
    Require(scePthreadMutexattrSettype(&attr, MUTEX_TYPE_RECURSIVE) == SCE_OK);
    PthreadMutex mutex = nullptr;
    Require(scePthreadMutexInit(&mutex, &attr, nullptr) == SCE_OK);
    Require(scePthreadMutexattrDestroy(&attr) == SCE_OK);

    Require(scePthreadMutexLock(&mutex) == SCE_OK);

    WorkerContext context;
    context.mutex = &mutex;
    Require(scePthreadCreate(&context.thread, nullptr, Worker, &context, nullptr) == SCE_OK);
    Require(context.thread != nullptr);
    Require(context.thread != mainSelf);

    void* result = nullptr;
    Require(scePthreadJoin(context.thread, &result) == SCE_OK);
    Require(reinterpret_cast<std::intptr_t>(result) == WorkerRetval);
    Require(context.selfFromWorker != nullptr);
    Require(context.selfFromWorker == context.thread);
    Require(context.selfFromWorker != mainSelf);
    Require(context.unlockResult == SCE_KERNEL_ERROR_EPERM);
    Require(context.workerStackReported);
    Require(context.testcancelReturned);

    scePthreadTestcancel();
    pthread_testcancel_nid_postfix();
    CheckCancellation();

    Require(scePthreadMutexUnlock(&mutex) == SCE_OK);
    Require(scePthreadMutexDestroy(&mutex) == SCE_OK);
}
