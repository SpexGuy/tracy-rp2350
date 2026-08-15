#ifndef __TRACYMUTEX_HPP__
#define __TRACYMUTEX_HPP__

#if defined _MSC_VER

#  include <shared_mutex>

namespace tracy
{
using TracyMutex = std::shared_mutex;
using TracyCondMutex = std::mutex;
using TracyConditionVariable = std::condition_variable;
}

#elif !defined TRACY_NO_THREADS

#include <mutex>

namespace tracy
{
using TracyMutex = std::mutex;
using TracyCondMutex = std::mutex;
using TracyConditionVariable = std::condition_variable;
}

#else

#include <mutex>

namespace tracy
{

// TODO RP2350 implement mutex?
class TracyMutex {
public:
    void lock() {}
    bool try_lock() { return true; }
    void unlock() {}
};

using TracyCondMutex = TracyMutex;

class TracyConditionVariable {
public:
    void notify_one() {}
    template< class Rep, class Period, class Predicate >
    bool wait_for( std::unique_lock<TracyCondMutex>& lock,
        const std::chrono::duration<Rep, Period>& rel_time,
        Predicate pred )
    {
        // TODO calls to this are an error
        return false;
    }
};

}

#endif

#endif
