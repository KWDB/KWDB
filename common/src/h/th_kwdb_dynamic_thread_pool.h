// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#ifndef COMMON_SRC_H_TH_KWDB_DYNAMIC_THREAD_POOL_H_
#define COMMON_SRC_H_TH_KWDB_DYNAMIC_THREAD_POOL_H_


#include <stdint.h>
#include <mutex>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <atomic>
#include <condition_variable>
#include <list>
#include "th_kwdb_operator_info.h"
#include "kwdb_type.h"
#include "cm_kwdb_context.h"
#include "tr_api.h"

namespace kwdbts {
#define TH_TRACE_1(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 1, __VA_ARGS__)
#define TH_TRACE_3(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 3, __VA_ARGS__)
#define TH_TRACE_4(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 4, __VA_ARGS__)

// These are the output interfaces without context.
#define LOG_TH_WARN(...) kwdbts::LOGGER.Log(kwdbts::LG_WARN, gettid(), __FILE__, __LINE__, __VA_ARGS__)
#define TRACE_TH_WARN(...) kwdbts::TRACER.Trace(nullptr, __FILE__, __LINE__, 3, __VA_ARGS__)

class KWDBDynamicThreadPool;

#ifdef WITH_TESTS
struct Telemetry{
    Telemetry():
    enter_start_thread_count(0),
    enter_routine_count(0),
    hook_user_routine_count(0) {
  }

  std::atomic<uint> enter_start_thread_count;
  std::atomic<uint> enter_routine_count;
  std::atomic<uint> hook_user_routine_count;
};
#endif

/**
 * @brief KWDBWrappedThread - The basic unit for storing thread and its info in KWDBThreadpool.
 */
struct KWDBWrappedThread {
  KWDBWrappedThread();
  // A context associated to this thread.
  kwdbContext_t m_context;
  // A function pointer, store functions that need to use threads from the thread pool.
  std::function<void(void *)> user_routine;
  // A void pointer, holds the arguments required by the function to be executed.
  void *arg;
  // The thread pool pointer to which the thread belongs
  KWDBDynamicThreadPool *pool;
  // The thread's KWDBOperatorInfo information
  KWDBOperatorInfo m_info;
  // The thread ID of the thread in the thread pool, counting from 100, is unique
  // for the lifetime of the thread
  KThreadID m_id;
  // A thread object created by c++ thread to execute the function passed in from the Applythread method.
  std::shared_ptr<std::thread> m_thread;
  // A lock that controls the execution of tasks by threads in a thread pool to avoid wasting thread resources
  std::mutex m_mutex;
  // Condition variables that control thread operations
  std::condition_variable m_cond_var;
  // An atomic variable identifier that checks whether the thread state is stop
  std::atomic_bool m_thread_stop;
  // An atomic variable identifier that checks whether the task performed by the thread is marked cancel
  std::atomic_bool m_is_cancel;
  // Thread State identification, including INIT,WAIT,RUNNING,CANCELLING,ERROR five states. After the KWDBWrappedThread
  // is created by its constructor, its state is set to INIT; startThread is used to create a thread
  // for KWDBWrappedThread. If the thread is successfully created, its state is set to WAIT. If the thread
  // fails to be created, it is set to ERROR. After the ApplyThread method is called to get the thread
  // and start executing the task, the state is set to RUNNING. If a loop task is cancelled during its execution,
  // the status is set to CANCELLING. When the task is completed, the state is set to WAIT again.
  enum ThreadStatus : kwdbts::k_int32 {
    INIT = 0,
    WAIT,
    RUNNING,
    CANCELLING,
    ERROR,
  };
  std::atomic<ThreadStatus> thread_status;
#ifdef WITH_TESTS
  Telemetry m_telemetry;
#endif
};

/**
 * @class KWDBDynamicThreadPool
 * @brief KWDBDynamicThreadPool - A mode of using thread resources using the idea of "pooling" of resources
 * for KWDB system, in order to reduce the resource consumption of thread creation, scheduling, and destruction.
 * @note ThreadPool creates a specified number of threads during the initialization phase and places the thread_id
 * of the specified number of initialized KWDB Threads in ALL_Threads (vector) into the WAIT_Threads (list). When other
 * modules need thread resources, they can obtain thread resources through the ThreadPool interface and support
 * concurrent requests. When WAIT_Threads is not empty, provide the requester with a KWDB Thread for each operation
 * and return a thread_id; When wait_Threads is empty and the number of threads in the Thread pool has not reached
 * the upper limit, try to initialize some KWDB Threads again and put the thread_id into wait_Threads (list) for higher-level invocation.
 */
class KWDBDynamicThreadPool {
 public:
   /**
   * @brief Thread start function.
   * @param arg The KWDBWrappedThread object that you need to perform the task.
    */
  friend void *Routine(void *arg);
  // The starting ThreadID value of the KWDBWrappedThread. ThreadID remains the same
  // throughout the lifetime of the KWDBWrappedThread.
  static const k_uint16 thread_id_offset_ = 100;
  // KWDBDynamicThreadPool Default maximum number of threads.
  static const k_uint16 default_max_thread_number_ = 128;
  // KWDBDynamicThreadPool Default minimum number of threads.
  static const k_uint16 default_min_thread_number_ = 8;
  // The default wait time for the JoinThread() function.
  static const k_uint16 default_check_time_ms_ = 1000;  // default 1s.
  static const k_uint64 main_thread_id_ = 99;
  // KWDBDynamicThreadPool State identification。After the thread pool is created
  // through its constructor, the state is set to UNINIT; After calling the Init method
  // to initialize the thread pool, set the state to RUNNING; After calling the STOP
  // method to safely end the thread pool, the thread pool is in the process of stopping,
  // and the state is set to stopping. The thread pool is finished and the status is set to stopped.
  enum PoolStatus : k_int32 {
      UNINIT = 0,
      RUNNING,
      STOPPING,
      STOPPED,
  };

  // Thread pool life cycle status
  static std::atomic<PoolStatus> pool_status_;
  /**
  * @brief KWDBDynamicThreadPool is a singleton pattern.
  * @param max_threads_nums The maximum number of threads in the thread pool
  * @return The first call to this method creates a thread pool of the specified size based on
  * the max_threads_NUMs passed in, and subsequent calls return the thread pool object created on the first call.
  * @note The first time this method is called, it builds a Thread pool object, specifies the maximum number of
  * threads in the Thread pool, creates a fixed number of KWDB threads in the all_Threads array, and initializes
  * KWDB Threads by subscript in the all_threads array. Calling this method later gets the thread pool object
  * created when the method was first called, which can directly use its thread pool method to get the thread.
  */
  #ifndef WITH_TESTS
    static KWDBDynamicThreadPool &
      GetThreadPool(k_uint16 max_threads_nums = default_max_thread_number_, k_uint16 min_threads_nums
      = default_min_thread_number_) {
    static KWDBDynamicThreadPool kwdb_thread_pool(max_threads_nums, min_threads_nums);

    if (pool_status_.load() == PoolStatus::RUNNING) {
      return kwdb_thread_pool;
    }

    static std::mutex mutex_;
    std::lock_guard<std::mutex> lock(mutex_);

    if (pool_status_.load() == PoolStatus::UNINIT) {
      kwdb_thread_pool.InitImplicitly();
    }

    return kwdb_thread_pool;
  }
  // It is limited to unit testing and supports thread pool destruction for unit testing scenarios.
  #else
    typedef std::shared_ptr<KWDBDynamicThreadPool> Ptr;
    ~KWDBDynamicThreadPool();
    static KWDBDynamicThreadPool & GetThreadPool(k_uint16 max_threads_nums = default_max_thread_number_,
                                                 k_uint16 min_threads_nums = default_min_thread_number_) {
      if (instance_ == nullptr) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (instance_ == nullptr) {
          instance_ = std::shared_ptr<KWDBDynamicThreadPool>(new KWDBDynamicThreadPool(max_threads_nums,
                                                                                       min_threads_nums));
        }
        if (instance_ != nullptr) {
          instance_->InitImplicitly();
        }
      }
      return *instance_;
    }

    static void Destory() {
      if (instance_) {
        instance_.reset();
      }
    }
  #endif

  /**
   * @brief KWDBDynamicThreadPool initializer method.
   * @param min_threads_num Initial number of threads in the thread pool.
   * @return A KStatus that determines whether the thread pool has been initialized successfully.
   * This method creates a number equal to min_threads_num in the thread pool by calling the startThread method.
   * @note min_threads_num needs to be provided and can only be initialized once.
   */
  // to be removed.
  KStatus Init(k_uint16 min_threads_num, kwdbContext_p ctx);
  KStatus InitImplicitly();

  /**
   * @brief KWDBDynamicThreadPool method which gets the number of threads that have been created in the thread pool.
   * @return The number of threads in the current thread pool, including
   * running and waiting threads, is the number of threads that have been created in the thread pool.
   */
  k_int32 GetThreadsNum() const { return threads_num_.load(); }

  /**
   * @brief KWDBDynamicThreadPool method which gets the thread pool lifecycle status.
   * @return The current state of the thread pool.
   */
  PoolStatus GetStatus() { return pool_status_; }

  /**
   * @brief KWDBDynamicThreadPool method which gets the Active number of threads that have been created in the thread pool.
   * @return The number of Active threads in the current thread pool.
   */
  k_double64 GetActiveThreadsNum() const { return threads_num_.load() - wait_threads_.size(); }

  /**
   * @brief KWDBDynamicThreadPool method which gets the Max number of threads in the thread pool.
   * @return The number of Max threads in the current thread pool.
   */
  k_double64 GetMaxThreadsNum() const { return max_threads_num_;}

  /**
    * @brief KWDBDynamicThreadPool method which gets the number of threads that waited in the thread pool.
    * @return The number of threads in the thread pool waiting for the task to execute.
    */
  k_int32 GetWaitThreadsNum() const { return wait_threads_.size(); }

  #ifdef WITH_TESTS
    /**
     * @brief KWDBDynamicThreadPool method which gets the KWDBWrappedThread thread id of all executing tasks
     * in the thread pool.
     * @return The KWDBWrappedThread thread id that holds the executing task in the thread pool.
     */
    std::vector<KWDBWrappedThread> *GetAllThreads() { return &all_threads_; }
  #endif

  /**
   * @brief KWDBDynamicThreadPool method which wait for the thread operation to finish.
   * @param thread_id The thread ID of the thread in the thread pool.
   * @param wait_microseconds Timeout duration of the JoinThread operation (1s by default)
   * @return A KStatus that check whether the join operation is successful
   * @note This method causes the main thread to block and wait for a specified period of time while the
   * thread completes its operation. If the waiting time exceeds the specified time, the main thread continues
   * to complete the subsequent work.
   */
  KStatus JoinThread(KThreadID thread_id, k_uint16 wait_microseconds = default_check_time_ms_);
  /**
   * @brief KWDBDynamicThreadPool method which cancels an operation being performed by the thread.
   * @param thread_id The thread ID of the thread in the thread pool.
   * @return A KStatus that check whether the Cancel operation is successful
   * @note This method is called outside the thread executing the task and requires a thread_id.
   * This does not stop the task being executed by the corresponding KWDB Thread. It only changes
   * the is_cancel identifier for the specified Thread to true. To cancel a task, the user must call IsCancel()
   * at an appropriate location in the function code.
   * It is recommended that the cancelThread method not be used together with the joinThread method to
   * avoid cancelling tasks that should have been done.
   */
  KStatus CancelThread(KThreadID thread_id);
  /**
   * @brief KWDBDynamicThreadPool method which Check the status of the thread executing the current task.
   * @return A k_bool that Check the status of the thread executing the current task
   * @note This method requires the user to call the method method from a suitable location in the function
   * code inside the thread. If true is returned, it indicates that the task has been set to Cancel or
   * the thread pool has been closed. In this case, the user who uses the thread needs to perform
   * necessary operations before ending the task execution. The IsCancel() method is required to be at
   * a safe interrupt checkpoint, where operations such as data drop in previous phases should have
   * been completed, and where the user closes the task without causing unexpected data loss.
   */
  k_bool IsCancel();
  /**
   * @brief KWDBDynamicThreadPool method which user gets the thread used to execute its task
   * @param job Functions that need to be executed using threads
   * @param arg Parameter required by job which stored in a void pointer
   * @param kwdb_operator_info Information about what this applyThread method does
   * @return A KStatus that check whether the Cancel operation is successful
   * @note This method allows users to apply for thread resources and provides information
   * about function Pointers, parameters, and operations. After calling this method, it first
   * checks whether the Thread Pool has been closed. If so, it stops the resource request. If
   * the Thread Pool status is normal, then check whether wait_threads is empty, that is, whether
   * KWDB Threads are available in the Thread Pool. If it exists, try to obtain a KWDBThread. If
   * the KWDBThread is successfully obtained, the corresponding KWDBThreadID will be returned to
   * the applicant. If no, check whether the number of threads in the current Thread Pool exceeds
   * the maximum value. If yes, application failure 0 is returned. If not, try to continue initializing
   * some KWDB Threads, putting the corresponding thread_id into wait_threads for the user to request and use.
   */
  KThreadID ApplyThread(std::function<void(void *)> &&job, void *arg,
                               KWDBOperatorInfo *kwdb_operator_info);
  /**
   * @brief KWDBDynamicThreadPool method which exit the thread pool safely
   * @return A KStatus that check whether Thread Pool is successful stop
   * @note This method is used to safely exit the Thread Pool by first marking the
   * Thread Pool state and then calling the KWDB Thread exit method in the all_threads
   * loop to safely close the Thread pool.
   */
  KStatus Stop();

  /**
   * @brief KWDBDynamicThreadPool method which checks the state of threadpool
   * @return A bool that determines the state of the thread pool
   */
  k_bool IsStop();

  /**
  * @brief KWDBDynamicThreadPool method which checks the state of threadpool
  * @return A bool that determines the running status of the thread pool
  */
  k_bool IsAvailable() { return pool_status_.load() == PoolStatus::RUNNING; }

  /**
   * @brief KWDBDynamicThreadPool method which exit the thread pool safely
   * @note This method needs to be called during function execution by first defining
   * the KWDBOperatorInfo object and passing in a pointer to it. This method stores the execution
   * information of the thread into the KWDBOperatorInfo object so that the user can check the information.
   */
  void GetOperatorInfo(KWDBOperatorInfo *kwdb_operator_info);
  /**
   * @brief KWDBDynamicThreadPool method which exit the thread pool safely
   * @note This method needs to be called during the execution of the function by
   * defining the KWDBOperatorInfo object, storing the information to be modified,
   * and passing its pointer to the method. This method changes the execution information
   * of the thread to the information stored by the KWDBOperatorInfo object, so that the user
   * can modify the thread information for different situations.
   */
  void SetOperatorInfo(KWDBOperatorInfo *kwdb_operator_info);
  kwdbContext_p GetOperatorContext();
  void SetOperatorContext(kwdbContext_p kwdb_operator_context);

#ifdef WITH_TESTS
  uint DebugGetEnterStartThreadCount();
  uint DebugGetEnterRoutineCount();
  uint DebugGetUserRoutineCount();
#endif

 private:
  explicit KWDBDynamicThreadPool(
    uint16_t max_threads_nums = default_max_thread_number_, uint16_t min_threads_nums = default_min_thread_number_);
  #ifndef WITH_TESTS
    KWDBDynamicThreadPool(KWDBDynamicThreadPool &&) = delete;
    KWDBDynamicThreadPool &operator=(KWDBDynamicThreadPool &&) = delete;
    /**
     * @brief KWDBDynamicThreadPool destructor method
     * @note The destructor method is called after the lifetime of the thread pool object.
     * It determines whether the thread pool state has been set to stop. If not, it proves
     * that the user has not called the stop() method to safely exit the thread pool
     */
    ~KWDBDynamicThreadPool();
  #else
    // Make its copy construction and assignment private, disallowing external copy and assignment.
    KWDBDynamicThreadPool(const KWDBDynamicThreadPool &signal);
    const KWDBDynamicThreadPool &operator=(const KWDBDynamicThreadPool &signal);

    static Ptr instance_;
    static std::mutex mutex_;
  #endif

  /**
   * @brief KWDBDynamicThreadPool method which check thread status
   * @note This function is called in the ApplyThread() function to check the state of the thread.
   * It traverses the threads created in the thread pool. If the pool of threads traversed is not
   * already endowed with a thread object, startThread is called to create a thread that can
   * be used to execute the task
   */
  k_bool checkThreadStatus(k_int32 thread_index);
  /**
   * @brief KWDBDynamicThreadPool method which create a new thread
   * @param kwdb_thread The KWDBWrappedThread object stored in all_threads_
   * @return A KStatus that Check whether KWDBWrappedThread is created successfully
   * @note When this method is called, it constructs a thread object using the Routine method through the
   * std::thread method and stores it in m_thread of kwdb_thread. If the thread is created successfully,
   * obtain the id of the thread in the thread pool and store the id in wait_threads_ for use. The id
   * of the thread itself is also obtained and stored in all_threads_id_ paired with the id of the
   * thread in the thread pool. If a thread fails to be created, set the thread's state to ERROR.
   */
  KStatus startThread(KWDBWrappedThread& kwdb_thread);  // NOLINT(runtime/references)
  /**
   * @brief KWDBDynamicThreadPool method which stop an old thread
   * @param kwdb_thread The KWDBWrappedThread object stored in all_threads_
   * @return A KStatus that check whether KWDBWrappedThread is stopped successfully
   * @note When this method is called, it first sets the thread m_thread_stop parameter to
   * true, then removes the id of the thread that needs to be stopped from wait_threads_ and wakes up
   * the waiting thread. Finally, the join() method of thread is called to ensure that the stored
   * thread in KWDBWrappedThread completes its existing task.
   */
  KStatus stopThread(KWDBWrappedThread& kwdb_thread);  // NOLINT(runtime/references)
  /**
   * @brief Checks the status of the current thread in the thread pool.
   * @return A k_bool that check whether the thread is in the cancel state
   * @note This method is called in IsCancel() and, when called, gets the hash of the
   * current thread ID. If this thread is not a thread in the thread pool, this method
   * returns false. This method then checks the state of the corresponding thread in the
   * KWDBThreadPool object based on the hash. if its status is stop or cancel, returns true.
   */
  k_bool isThreadCancelOrStop();
  /**
   * @brief Checks the status of the current thread in the thread pool.
   * @return A KStatus that clear the recorded information.
   */
  KStatus clear();

  // The number of initialization threads passed in by the init method
  k_uint16 min_threads_num_;
  // The maximum number of threads passed into the thread pool when creating the thread pool
  k_uint16 max_threads_num_;

  // Current number of threads in the thread pool
  std::atomic<k_uint16> threads_num_;

  std::list<k_uint16> wait_threads_;  // lock free list of waiting threads.
  std::list<k_uint16> error_threads_;  // lock free list of error threads.
  // The KWDBWrappedThread thread id that holds the executing task in the thread pool
  std::vector<KWDBWrappedThread> all_threads_;
  std::mutex error_mutex_;
  std::mutex wait_mutex_;
};

}  // namespace kwdbts

#endif  // COMMON_SRC_H_TH_KWDB_DYNAMIC_THREAD_POOL_H_
