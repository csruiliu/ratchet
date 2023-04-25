//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;
class Executor;

enum class TaskExecutionMode : uint8_t { PROCESS_ALL, PROCESS_PARTIAL };

enum class TaskExecutionResult : uint8_t { TASK_FINISHED, TASK_NOT_FINISHED, TASK_ERROR };

//! Generic parallel task
class Task {
public:
	virtual ~Task() {
	}

	//! Execute the task in the specified execution mode
	//! If mode is PROCESS_ALL, Execute should always finish processing and return TASK_FINISHED
	//! If mode is PROCESS_PARTIAL, Execute can return TASK_NOT_FINISHED, in which case Execute will be called again
	//! In case of an error, TASK_ERROR is returned
	virtual TaskExecutionResult Execute(TaskExecutionMode mode) = 0;
    virtual TaskExecutionResult ExecuteSuspend(TaskExecutionMode mode) = 0;
    virtual TaskExecutionResult ExecuteResume(TaskExecutionMode mode) = 0;
};

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor);
	ExecutorTask(ClientContext &context);
	virtual ~ExecutorTask();

	Executor &executor;

public:
	virtual TaskExecutionResult ExecuteTask(TaskExecutionMode mode) = 0;
    //! ExecutorTask inherits Task
    //! So ExecuteSuspend in [ExecutorTask] implements the ExecuteSuspend in [Task]
    //! ExecuteTaskSuspend is invoked in [ExecutorTask::ExecuteSuspend]
    //! ExecuteTaskSuspend is a pure virtual need to be implemented by various physical operators
    virtual TaskExecutionResult ExecuteTaskSuspend(TaskExecutionMode mode) = 0;
    virtual TaskExecutionResult ExecuteTaskResume(TaskExecutionMode mode) = 0;

	TaskExecutionResult Execute(TaskExecutionMode mode) override;
    TaskExecutionResult ExecuteSuspend(TaskExecutionMode mode) override;
    TaskExecutionResult ExecuteResume(TaskExecutionMode mode) override;
};

} // namespace duckdb
