--!strict

--[[
    Centralized Server & Distributed Queue System
    ---------------------------------------------
    Author: @meifeng_ft
    Date: 2024-07-04 (Last Updated)
    Version: 1.0.0

    Description:
    This module provides a robust solution for managing a single "main" server
    within a Roblox experience and multiple distributed queues for processing tasks.
    It leverages Roblox's MemoryStoreService for server election (locking) and
    queue management, and MessagingService for faster main server failover.

    Features:
    - **Main Server Election**: Automatically elects and maintains a single main server
      responsible for processing tasks from a primary, centralized queue.
    - **Centralized Queue**: Allows any server to queue values that are processed exclusively
      by the elected main server.
    - **Distributed Queues**: Supports multiple distributed queues where multiple servers can
      add items and a configurable number of servers can process them, enabling load-balanced tasks.
    - **Processor Candidacy Priority**: Allows prioritizing which servers become processors in a DistributedQueue.
    - **Fault Tolerance**: Implements retry mechanisms with exponential backoff for
      MemoryStore operations, enhancing resilience against transient network issues.
    - **Graceful Shutdown**: Ensures the main server or processor lock is released upon server shutdown,
      allowing another server to quickly take over.
    - **Dynamic Callbacks**: Provides a mechanism for registering and unregistering callback functions
      for processing items from any queue.

    Usage Example:
    --------------
	To be added.

    Important Notes:
    - Relies heavily on `MemoryStoreService`. Ensure quotas are sufficient for your use case.
    - `MessagingService` is used for rapid main server and processor recovery and is highly recommended.
    - The `instanceId` parameter in constructors creates independent centralized server/queue systems.
    - `INVISIBILITY_TIMEOUT` and retry logic assertions are critical for preventing
      duplicate processing of queue items.
]]

--// Types \\--

type proxy = typeof(newproxy(true))

-- A function that processes a value from the queue.
type Callback<T...> = (T...) -> ()
-- A list of callback functions.
type Callbacks<T...> = { Callback<T...> }

--// BaseQueue Types

-- Base properties shared by all queue types.
type BaseProperties = {
	-- A unique identifier for this queue instance.
	-- Useful for distinguishing between multiple queues of the same type.
	InstanceId: string,

	-- The name of the queue class
	ClassName: string,

	-- Number of items to read in a single API call to the queue backend.
	-- Controls batch size during each polling cycle.
	ReadCount: number,

	-- Maximum total number of items that can be read during one PollInterval.
	-- Prevents overloading the system in high-traffic scenarios.
	MaxReadPerInterval: number,

	-- The interval (in seconds) between each polling operation to fetch new queue items.
	-- Determines how frequently the queue is checked for new items.
	PollInterval: number,

	-- Whether batching is enabled for queue item processing.
	-- If true, the queue will wait for a batch of items before invoking callbacks.
	BatchEnabled: boolean,

	-- The maximum time (in seconds) to wait for enough items to form a batch before processing.
	-- Helps reduce the number of API calls during low throughput periods.
	BatchWaitTime: number,

	-- The lifespan (in seconds) of an item in the queue.
	-- Items exceeding this duration without being processed are discarded.
	ItemExpirationTime: number,

	-- Whether this server is eligible to become the main server.
	CanBeMainServer: boolean,

	-- Whether the queue should automatically manage its lifecycle (e.g., start processing threads).
	AutomateLifecycle: boolean,
}

-- A function that disconnects a callback or releases a lock.
type DisconnectFunction = () -> ()

-- The base interface for all queue objects.
type BaseQueue<self, T> = {
	-- Queues a value to be processed with an optional priority.
	-- For CentralizedQueues, only the main server will process it.
	-- For DistributedQueues, any active processor server can process it.
	-- @param value The value to add to the queue. Must be JSON-serializable.
	-- @param priority A number representing the item's priority. Higher numbers are processed first.
	-- @returns `true` if the item was successfully queued, `false` otherwise.
	AddToQueue: (self: self, value: T, priority: number?) -> (boolean, string?),

	-- Registers a callback to execute when an item from this queue is processed.
	-- @param callback The function to execute for each processed item.
	-- @returns A 'disconnect' function that, when called, removes the callback.
	AddCallback: (self: self, callback: (value: T) -> ()) -> DisconnectFunction,

	-- Retrieves the current number of tasks inside the queue.
	-- @returns The number of tasks currently present in the queue, or nil on failure.
	GetQueueSize: (self: self) -> number?,

	-- The core processing method for the server.
	-- Dequeues and processes items from the queue until it's empty
	-- or a processing limit for a single cycle is reached.
	-- This is intended to be called repeatedly by the server's main loop.
	-- @returns `true` if items were successfully processed, `false` otherwise.
	ProcessQueue: (self: self) -> boolean,

	-- Checks if this server is currently the main server for the queue.
	-- In a DistributedQueue, the main server is a special processor responsible for managing other processors.
	-- @returns `true` if the current server instance holds the main server lock for this queue, `false` otherwise.
	IsMainServer: (self: self) -> boolean,

	-- Retrieves the JobId of the server currently designated as the main server.
	-- @returns The JobId of the main server, or `nil` if no main server is elected.
	GetMainServerId: (self: self) -> string?,

	-- Voluntarily relinquishes the main server or processor role, allowing another server to take over.
	-- This should be called during graceful shutdown.
	-- @param immediateRelease If true, immediately removes the lock from MemoryStore. If false, the lock expires naturally.
	ReleaseLock: (self: self, immediateRelease: boolean?) -> (),

	-- Sets the maximum number of items to read from the queue in a single API call.
	-- @param readCount The number of items to read. Must be between 1 and 100.
	SetReadCount: (self: self, readCount: number) -> (),
	-- Sets the maximum total number of items to read from the queue within one PollInterval.
	-- @param maxReadPerInterval The maximum number of items. Use -1 for no limit.
	SetMaxReadPerInterval: (self: self, maxReadPerInterval: number) -> (),
	-- Sets the interval (in seconds) at which the queue is polled for new items.
	-- @param pollInterval The polling interval in seconds. Must be greater than 0.
	SetPollInterval: (self: self, pollInterval: number) -> (),

	-- Enables or disables batching for queue reads.
	-- @param shouldBatch True to enable batching, false to disable.
	SetBatchEnabled: (self: self, shouldBatch: boolean) -> (),
	-- Sets the maximum wait time (in seconds) for a batch of items to be ready before processing.
	-- @param waitTime The maximum wait time in seconds.
	SetBatchWaitTime: (self: self, waitTime: number) -> (),

	-- Sets the expiration time (in seconds) for items in the queue.
	-- Items will be automatically removed after this time if not processed.
	-- @param expiration The expiration time in seconds. Must be greater than 0.
	SetItemExpirationTime: (self: self, expiration: number) -> (),

	-- Sets whether this server is eligible to become the main server. If set to false on the current
	-- main server, it will release its lock.
	-- @param canBeMainServer True if this server can be the main server, false otherwise.
	SetCanBeMainServer: (self: self, canBeMainServer: boolean) -> (),

	-- Sets whether the queue should automatically manage its lifecycle.
	-- @param automateLifecycle True to enable automatic lifecycle management, false to disable.
	SetLifecycleAutomation: (self: self, automateLifecycle: boolean) -> (),

	-- Attempts to acquire or refresh the session lock for this server.
	-- For CentralizedQueues, this is the main server election process.
	-- For DistributedQueues, this determines if the server is a main or regular processor.
	-- @returns `true` if the session was successfully updated, `false` otherwise.
	UpdateSession: (self: self) -> boolean,

	-- Gracefully shuts down the queue on this server, releasing any locks held.
	Shutdown: (self: self) -> (),

	-- Initializes the queue, starting session management and processing threads if applicable.
	-- This must be called before any other methods.
	-- @returns `true` if the queue was successfully initialized, `false` if it was already initialized.
	Init: (self: self) -> boolean,
} & BaseProperties

--// DistributedQueue Types

-- Properties specific to a DistributedQueue.
type DistributedQueueProperties = {
	ClassName: "DistributedQueue",

	-- The maximum number of servers that can process the queue simultaneously.
	-- A value of -1 means there is no limit.
	MaxQueueProcessor: number,
	-- Determines if this server is eligible to become a processor for the queue.
	CanBeProcessor: boolean,

	-- The current server's priority for processor candidacy. Lower numbers have higher priority.
	ProcessorCandidacyPriority: number,
}

-- A queue that can be processed by multiple servers simultaneously.
export type DistributedQueue<T> = {

	-- Updates how processors are distributed among the available servers.
	-- This method should be called when the set of available processors changes
	-- (e.g., a server joins or leaves the cluster) or to periodically rebalance the load.
	-- @returns `true` if the processors are successfully distributed, `false` otherwise.
	UpdateProcessorDistribution: (self: DistributedQueue<T>) -> boolean,

	-- Checks if the current server is an active processor for the queue.
	-- An active processor is any server (main or regular) that processes items from the queue.
	-- @returns `true` if the current server is an active processor for the queue.
	IsActiveProcessor: (self: DistributedQueue<T>) -> boolean,

	-- Retrieves the current active processor count for the queue.
	-- @returns The number of active processors currently processing the queue, or nil on failure.
	GetActiveProcessorCount: (self: DistributedQueue<T>) -> number?,

	-- Retrieves a list of JobIds for the servers currently processing the queue.
	-- @param allOrNothing If true, returns all active processors only if all the requests succeed. Otherwise, returns a partial list on failure.
	-- @returns A list of JobIds for the active processors, or nil on failure if allOrNothing is true.
	GetActiveProcessorList: (self: DistributedQueue<T>, allOrNothing: boolean) -> { string }?,

	-- Sets the maximum number of servers that can process the queue simultaneously.
	-- This can only be set before Init() is called.
	-- @param concurrency The maximum number of processors. Use -1 for no limit.
	SetMaxQueueProcessor: (self: DistributedQueue<T>, concurrency: number) -> (),

	-- Sets whether this server is eligible to become a processor. If set to false on an active
	-- processor, it will release its lock.
	-- @param canBeProcessor True if this server can be a processor, false otherwise.
	SetCanBeProcessor: (self: DistributedQueue<T>, canBeProcessor: boolean) -> (),

	-- Sets the priority for the current server's processing role candidacy.
	-- This priority is used to determine which server should become a processor first when a slot is available. Higher numbers have higher priority.
	-- @param priority The candidacy priority.
	SetProcessorCandidacyPriority: (self: DistributedQueue<T>, priority: number) -> (),
} & BaseQueue<DistributedQueue<T>, T> & DistributedQueueProperties

--// CentralizedQueue Types

-- Properties specific to a CentralizedQueue.
type CentralizedQueueProperties = {
	ClassName: "CentralizedQueue",
}

-- A queue that is processed by a single, elected main server.
export type CentralizedQueue<T> = {} & BaseQueue<CentralizedQueue<T>, T> & CentralizedQueueProperties

--// Constructor Types

-- The public constructor interface for creating queues.
type Constructor = {
	-- Creates a centralized server instance where only one server processes the queue.
	-- An optional `instanceId` can be provided to manage multiple independent systems.
	-- @param instanceId A unique string to identify this queue system. Defaults to "global".
	-- @returns A CentralizedQueue instance.
	CreateCentralizedQueue: <T>(instanceId: string?) -> CentralizedQueue<T>,

	-- Creates a distributed queue instance that can be processed by multiple servers.
	-- @param instanceId A unique string to identify this queue system. Defaults to "global".
	-- @returns A DistributedQueue instance.
	CreateDistributedQueue: <T>(instanceId: string?) -> DistributedQueue<T>,

	-- Sets the unique identifier for the current server instance. Defaults to game.JobId.
	-- @param serverId A unique string to identify this server.
	SetServerId: (serverId: string) -> (),

	-- Sets the list of network errors that should be retried.
	-- This allows the retry logic to handle specific, known transient errors.
	-- @param errors An array where values are error message substrings.
	SetRetryableErrors: (errors: { string }) -> (),

	-- Suppresses logging for a specific severity level.
	-- @param logSeverity The severity level to suppress (1-5).
	SuppressLogLevel: (logSeverity: number) -> (),

	-- Adds a callback function that will be invoked for log messages.
	-- This allows external systems to capture and handle log messages.
	-- @param callback A function that takes a message string and a severity level (1-5).
	AddLogCallback: (callback: (message: string, severity: number) -> ()) -> DisconnectFunction,
}

--// Services \\--

local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")
local HttpService = game:GetService("HttpService")

--// Variables \\--

local NO_LIMIT = -1

-- Default to automatically managing the queue lifecycle.
local AUTOMATE_LIFECYCLE_BY_DEFAULT = true

-- The duration (in seconds) that items will persist in any queue before expiring (Default: 24 hours).
local DEFAULT_QUEUE_EXPIRATION = 86400
-- The delay (in seconds) for each queue processing thread to wait after it has cleared the queue.
local DEFAULT_QUEUE_POLL_INTERVAL = 1

-- The duration (in seconds) that an item read from a queue is hidden from other read attempts.
local INVISIBILITY_TIMEOUT = 30
-- An estimate of the maximum time it could take to remove an item from a queue after processing.
local AVERAGE_MAX_ITEM_REMOVAL_TIME = 0.5

-- The time (in seconds) the main server lock is held before it needs to be refreshed.
local SESSION_TIMEOUT = 30
-- The interval (in seconds) at which the main server refreshes its lock.
-- Must be less than SESSION_TIMEOUT.
local SESSION_REFRESH_INTERVAL = 12

-- Ensures the session refresh happens before the lock expires, preventing accidental loss of main server status.
assert(SESSION_REFRESH_INTERVAL < SESSION_TIMEOUT, "SESSION_REFRESH_INTERVAL must be less than SESSION_TIMEOUT")

-- The maximum number of times to retry a failing MemoryStore operation.
local MAX_ATTEMPTS = 5
-- The initial wait time (in seconds) before the first retry.
local RETRY_INTERVAL = 0.5
-- The multiplier for increasing the wait time between retries, creating an exponential backoff.
local EXPONENTIAL_BACKOFF = 2

-- This assertion verifies that the system's retry logic won't exceed the invisibility timeout,
-- which is critical for preventing an item from being read by two servers simultaneously.
assert(
	RETRY_INTERVAL * (EXPONENTIAL_BACKOFF ^ (MAX_ATTEMPTS - 1)) -- Maximum retry duration
			+ AVERAGE_MAX_ITEM_REMOVAL_TIME * MAX_ATTEMPTS
		< INVISIBILITY_TIMEOUT,
	"INVISIBILITY_TIMEOUT must be greater than the maximum retry duration plus the average item removal time."
)

-- The maximum number of items that can be read from a MemoryStoreSortedMap in a single operation.
local MAX_MEMORY_STORE_GET_RANGE_COUNT = 200
-- The maximum number of items that can be read from a MemoryStoreQueue in a single operation.
local MAX_MEMORY_STORE_READ_COUNT = 100
-- The number of items to retrieve from a queue in a single read operation.
local DEFAULT_READ_COUNT = MAX_MEMORY_STORE_READ_COUNT
-- The default priority for items added to a queue if not specified.
local DEFAULT_QUEUE_PRIORITY = 0
-- The default priority for processor candidacy in a DistributedQueue.
local DEFAULT_PROCESSOR_CANDIDACY_PRIORITY = DEFAULT_QUEUE_PRIORITY
-- The maximum number of items that can be read from a queue in a single interval.
-- This is set to NO_LIMIT by default, meaning no limit on the number of items read.
local DEFAULT_MAX_READ_PER_INTERVAL = NO_LIMIT
-- The default batch processing behavior for distributed queues.
local ENABLE_BATCH_BY_DEFAULT = false
-- The default wait time (in seconds) for batch processing in distributed queues.
local DEFAULT_BATCH_WAIT_TIME = 30

-- This assertion ensures the read count does not exceed the MemoryStoreQueue API limit.
assert(DEFAULT_READ_COUNT <= MAX_MEMORY_STORE_READ_COUNT, "DEFAULT_READ_COUNT must be less than or equal to MAX_MEMORY_STORE_READ_COUNT")

-- MessagingService topics for notifying servers of state changes.
local DISTRIBUTED_QUEUE_CHANGES_TOPIC = `DistributedQueue_ServerChanges`
local CENTRALIZED_QUEUE_CHANGES_TOPIC = `CentralizedQueue_ServerChanges`

local MEMORY_STORE_LOCK_FORMAT = `%s_LOCK_%s` -- Format for MemoryStore lock names, where %s is the queue type and %s is the instance ID.
local MEMORY_STORE_QUEUE_FORMAT = `%s_TASK_QUEUE_%s` -- Format for MemoryStore queue names, where %s is the queue type and %s is the instance ID.
local MEMORY_STORE_PROCESSOR_DISTRIBUTION_QUEUE_FORMAT = `PROCESSOR_DISTRIBUTION_QUEUE_%s` -- Format for processor distribution queues, where %s is the instance ID.

-- The maximum length for an instance ID in queue names.
-- This is used to ensure that memory store names do not exceed Roblox's limits (50 characters).
local MAX_INSTANCE_ID_LENGTH = 21

-- Default settings for server eligibility.
local DEFAULT_CAN_BE_MAIN_SERVER = true -- Default to allowing servers to become the main server.
local DEFAULT_MAX_QUEUE_PROCESSOR = NO_LIMIT -- Default to no limit on the number of servers processing the queue.
local DEFAULT_CAN_BE_PROCESSOR = true -- Default to allowing servers to process the queue.

--// Mapping & State

-- Configuration / Initialization
local INDEX_PROTOTYPE = 1 -- The prototype table for the queue object.
local INDEX_INITIALIZED = 2 -- A boolean flag indicating if Init() has been called.

-- MemoryStore components
local INDEX_MEMORY_STORE_LOCK = 3 -- The MemoryStore (HashMap or SortedMap) used for session locking.
local INDEX_MEMORY_STORE_QUEUE = 4 -- The MemoryStoreQueue used for items.

-- Server role & distribution
local INDEX_MAIN_SERVER_ID = 5 -- The JobId of the main server instance.
local INDEX_SERVER_ROLE = 6 -- The role of the current server (e.g., MAIN_SERVER, PROCESSOR_SERVER).

-- Callbacks and processing
local INDEX_CALLBACKS = 7 -- The list of registered callback functions.
local INDEX_UPDATE_THREAD = 8 -- The coroutine handling queue processing.
local INDEX_DISTRIBUTION_THREAD = 9 -- The coroutine handling processor distribution for DistributedQueues.
local INDEX_UPDATE_FLAG = 10 -- A flag indicating if the update thread is currently running.

-- MessagingService components
local INDEX_SUBSCRIPTION = 11 -- The MessagingService subscription for queue updates.

-- Represents the different roles a server can have for a given queue.
local NON_PROCESSOR_SERVER = 0 -- Server does not process the queue.
local PROCESSOR_SERVER = 1 -- Server is a regular processor for a DistributedQueue.
local MAIN_SERVER = 2 -- Server is the main processor for a DistributedQueue or CentralizedQueue.

-- Severity levels for logging messages.
local INFO_SEVERITY = 1 -- Informational messages.
local IMPORTANT_INFO_SEVERITY = 2 -- Important informational messages that may require attention.
local MINOR_ISSUE_SEVERITY = 3 -- Minor issues that does not include logical errors (network errors).
local WARNING_SEVERITY = 4 -- Warnings that may indicate potential issues.
local ERROR_SEVERITY = 5 -- Critical errors that require attention.

-- The unique identifier for the current server instance.
local ServerId: string = game.JobId
-- Maps a queue proxy object to its internal properties table.
local ProxyMap: { [proxy]: BaseProperties & DistributedQueueProperties } = {}
-- Maps a unique queue name (ClassName_InstanceId) to its proxy object to prevent duplicates.
local BaseQueueMap: { [string]: CentralizedQueue<any> | DistributedQueue<any> } = {}

-- The constructor object that will be returned by the module.
local Constructor: Constructor = {} :: Constructor

-- A list of all active queues that require periodic session updates.
local ActiveSessionQueues: { CentralizedQueue<any> | DistributedQueue<any> } = {}

-- A list of specific error message substrings that signify transient network issues, which are safe to retry.
-- It can be populated by the user to include any known retryable errors.
local RetryableNetworkErrors: { string } = {
	--// MemoryStoreService errors
	"TotalRequestsOverLimit",
	"InternalError",
	"Throttled",
	"PartitionRequestsOverLimit",
	"Timeout",

	--// MessagingService errors
	"Too many publish request",
}

local SuppressedLogLevels: { [number]: boolean } = {
	[INFO_SEVERITY] = false,
	[IMPORTANT_INFO_SEVERITY] = false,
	[MINOR_ISSUE_SEVERITY] = false,
	[WARNING_SEVERITY] = false,
	[ERROR_SEVERITY] = false,
}
local LogCallbacks: Callbacks<string, number> = {} -- Callbacks for log messages.

--// Private Functions \\--

-- A utility function to create an error output that can be used without stopping the thread.
local function createErrorOutput(msg: string)
	task.spawn(error, msg)
end

-- A utility function to log messages with optional severity and traceback.
local function logMessage(message: string, severity: number?, includeTraceback: boolean?)
	local messageSeverity = severity or INFO_SEVERITY

	if SuppressedLogLevels[messageSeverity] then return end -- Skip logging if this severity is suppressed.

	local output, outputFunc: (string) -> ()
	if messageSeverity == INFO_SEVERITY then
		output = `[Info] {message}`
		outputFunc = print
	elseif messageSeverity == IMPORTANT_INFO_SEVERITY then
		output = `[Important Info] {message}`
		outputFunc = warn
	elseif messageSeverity == MINOR_ISSUE_SEVERITY then
		output = `[Minor Issue] {message}`
		outputFunc = print
	elseif messageSeverity == WARNING_SEVERITY then
		output = `[Warning] {message}`
		outputFunc = warn
	elseif messageSeverity == ERROR_SEVERITY then
		output = `[Error] {message}`
		outputFunc = createErrorOutput
	else
		error("Invalid severity level. Must be 1 (Info), 2 (Important Info), 3 (Minor Issue), 4 (Warning), or 5 (Error).", 2)
	end

	if includeTraceback then output = output .. `\nTraceback: {debug.traceback("", 2)}` end

	outputFunc(output)
	-- Invoke all registered log callbacks with the message and severity.
	for _, callback in LogCallbacks do
		task.spawn(callback, message, messageSeverity)
	end
end

-- A utility to capture and yield the results of a function call from within a coroutine.
-- This allows `retryAsync` to get the return values from a `pcall`.
local function captureResult<T...>(boolean, ...: T...): T...
	coroutine.yield(boolean)
	coroutine.yield(...)
end

-- A robust wrapper for MemoryStore/MessagingService operations that automatically retries on failure.
-- It employs an exponential backoff strategy to avoid overwhelming services during outages.
-- @param func The function to execute and retry.
-- @param ... Arguments to pass to the function.
local function retryAsync<T..., R...>(func: (T...) -> R..., ...: T...): (boolean, R...)
	for retryAttempt = 1, MAX_ATTEMPTS - 1 do
		local thread = coroutine.create(captureResult)

		-- Execute the function in a protected call to catch errors.
		local _, success = coroutine.resume(thread, pcall(func, ...))
		if success then
			return coroutine.resume(thread) -- Return the successful result.
		end

		local _, errorMsg: any = coroutine.resume(thread)

		-- Retry only if there are defined retryable errors.
		if RetryableNetworkErrors and #RetryableNetworkErrors > 0 then
			local shouldRetry = false

			for _, errorCode in RetryableNetworkErrors do
				if errorMsg:find(errorCode) then
					shouldRetry = true
					break
				end
			end

			if not shouldRetry then
				logMessage(`[Retry] Non-retryable error for function '{func}': {errorMsg}`, ERROR_SEVERITY, true)
				return false, errorMsg -- Non-retryable error.
			end

			logMessage(
				`[Retry] Network operation failed for function '{func}', attempt ({retryAttempt}/{MAX_ATTEMPTS}): {errorMsg}`,
				MINOR_ISSUE_SEVERITY
			)
		else
			logMessage(
				`[Retry] No retryable errors defined. Function '{func}' failed with: {errorMsg}, attempt ({retryAttempt}/{MAX_ATTEMPTS})`,
				WARNING_SEVERITY,
				true
			)
		end

		-- Wait longer before the next attempt.
		task.wait(RETRY_INTERVAL * retryAttempt ^ EXPONENTIAL_BACKOFF)
	end

	-- If all attempts fail, return the result of the final attempt.
	return pcall(func, ...)
end

-- Validates that a value is non-nil and can be serialized to JSON for queueing.
local function checkValue(value: any, level: number?)
	local currentLevel = level or 1
	if value == nil then error("Value cannot be nil.", currentLevel + 1) end
	if not pcall(HttpService.JSONEncode, HttpService, value) then error("Value must be serializable to JSON.", currentLevel + 1) end
end

-- Checks if the module has been initialized before allowing public functions to be called.
local function checkInit(init: boolean, level: number?)
	local currentLevel = level or 1
	if not init then error("Queue is not initialized. Call .Init() first.", currentLevel + 1) end
end

-- Executes all registered callbacks for a list of processed items.
local function executeCallbacks<T>(callbacks: Callbacks<T>, items: { T })
	if #callbacks == 0 then return end

	-- Executes all registered callbacks for a given value.
	for _, item in items do
		for _, callback in callbacks do
			task.spawn(callback, item)
		end
	end
end

--//

-- Retrieves the internal properties table for a given queue proxy.
local function getQueueProperties(self: proxy): BaseProperties & DistributedQueueProperties
	local queueProperties = ProxyMap[self]
	if not queueProperties then error("BaseQueueMap is not initialized for this instance.", 3) end
	return queueProperties
end

local function processQueue(properties: BaseProperties): boolean
	local memoryStoreQueue = properties[INDEX_MEMORY_STORE_QUEUE] :: MemoryStoreQueue
	local callbacks = properties[INDEX_CALLBACKS]

	-- Read a batch of items from the queue.
	local readCount = properties.ReadCount
	local maxReadPerInterval = properties.MaxReadPerInterval
	local batchWaitTime = if properties.BatchEnabled then properties.BatchWaitTime else 0
	local itemsProcessedThisInterval = 0

	local processSuccess = false

	while maxReadPerInterval == NO_LIMIT or itemsProcessedThisInterval < maxReadPerInterval do
		local readAmount = if maxReadPerInterval == NO_LIMIT
			then readCount
			else math.min(readCount, maxReadPerInterval - itemsProcessedThisInterval)
		if readAmount <= 0 then break end

		local success, items: { any }?, id: string? =
			retryAsync(memoryStoreQueue.ReadAsync, memoryStoreQueue, readAmount, false, batchWaitTime)

		if success then
			if not (items and id) then
				logMessage(`[{properties.ClassName}][{properties.InstanceId}] No items read from queue. Queue may be empty.`, INFO_SEVERITY)
				processSuccess = true
				break
			end

			local nItems = #items
			if nItems == 0 then -- Queue is empty.
				processSuccess = true
				break
			end

			itemsProcessedThisInterval += nItems

			-- If read is successful, permanently remove the items to prevent reprocessing.
			local removeSuccess, removeError: string? = retryAsync(memoryStoreQueue.RemoveAsync, memoryStoreQueue, id)
			if not removeSuccess then
				logMessage(
					`[{properties.ClassName}][{properties.InstanceId}] Failed to remove items from queue after reading. Items may be processed again. Error: {removeError}`,
					MINOR_ISSUE_SEVERITY
				)
				break
			end

			executeCallbacks(callbacks, items)
			logMessage(`[{properties.ClassName}][{properties.InstanceId}] Processed {nItems} items from the queue.`, INFO_SEVERITY)

			-- If we read fewer items than requested, it indicates the queue is now empty.
			if nItems < readAmount then
				processSuccess = true
				break
			end
		else
			logMessage(`[{properties.ClassName}][{properties.InstanceId}] Failed to read from queue: {items}`, INFO_SEVERITY)
			-- Break on read failure to avoid a tight loop of failed reads.
			break
		end
	end

	return processSuccess
end

-- Creates and starts the main processing thread for a queue.
local function startUpdateThread(properties: BaseProperties)
	if properties[INDEX_UPDATE_THREAD] then return end -- Avoid starting multiple threads.
	if properties[INDEX_SERVER_ROLE] == NON_PROCESSOR_SERVER then
		logMessage(`[{properties.ClassName}] Attempted to start update thread, but this is not a processor server.`, WARNING_SEVERITY, true)
		return
	end
	if properties.AutomateLifecycle == false then
		logMessage(
			`[{properties.ClassName}][{properties.InstanceId}] Lifecycle automation is disabled. Not starting update thread.`,
			WARNING_SEVERITY,
			true
		)
		return
	end

	logMessage(
		`[{properties.ClassName}][{properties.InstanceId}] This server is now an active processor. Starting queue processing.`,
		INFO_SEVERITY
	)

	properties[INDEX_UPDATE_THREAD] = task.spawn(function()
		while true do
			processQueue(properties)
			task.wait(properties.PollInterval) -- Wait before processing the next batch.
		end
	end)
end

-- Stops the queue processing thread for a given queue.
local function stopUpdateThread(properties: BaseProperties)
	local updateThread = properties[INDEX_UPDATE_THREAD]
	if updateThread then
		task.cancel(updateThread)
		properties[INDEX_UPDATE_THREAD] = nil
		logMessage(`[{properties.ClassName}][{properties.InstanceId}] Stopping queue processing thread.`, INFO_SEVERITY)
	end
end

--// Classes \\--

-- Metatable for all queue objects, delegating key access to their internal properties table.
local ClassMetatable = {
	__index = function(self: proxy, key: any)
		local properties = ProxyMap[self]
		if properties == nil then error(`Attempted to access a destroyed object: {self} - {key}`, 2) end
		if type(key) ~= "string" then error("Index must be a string.", 2) end

		local value = properties[key]
		if value ~= nil then return value end

		local func = properties[INDEX_PROTOTYPE][key]
		if func then return func end

		error(`{key} is not a valid member of {properties.ClassName} {properties.InstanceId}`, 2)
	end,
	__newindex = function(self: proxy, key: any, value: any)
		local properties = getQueueProperties(self)
		error(`Cannot set {key} on {properties.ClassName} {properties.InstanceId}. Class is read-only.`, 2)
	end,
	__tostring = function(self: proxy)
		local properties = ProxyMap[self]
		if properties == nil then return `DestroyedQueue` end
		return `{properties.ClassName}_{properties and properties.InstanceId or "Unknown"}`
	end,
	__metatable = "Locked.",
}

-- The default template for creating the internal properties table of a new queue instance.
local BASE_QUEUE_TEMPLATE: { [any]: any } = {
	[INDEX_PROTOTYPE] = nil,
	[INDEX_INITIALIZED] = false,
	[INDEX_MEMORY_STORE_LOCK] = nil,
	[INDEX_MEMORY_STORE_QUEUE] = nil,
	[INDEX_MAIN_SERVER_ID] = nil,
	[INDEX_SERVER_ROLE] = nil,
	[INDEX_CALLBACKS] = nil,
	[INDEX_UPDATE_THREAD] = nil,
	[INDEX_DISTRIBUTION_THREAD] = nil,
	[INDEX_UPDATE_FLAG] = false,
	[INDEX_SUBSCRIPTION] = nil,

	InstanceId = "global",
	ClassName = "BaseQueue",
	ReadCount = DEFAULT_READ_COUNT,
	MaxReadPerInterval = DEFAULT_MAX_READ_PER_INTERVAL,
	PollInterval = DEFAULT_QUEUE_POLL_INTERVAL,
	BatchEnabled = ENABLE_BATCH_BY_DEFAULT,
	BatchWaitTime = DEFAULT_BATCH_WAIT_TIME,
	ItemExpirationTime = DEFAULT_QUEUE_EXPIRATION,
	CanBeMainServer = DEFAULT_CAN_BE_MAIN_SERVER,
	AutomateLifecycle = AUTOMATE_LIFECYCLE_BY_DEFAULT,
}

table.freeze(BASE_QUEUE_TEMPLATE) -- Prevent modification of the base template.
table.freeze(ClassMetatable)

--// BaseQueue Prototype \\--

local BaseQueuePrototype = {} :: BaseQueue<proxy, any>
local BaseQueueMetatable = {
	__index = BaseQueuePrototype,
	__metatable = "Locked.",
}

function BaseQueuePrototype:AddToQueue<T>(value: T, priority: number?): (boolean, string?)
	local properties = getQueueProperties(self)
	checkInit(properties[INDEX_INITIALIZED], 2)
	checkValue(value, 2)

	local queue = properties[INDEX_MEMORY_STORE_QUEUE] :: MemoryStoreQueue
	logMessage(`[{self.ClassName}][{self.InstanceId}] Adding item to queue.`, INFO_SEVERITY)

	local success, errorMsg: string? =
		retryAsync(queue.AddAsync, queue, value, properties.ItemExpirationTime, priority or DEFAULT_QUEUE_PRIORITY)
	if not success then
		logMessage(`[{self.ClassName}][{self.InstanceId}] Failed to add item to queue: {errorMsg}`, MINOR_ISSUE_SEVERITY)
		return false, errorMsg
	end

	logMessage(`[{self.ClassName}][{self.InstanceId}] Item added to queue successfully.`, INFO_SEVERITY)
	return true
end

function BaseQueuePrototype:AddCallback<T>(callback: Callback<T>): () -> ()
	local properties = getQueueProperties(self)
	local callbacks = properties[INDEX_CALLBACKS] :: Callbacks<T>

	logMessage(`[{self.ClassName}][{self.InstanceId}] Registering a new callback.`, INFO_SEVERITY)
	table.insert(callbacks, callback)

	local disconnected = false
	return function()
		if disconnected then
			logMessage(`[{self.ClassName}][{self.InstanceId}] Callback already disconnected.`, WARNING_SEVERITY, true)
			return
		end

		local index = table.find(callbacks, callback)
		if index then
			logMessage(`[{self.ClassName}][{self.InstanceId}] Disconnecting a callback.`, INFO_SEVERITY)
			table.remove(callbacks, index)
		end

		disconnected = true
	end
end

function BaseQueuePrototype:GetQueueSize(): number?
	local properties = getQueueProperties(self)
	checkInit(properties[INDEX_INITIALIZED], 2)

	local memoryStoreQueue = properties[INDEX_MEMORY_STORE_QUEUE] :: MemoryStoreQueue
	local success, size: number? = retryAsync(memoryStoreQueue.GetSizeAsync, memoryStoreQueue)
	if not success then
		logMessage(`[{self.ClassName}][{self.InstanceId}] Failed to get queue size.`, MINOR_ISSUE_SEVERITY)
		return nil
	end

	return size
end

function BaseQueuePrototype:ProcessQueue()
	local properties = getQueueProperties(self)
	checkInit(properties[INDEX_INITIALIZED], 2)

	if properties[INDEX_SERVER_ROLE] == NON_PROCESSOR_SERVER then
		logMessage(`[{self.ClassName}][{self.InstanceId}] This server is not a processor. Cannot process queue.`, WARNING_SEVERITY, true)
		return false
	end

	logMessage(`[{self.ClassName}][{self.InstanceId}] Processing queue...`, INFO_SEVERITY)
	return processQueue(properties)
end

function BaseQueuePrototype:IsMainServer()
	local properties = getQueueProperties(self)
	checkInit(properties[INDEX_INITIALIZED], 2)

	return properties[INDEX_SERVER_ROLE] == MAIN_SERVER
end

function BaseQueuePrototype:GetMainServerId(): string?
	local properties = getQueueProperties(self)
	checkInit(properties[INDEX_INITIALIZED], 2)
	return properties[INDEX_MAIN_SERVER_ID]
end

function BaseQueuePrototype:SetReadCount(readCount: number)
	local properties = getQueueProperties(self)
	if type(readCount) ~= "number" then error("Read count must be a number.", 2) end
	if readCount <= 0 then error("Read count must be greater than 0.", 2) end
	if readCount > MAX_MEMORY_STORE_READ_COUNT then
		logMessage(`Read count {readCount} exceeds MemoryStore limit of {MAX_MEMORY_STORE_READ_COUNT}. Clamping value.`, WARNING_SEVERITY)
		readCount = MAX_MEMORY_STORE_READ_COUNT
	end
	properties.ReadCount = readCount
end

function BaseQueuePrototype:SetMaxReadPerInterval(maxReadPerInterval: number)
	local properties = getQueueProperties(self)
	if type(maxReadPerInterval) ~= "number" then error("Max read per interval must be a number.", 2) end
	if maxReadPerInterval ~= NO_LIMIT and maxReadPerInterval < 1 then error("Invalid max read per interval.", 2) end
	properties.MaxReadPerInterval = maxReadPerInterval
end

function BaseQueuePrototype:SetPollInterval(pollInterval: number)
	local properties = getQueueProperties(self)
	if type(pollInterval) ~= "number" then error("Poll interval must be a number.", 2) end
	if pollInterval <= 0 then error("Poll interval must be greater than 0.", 2) end
	properties.PollInterval = pollInterval
end

function BaseQueuePrototype:SetBatchEnabled(shouldBatch: boolean)
	local properties = getQueueProperties(self)
	if type(shouldBatch) ~= "boolean" then error("Batch enabled must be a boolean.", 2) end
	properties.BatchEnabled = shouldBatch
end

function BaseQueuePrototype:SetBatchWaitTime(waitTime: number)
	local properties = getQueueProperties(self)
	if type(waitTime) ~= "number" then error("Batch wait time must be a number.", 2) end
	properties.BatchWaitTime = waitTime
end

function BaseQueuePrototype:SetItemExpirationTime(expiration: number)
	local properties = getQueueProperties(self)
	if type(expiration) ~= "number" then error("Item expiration time must be a number.", 2) end
	if expiration <= 0 then error("Item expiration time must be greater than 0.", 2) end
	properties.ItemExpirationTime = expiration
end

function BaseQueuePrototype:SetCanBeMainServer(canBeMainServer: boolean)
	local properties = getQueueProperties(self)
	if type(canBeMainServer) ~= "boolean" then error("Can be main server must be a boolean.", 2) end
	properties.CanBeMainServer = canBeMainServer

	if not canBeMainServer and (properties[INDEX_SERVER_ROLE] == MAIN_SERVER) then
		logMessage(`[{self.ClassName}][{self.InstanceId}] CanBeMainServer set to false. Releasing main server lock.`, INFO_SEVERITY)
		self:ReleaseLock(true)
	end
end

function BaseQueuePrototype:Shutdown()
	local properties = getQueueProperties(self)
	logMessage(`[{self.ClassName}][{self.InstanceId}] Shutting down...`, INFO_SEVERITY)

	local index = table.find(ActiveSessionQueues, self)
	if index then table.remove(ActiveSessionQueues, index) end

	if properties[INDEX_SUBSCRIPTION] then
		properties[INDEX_SUBSCRIPTION]:Disconnect()
		properties[INDEX_SUBSCRIPTION] = nil
	end

	if properties[INDEX_UPDATE_FLAG] then task.cancel(properties[INDEX_UPDATE_FLAG]) end

	local serverRole = properties[INDEX_SERVER_ROLE]
	if serverRole == MAIN_SERVER then
		logMessage(`[{self.ClassName}][{self.InstanceId}] This server is the main server. Releasing main server lock.`, INFO_SEVERITY)
		self:ReleaseLock(true)
	elseif serverRole == PROCESSOR_SERVER then
		logMessage(`[{self.ClassName}][{self.InstanceId}] This server is a processor. Releasing processor lock.`, INFO_SEVERITY)
		self:ReleaseLock(false)
	else
		logMessage(`[{self.ClassName}][{self.InstanceId}] This server is non-processor. No lock to release.`, INFO_SEVERITY)
	end

	local entryName = `{properties.ClassName}_{properties.InstanceId}`
	BaseQueueMap[entryName] = nil
	ProxyMap[self] = nil

	logMessage(`[{properties.ClassName}][{properties.InstanceId}] Shutdown complete.`, IMPORTANT_INFO_SEVERITY)

	table.clear(properties)
	table.freeze(properties)
end

table.freeze(BaseQueuePrototype)
table.freeze(BaseQueueMetatable)

--// Constructors \\--

-- Internal function to create a new base queue object, or return an existing one.
local function createBaseQueue<T>(
	instanceId: string?,
	className: string
): (BaseQueue<proxy, T>, (BaseProperties & DistributedQueueProperties)?)
	local id = instanceId or "global"
	if type(id) ~= "string" then error("InstanceId must be a string.", 3) end
	if #id > MAX_INSTANCE_ID_LENGTH then error(`InstanceId "{id}" exceeds maximum length of {MAX_INSTANCE_ID_LENGTH} characters.`, 3) end

	local entryName = `{className}_{id}`
	local existingQueue = BaseQueueMap[entryName]

	if existingQueue then
		logMessage(`{className} with instanceId "{id}" already exists. Using existing instance.`, WARNING_SEVERITY)
		return existingQueue
	end

	logMessage(`Creating new {className} with instanceId "{id}".`, INFO_SEVERITY)

	local proxy = newproxy(true)
	local properties = table.clone(BASE_QUEUE_TEMPLATE)

	if instanceId then properties.InstanceId = instanceId end
	properties.ClassName = className

	BaseQueueMap[entryName] = proxy
	ProxyMap[proxy] = properties

	local proxyMetatable = getmetatable(proxy)
	for key, value in ClassMetatable do
		proxyMetatable[key] = value
	end

	table.freeze(proxyMetatable) -- Prevent modification of the metatable.
	return proxy, properties
end

--// Subclasses \\--

do -- DistributedQueue
	--[[
		A DistributedQueue allows multiple servers to process items concurrently.
		Each server can add items to the queue, and a specific number of servers (processors)
		can process them.  The queue is managed using MemoryStoreService.  Servers can become
		processors based on a defined concurrency limit.

		A single "main" server is elected among the processors. Only this main server can
		approve new servers to become processors.  This prevents a race condition where multiple
		servers might try to become processors simultaneously, ensuring the total number of
		processors does not exceed the specified limit.
	]]

	local DistributedQueuePrototype = {} :: DistributedQueue<any>
	setmetatable(DistributedQueuePrototype, BaseQueueMetatable)

	-- Atomically sets a server as a processor in the MemoryStore lock.
	local function createProcessor(memoryStoreQueue: MemoryStoreSortedMap, serverId: string, instanceId: string): (boolean, boolean)
		local success, becameProcessor = retryAsync(memoryStoreQueue.UpdateAsync, memoryStoreQueue, serverId, function(value): boolean?
			if value then
				return nil
			else
				return true, os.time()
			end
		end, SESSION_TIMEOUT)

		if not success then
			logMessage(
				`[DistributedQueue][{instanceId}] Failed to create processor for server {serverId}: {becameProcessor}`,
				MINOR_ISSUE_SEVERITY
			)
			return false, false
		end

		logMessage(`[DistributedQueue][{instanceId}] Server {serverId} is now a processor.`, INFO_SEVERITY)
		return true, becameProcessor
	end

	local function getProcessorDistributionQueue(instanceId: string): MemoryStoreQueue
		local memoryStoreQueue =
			MemoryStoreService:GetQueue(MEMORY_STORE_PROCESSOR_DISTRIBUTION_QUEUE_FORMAT:format(instanceId), INVISIBILITY_TIMEOUT)
		return memoryStoreQueue
	end

	local function distributeProcessors(
		processorDistributionQueue: MemoryStoreQueue,
		instanceId: string,
		maxQueueProcessor: number,
		memoryStoreLock: MemoryStoreSortedMap
	)
		local getSizeSuccess, processorCount = retryAsync(memoryStoreLock.GetSizeAsync, memoryStoreLock)
		if not getSizeSuccess then
			logMessage(`[DistributedQueue][{instanceId}] Failed to get processor count: {processorCount}`, MINOR_ISSUE_SEVERITY)
			return false
		end

		logMessage(`[DistributedQueue][{instanceId}] Current processor count: {processorCount}`, INFO_SEVERITY)
		if processorCount >= maxQueueProcessor then
			logMessage(`[DistributedQueue][{instanceId}] Maximum processor limit reached ({maxQueueProcessor}).`, INFO_SEVERITY)
			return true -- No need to add more processors.
		end

		local leftoverProcessors = maxQueueProcessor - processorCount
		logMessage(`[DistributedQueue][{instanceId}] Attempting to add {leftoverProcessors} more processors.`, INFO_SEVERITY)

		local processedProcessor = 0
		local processingProcessor = 0

		while processedProcessor < leftoverProcessors do
			local readAmount = math.min(MAX_MEMORY_STORE_READ_COUNT, leftoverProcessors - processedProcessor)

			local success, items: { any }?, id: string? =
				retryAsync(processorDistributionQueue.ReadAsync, processorDistributionQueue, readAmount, false, 0)

			if not success then
				logMessage(
					`[DistributedQueue][{instanceId}] Failed to read from processor distribution queue: {items}`,
					MINOR_ISSUE_SEVERITY
				)
				return false -- Exit on read failure.
			end

			if not (items and id) then
				logMessage(`[DistributedQueue][{instanceId}] No items read from processor distribution queue.`, INFO_SEVERITY)
				return true -- Queue may be empty, no more processors to process.
			end

			local nItems = #items
			if nItems == 0 then return true end -- Queue is empty.

			processingProcessor += nItems

			-- If read is successful, permanently remove the items to prevent reprocessing.
			local removeSuccess, removeError: string? = retryAsync(processorDistributionQueue.RemoveAsync, processorDistributionQueue, id)
			if not removeSuccess then
				logMessage(
					`[DistributedQueue][{instanceId}] Failed to remove items from queue after reading. Items may be processed again. Error: {removeError}`,
					MINOR_ISSUE_SEVERITY
				)
				return false -- Exit on remove failure.
			end

			local mainThread = coroutine.running() -- Capture the main thread to resume later.

			local function processItem(serverId: string)
				local createProcessorSuccess, becameProcessor = createProcessor(memoryStoreLock, serverId, instanceId)
				processingProcessor -= 1

				if createProcessorSuccess then
					if becameProcessor then
						logMessage(`[DistributedQueue][{instanceId}] Successfully created processor for server {serverId}.`, INFO_SEVERITY)
						retryAsync(MessagingService.PublishAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, {
							Event = "CreateProcessor",
							ServerId = serverId,
							InstanceId = instanceId,
						})
						processedProcessor += 1 -- Increment the count of successfully processed servers.
					else
						logMessage(`[DistributedQueue][{instanceId}] Server {serverId} is already a processor.`, IMPORTANT_INFO_SEVERITY)
					end
				else
					logMessage(`[DistributedQueue][{instanceId}] Failed to create processor for server {serverId}.`, MINOR_ISSUE_SEVERITY)
				end

				if processingProcessor == 0 then
					logMessage(`[DistributedQueue][{instanceId}] All processors processed. Resuming main thread.`, INFO_SEVERITY)
					coroutine.resume(mainThread) -- Resume the main thread to continue processing.
				end
			end

			for _, serverId in items do
				task.spawn(processItem, serverId)
			end

			if processingProcessor > 0 then coroutine.yield() end

			-- If we read fewer items than requested, it indicates the queue is now empty.
			if nItems < readAmount then return true end
		end

		return true -- Successfully distributed processors.
	end

	local function createProcessorDistributionThread(instanceId: string, maxQueueProcessor: number, memoryStoreLock: MemoryStoreSortedMap)
		local processorDistributionQueue = getProcessorDistributionQueue(instanceId)

		while true do
			task.wait(SESSION_REFRESH_INTERVAL)
			distributeProcessors(processorDistributionQueue, instanceId, maxQueueProcessor, memoryStoreLock)
		end
	end

	local function startProcessorDistributionThread(properties: BaseProperties & DistributedQueueProperties)
		if properties[INDEX_DISTRIBUTION_THREAD] then return end -- Avoid starting multiple threads
		if properties.MaxQueueProcessor == NO_LIMIT then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] No limit on queue processors. Distribution thread not started.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		-- Only the main server can start the distribution thread.
		if properties[INDEX_SERVER_ROLE] ~= MAIN_SERVER then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] This server is not the main server. Cannot start distribution thread.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		logMessage(`[DistributedQueue][{properties.InstanceId}] Starting processor distribution thread.`, INFO_SEVERITY)
		properties[INDEX_DISTRIBUTION_THREAD] = task.spawn(
			createProcessorDistributionThread,
			properties.InstanceId,
			properties.MaxQueueProcessor,
			properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap
		)
	end

	local function stopProcessorDistributionThread(properties: BaseProperties & DistributedQueueProperties)
		local distributionThread = properties[INDEX_DISTRIBUTION_THREAD]
		if distributionThread then
			task.cancel(distributionThread)
			properties[INDEX_DISTRIBUTION_THREAD] = nil
			logMessage(`[DistributedQueue][{properties.InstanceId}] Stopping processor distribution thread.`, INFO_SEVERITY)
		end
	end

	local function becomeMainServer(properties: BaseProperties & DistributedQueueProperties): ()
		local previousRole = properties[INDEX_SERVER_ROLE]
		if previousRole == MAIN_SERVER then
			logMessage(`[DistributedQueue][{properties.InstanceId}] Already the main server.`, WARNING_SEVERITY, true)
			return
		end

		if properties.MaxQueueProcessor == NO_LIMIT then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] No limit on queue processors. Cannot become main server.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		if not properties.CanBeMainServer then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] Cannot become main server. CanBeMainServer is false.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		properties[INDEX_SERVER_ROLE] = MAIN_SERVER
		properties[INDEX_MAIN_SERVER_ID] = ServerId

		if not properties.AutomateLifecycle then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] Lifecycle automation is disabled. Not starting update thread.`,
				INFO_SEVERITY
			)
			return
		end

		if previousRole == NON_PROCESSOR_SERVER then startUpdateThread(properties) end
		startProcessorDistributionThread(properties)
	end

	local function becomeProcessorServer(properties: BaseProperties & DistributedQueueProperties): ()
		if properties[INDEX_SERVER_ROLE] ~= NON_PROCESSOR_SERVER then
			logMessage(`[DistributedQueue][{properties.InstanceId}] Already a processor server.`, WARNING_SEVERITY, true)
			return
		end

		if not properties.CanBeProcessor then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] Cannot become processor server. CanBeProcessor is false.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		properties[INDEX_SERVER_ROLE] = PROCESSOR_SERVER
		if properties.AutomateLifecycle then
			logMessage(`[DistributedQueue][{properties.InstanceId}] Lifecycle automation is enabled. Starting update thread.`, INFO_SEVERITY)
			startUpdateThread(properties)
		else
			logMessage(`[DistributedQueue][{properties.InstanceId}] Lifecycle automation is disabled. Not starting update thread.`, INFO_SEVERITY)
		end
	end

	function DistributedQueuePrototype:UpdateProcessorDistribution()
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		if properties[INDEX_SERVER_ROLE] ~= MAIN_SERVER then
			logMessage(
				`[{self.ClassName}][{properties.InstanceId}] This server is not the main server. Cannot update distribution.`,
				WARNING_SEVERITY,
				true
			)
			return false
		end

		return distributeProcessors(
			getProcessorDistributionQueue(properties.InstanceId),
			properties.InstanceId,
			properties.MaxQueueProcessor,
			properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap
		)
	end

	function DistributedQueuePrototype:IsActiveProcessor()
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		return (properties[INDEX_SERVER_ROLE] ~= NON_PROCESSOR_SERVER)
	end

	function DistributedQueuePrototype:GetActiveProcessorCount(): number?
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		logMessage(`[{self.ClassName}][{properties.InstanceId}] Getting active processor count...`, INFO_SEVERITY)

		local memoryStoreLock = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap
		local success, count: any = retryAsync(memoryStoreLock.GetSizeAsync, memoryStoreLock)
		if not success then
			logMessage(`[{self.ClassName}][{properties.InstanceId}] Failed to get active processor count: {count}`, MINOR_ISSUE_SEVERITY)
			return nil
		end

		return count
	end

	function DistributedQueuePrototype:GetActiveProcessorList(allOrNothing: boolean): { string }?
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		local memoryStoreLock = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap

		local items: { string } = {}
		local nItems = 0
		local exclusiveLowerBound = nil

		local readCount = MAX_MEMORY_STORE_GET_RANGE_COUNT
		readCount = readCount -- To prevent compiler from folding

		while true do
			local success, nextItems: any =
				retryAsync(memoryStoreLock.GetRangeAsync, memoryStoreLock, Enum.SortDirection.Ascending, readCount, exclusiveLowerBound)

			if not success then
				logMessage(
					`[{self.ClassName}][{properties.InstanceId}] Failed to get additional active processors: {nextItems}`,
					MINOR_ISSUE_SEVERITY
				)
				return if allOrNothing then nil else items
			end

			local nNextItems = #nextItems
			if nNextItems == 0 then break end -- No more items to process.
			table.move(nextItems, 1, nNextItems, nItems + 1, items)

			if nNextItems ~= readCount then break end -- No more items to process.
			exclusiveLowerBound = nextItems[nNextItems]
		end

		return items
	end

	function DistributedQueuePrototype:SetMaxQueueProcessor(concurrency: number)
		local properties = getQueueProperties(self)
		if properties[INDEX_INITIALIZED] then error("Cannot change MaxQueueProcessor after initialization.", 2) end

		if type(concurrency) ~= "number" then error("Concurrency must be a number.", 2) end
		if concurrency ~= NO_LIMIT and concurrency < 1 then error("Invalid Concurrency. Must be -1 or >= 1.", 2) end

		properties.MaxQueueProcessor = concurrency
	end

	function DistributedQueuePrototype:SetCanBeProcessor(canBeProcessor: boolean)
		local properties = getQueueProperties(self)
		if type(canBeProcessor) ~= "boolean" then error("Can be processor must be a boolean.", 2) end
		properties.CanBeProcessor = canBeProcessor
		if not canBeProcessor then
			properties.CanBeMainServer = false -- If a server cannot be a processor, it also cannot be the main server.
		end

		if not canBeProcessor and properties[INDEX_SERVER_ROLE] ~= NON_PROCESSOR_SERVER then
			logMessage(`[{self.ClassName}][{self.InstanceId}] CanBeProcessor set to false. Releasing processor lock.`, INFO_SEVERITY)
			self:ReleaseLock(true)
		end
	end

	function DistributedQueuePrototype:SetProcessorCandidacyPriority(priority: number)
		local properties = getQueueProperties(self)
		if type(priority) ~= "number" then error("Processor candidacy priority must be a number.", 2) end
		properties.ProcessorCandidacyPriority = priority
	end

	function DistributedQueuePrototype:ReleaseLock(immediateRelease: boolean?)
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		local currentServerType = properties[INDEX_SERVER_ROLE]

		if currentServerType == NON_PROCESSOR_SERVER then
			logMessage("[DistributedQueue] This server is not an active processor. No lock to release.", WARNING_SEVERITY, true)
			return
		end

		stopUpdateThread(properties)
		properties[INDEX_SERVER_ROLE] = NON_PROCESSOR_SERVER

		if currentServerType == MAIN_SERVER then
			stopProcessorDistributionThread(properties)
			logMessage(`[{self.ClassName}][{self.InstanceId}] Releasing main server lock...`, INFO_SEVERITY)
		else
			logMessage(`[{self.ClassName}][{self.InstanceId}] Releasing processor lock...`, INFO_SEVERITY)
		end

		if immediateRelease == false then
			logMessage(
				`[{self.ClassName}][{self.InstanceId}] Lock ownership lost locally. Waiting for session to expire in MemoryStore.`,
				INFO_SEVERITY
			)
			return
		end

		if properties.MaxQueueProcessor == NO_LIMIT then
			logMessage("[DistributedQueue] No limit on queue processors. Lock release is not applicable.", INFO_SEVERITY)
			return
		end

		-- Immediately remove the lock from MemoryStore instead of waiting for it to expire.
		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreHashMap
		local instanceId = properties.InstanceId

		if currentServerType == MAIN_SERVER then
			logMessage(`[{self.ClassName}][{instanceId}] Releasing main server lock...`, INFO_SEVERITY)
			-- Remove the main server lock from MemoryStore.
			local success = retryAsync(memoryStore.RemoveAsync, memoryStore, "MainServer")
			if success then
				logMessage(`[{self.ClassName}][{instanceId}] Main server lock released.`, INFO_SEVERITY)

				-- Notify other servers via MessagingService that they can now attempt to claim the main server role.
				logMessage(`[{self.ClassName}][{instanceId}] Notifying other servers of main server release.`, INFO_SEVERITY)
				retryAsync(MessagingService.PublishAsync, MessagingService, CENTRALIZED_QUEUE_CHANGES_TOPIC, {
					Event = "MainServerReleased",
					InstanceId = instanceId,
					PreviousServer = ServerId,
				})
			else
				logMessage(
					`[{self.ClassName}][{instanceId}] Failed to remove main server lock from MemoryStore during release.`,
					MINOR_ISSUE_SEVERITY
				)
			end
		else -- currentServerType == PROCESSOR_SERVER
			logMessage(`[{self.ClassName}][{instanceId}] Releasing processor lock...`, INFO_SEVERITY)
			-- Remove the processor lock from MemoryStore.
			local success = retryAsync(memoryStore.RemoveAsync, memoryStore, ServerId)
			if success then
				logMessage(`[{self.ClassName}][{instanceId}] Processor lock released.`, INFO_SEVERITY)

				-- Notify other servers via MessagingService that this server has released its processor role.
				logMessage(`[{self.ClassName}][{instanceId}] Notifying other servers of processor slot availability.`, INFO_SEVERITY)
				retryAsync(MessagingService.PublishAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, {
					Event = "ProcessorReleased",
					ServerId = ServerId,
					InstanceId = instanceId,
				})
			else
				logMessage(
					`[{self.ClassName}][{instanceId}] Failed to remove processor lock from MemoryStore during release.`,
					MINOR_ISSUE_SEVERITY
				)
			end
		end
	end

	function DistributedQueuePrototype:SetLifecycleAutomation(automateLifecycle: boolean)
		local properties = getQueueProperties(self)
		if type(automateLifecycle) ~= "boolean" then error("Automate lifecycle must be a boolean.", 2) end

		local previousLifecycleAutomation = properties.AutomateLifecycle
		if automateLifecycle == previousLifecycleAutomation then
			logMessage(
				`[DistributedQueue][{properties.InstanceId}] Lifecycle automation already set to {automateLifecycle}.`,
				INFO_SEVERITY
			)
			return
		end

		properties.AutomateLifecycle = automateLifecycle

		if automateLifecycle then
			logMessage(`[DistributedQueue][{properties.InstanceId}] Lifecycle automation enabled.`, INFO_SEVERITY)
			if properties[INDEX_SERVER_ROLE] == PROCESSOR_SERVER then
				logMessage(`[DistributedQueue][{properties.InstanceId}] This server is a processor. Starting update thread.`, INFO_SEVERITY)
				startUpdateThread(properties)
			end
			if properties[INDEX_SERVER_ROLE] == MAIN_SERVER then
				logMessage(
					`[DistributedQueue][{properties.InstanceId}] This server is the main server. Starting distribution thread.`,
					INFO_SEVERITY
				)
				startProcessorDistributionThread(properties)
			end
			local index = table.find(ActiveSessionQueues, self)
			if not index then
				logMessage(`[DistributedQueue][{properties.InstanceId}] Adding queue to active session queues.`, INFO_SEVERITY)
				table.insert(ActiveSessionQueues, self)
			end
		else
			logMessage(`[DistributedQueue][{properties.InstanceId}] Lifecycle automation disabled.`, INFO_SEVERITY)
			if properties[INDEX_SERVER_ROLE] == PROCESSOR_SERVER then
				logMessage(`[DistributedQueue][{properties.InstanceId}] This server is a processor. Stopping update thread.`, INFO_SEVERITY)
				stopUpdateThread(properties)
			end
			if properties[INDEX_SERVER_ROLE] == MAIN_SERVER then
				logMessage(
					`[DistributedQueue][{properties.InstanceId}] This server is the main server. Stopping distribution thread.`,
					INFO_SEVERITY
				)
				stopProcessorDistributionThread(properties)
			end
			local index = table.find(ActiveSessionQueues, self)
			if index then
				logMessage(`[DistributedQueue][{properties.InstanceId}] Removing queue from active session queues.`, INFO_SEVERITY)
				table.remove(ActiveSessionQueues, index)
			end
		end
	end

	function DistributedQueuePrototype:UpdateSession(): boolean
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		if properties[INDEX_UPDATE_FLAG] then
			logMessage(`[DistributedQueue][{properties.InstanceId}] Session is being updated.`, INFO_SEVERITY)
			return true -- Session is being updated
		end

		logMessage(`[DistributedQueue][{properties.InstanceId}] Updating session...`, INFO_SEVERITY)
		properties[INDEX_UPDATE_FLAG] = coroutine.running() -- Set the flag to prevent re-entrancy.

		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap
		local serverLevel = properties[INDEX_SERVER_ROLE]
		local instanceId = properties.InstanceId

		local maxQueueProcessor = properties.MaxQueueProcessor
		if maxQueueProcessor == NO_LIMIT then
			-- No limit on the number of servers processing this queue. This server will always process.
			logMessage(
				`[DistributedQueue][{instanceId}] No limit on queue processors. This server will always process.`,
				WARNING_SEVERITY,
				true
			)
			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return true
		end

		local releaseLock = false

		if serverLevel == MAIN_SERVER then
			if not properties.CanBeMainServer then
				logMessage(`[DistributedQueue][{instanceId}] This server cannot be the main server. Releasing lock.`, INFO_SEVERITY)
				self:ReleaseLock(true) -- Relinquish role if it cannot be main server.
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			-- This server is the main server. Attempt to refresh the lock.
			local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
				if not properties.CanBeMainServer then
					releaseLock = true
					return nil -- Abort update, this server cannot be main.
				end

				if not value or value == ServerId then return ServerId end
				return nil -- Abort update, another server took the lock.
			end, SESSION_TIMEOUT)

			if releaseLock then
				logMessage(`[DistributedQueue][{instanceId}] This server cannot be the main server. Releasing lock.`, INFO_SEVERITY)
				self:ReleaseLock(true) -- Relinquish role if it cannot be main server.
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			-- Refresh successful.
			if success and updateResult then
				logMessage(`[DistributedQueue][{instanceId}] Successfully refreshed main server lock.`, INFO_SEVERITY)

				if not properties.CanBeMainServer then
					logMessage(`[DistributedQueue][{instanceId}] This server is not eligible to be the main server.`, INFO_SEVERITY)
					self:ReleaseLock(true) -- If this server cannot be main, release the lock.
				end

				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			logMessage(`[DistributedQueue][{instanceId}] Failed to refresh main server lock. Relinquishing role.`, IMPORTANT_INFO_SEVERITY)
			-- If the atomic update fails, this server must assume it has lost the lock.
			self:ReleaseLock(false)
			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return false
		end

		if properties.CanBeMainServer then
			-- Check on the main server, is it still active?
			local success, mainServerId = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
				if not properties.CanBeMainServer then
					logMessage(`[DistributedQueue][{instanceId}] This server cannot be the main server.`, INFO_SEVERITY)
					return nil -- Abort update, this server cannot be main.
				end

				if not value or value == ServerId then return ServerId end
				return nil -- Abort update, another server held the lock.
			end, SESSION_TIMEOUT)

			if not success then
				logMessage(`[DistributedQueue][{instanceId}] Failed to check main server status.`, MINOR_ISSUE_SEVERITY)
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return false
			end

			if mainServerId then
				logMessage(`[DistributedQueue][{instanceId}] Successfully became the main processor.`, IMPORTANT_INFO_SEVERITY)
				if not properties.CanBeMainServer then
					logMessage(`[DistributedQueue][{instanceId}] This server cannot be a processor. Releasing lock.`, INFO_SEVERITY)
					properties[INDEX_SERVER_ROLE] = MAIN_SERVER
					self:ReleaseLock(true) -- Relinquish role if it cannot be a processor.
					properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
					return true
				end

				becomeMainServer(properties)
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end
		end

		if serverLevel == PROCESSOR_SERVER then
			if not properties.CanBeProcessor then
				logMessage(`[DistributedQueue][{instanceId}] This server cannot be a processor. Releasing lock.`, WARNING_SEVERITY)
				self:ReleaseLock(true) -- Relinquish role if it cannot be a processor.
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			-- This server is a regular processor. Attempt to refresh the lock.
			local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, ServerId, function(value): boolean?
				if not properties.CanBeProcessor then
					releaseLock = true
					return nil -- Abort update, this server cannot be a processor.
				end

				if value then return true end -- Lock exists, refresh it.
				return nil -- Lock expired, abort refresh.
			end, SESSION_TIMEOUT)

			if releaseLock then
				logMessage(`[DistributedQueue][{instanceId}] This server cannot be a processor. Releasing lock.`, INFO_SEVERITY)
				self:ReleaseLock(true) -- Relinquish role if it cannot be a processor.
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			if success and updateResult then
				logMessage(`[DistributedQueue][{instanceId}] Successfully refreshed processor lock.`, INFO_SEVERITY)

				if not properties.CanBeProcessor then
					logMessage(`[DistributedQueue][{instanceId}] This server is not eligible to be a processor.`, INFO_SEVERITY)
					self:ReleaseLock(true) -- If this server cannot be a processor, release the lock.
				end

				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			logMessage(
				`[DistributedQueue][{instanceId}] Session expired or failed to refresh. This server is no longer a processor.`,
				IMPORTANT_INFO_SEVERITY
			)
			self:ReleaseLock(success) -- If refresh failed due to network, don't publish release.

			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return false
		elseif serverLevel == NON_PROCESSOR_SERVER then
			-- This server is not a processor. Check if it can become one.
			if not properties.CanBeProcessor then
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end -- Not eligible to be a processor.

			local getSizeSuccess, currentProcessorsAmount = retryAsync(memoryStore.GetSizeAsync, memoryStore)
			if not getSizeSuccess then
				logMessage(`[DistributedQueue][{instanceId}] Failed to get current processor count.`, MINOR_ISSUE_SEVERITY)
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return false
			end

			-- If there are no active processors, attempt to become the main processor.
			if currentProcessorsAmount == 0 and properties.CanBeMainServer then
				logMessage(`[DistributedQueue][{instanceId}] No active processors. Attempting to become main processor.`, INFO_SEVERITY)
				local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
					if not properties.CanBeMainServer then
						logMessage(`[DistributedQueue][{instanceId}] This server cannot be the main server.`, INFO_SEVERITY)
						return nil -- Abort update, this server cannot be main.
					end
					if not value then return ServerId end -- No main server, claim it.
					return nil -- Another server claimed it first.
				end, SESSION_TIMEOUT)

				if not success then
					logMessage(`[DistributedQueue][{instanceId}] Failed to become main processor: {updateResult}`, MINOR_ISSUE_SEVERITY)
					properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
					return false
				end

				if updateResult then
					logMessage(`[DistributedQueue][{instanceId}] Successfully became the main processor.`, IMPORTANT_INFO_SEVERITY)

					if not properties.CanBeMainServer then
						logMessage(`[DistributedQueue][{instanceId}] This server cannot be a processor. Releasing lock.`, INFO_SEVERITY)
						properties[INDEX_SERVER_ROLE] = MAIN_SERVER
						self:ReleaseLock(true) -- Relinquish role if it cannot be a processor.
						properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
						return true
					end

					becomeMainServer(properties)
					properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
					return true
				else
					logMessage(`[DistributedQueue][{instanceId}] Another server is already the main processor.`, INFO_SEVERITY)
				end
			end

			-- If capacity is full, do nothing.
			if currentProcessorsAmount >= maxQueueProcessor then
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			-- Request to become a processor.
			logMessage(`[DistributedQueue][{instanceId}] Requesting to become a processor...`, INFO_SEVERITY)
			local processorDistributionQueue = getProcessorDistributionQueue(instanceId)

			local success, errorMsg: string? = retryAsync(
				processorDistributionQueue.AddAsync,
				processorDistributionQueue,
				ServerId,
				SESSION_REFRESH_INTERVAL,
				properties.ProcessorCandidacyPriority
			)
			if not success then
				logMessage(`[DistributedQueue][{instanceId}] Failed to request processor role: {errorMsg}`, MINOR_ISSUE_SEVERITY)
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return false
			end
		end
		properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
		return true
	end

	function DistributedQueuePrototype:Init(): boolean
		local properties = getQueueProperties(self)
		if properties[INDEX_INITIALIZED] then
			logMessage(`[DistributedQueue][{properties.InstanceId}] DistributedQueue is already initialized.`, WARNING_SEVERITY, true)
			return false
		end
		logMessage(`[DistributedQueue][{properties.InstanceId}] Initializing.`, INFO_SEVERITY)
		properties[INDEX_INITIALIZED] = true

		if properties.MaxQueueProcessor == NO_LIMIT then
			-- No limit on processors. Every server can process.
			if properties.CanBeProcessor then
				logMessage(
					`[DistributedQueue][{properties.InstanceId}] No limit on queue processors. This server will process items.`,
					INFO_SEVERITY
				)
				becomeProcessorServer(properties)
			else
				logMessage(
					`[DistributedQueue][{properties.InstanceId}] No limit on queue processors. This server will not process items, as it cannot be processor.`,
					INFO_SEVERITY
				)
			end
			return true
		end

		local instanceId = properties.InstanceId

		-- Subscribe to messages for faster state changes.
		local success, subscription = retryAsync(
			MessagingService.SubscribeAsync,
			MessagingService,
			DISTRIBUTED_QUEUE_CHANGES_TOPIC,
			function(message)
				local messageData = message.Data
				if messageData.InstanceId ~= instanceId then return end -- Ignore messages for other instances.

				if messageData.Event == "ProcessorReleased" or messageData.Event == "MainServerReleased" then
					if not properties.CanBeProcessor or self:IsActiveProcessor() then return end
					-- A slot has opened up. Try to take it.
					logMessage(
						`[DistributedQueue][{instanceId}] Received release message. Checking for available processor slots.`,
						INFO_SEVERITY
					)
					self:UpdateSession()
				elseif messageData.Event == "CreateProcessor" then
					if messageData.ServerId ~= ServerId then return end
					-- This server's request was approved by the main server.
					logMessage(
						`[DistributedQueue][{instanceId}] Processor request approved. This server is now a processor.`,
						IMPORTANT_INFO_SEVERITY
					)
					becomeProcessorServer(properties)
				end
			end
		)

		if not success then
			logMessage(
				`[DistributedQueue][{instanceId}] Failed to subscribe to DistributedQueue changes: {subscription}`,
				IMPORTANT_INFO_SEVERITY
			)
			properties[INDEX_INITIALIZED] = false -- Reset initialization state.
			return false
		end

		properties[INDEX_SUBSCRIPTION] = subscription -- Store the subscription for later cleanup.

		if properties.AutomateLifecycle then
			logMessage(`[CentralizedQueue][{instanceId}] Lifecycle automation is enabled. Starting session update.`, INFO_SEVERITY)
			-- Immediately try to become the main server on initialization.
			task.spawn(self.UpdateSession, self)
			-- Start a persistent thread to periodically refresh the session lock.
			table.insert(ActiveSessionQueues, self)
		end

		logMessage(`[DistributedQueue][{instanceId}] Initialized successfully.`, INFO_SEVERITY)
		return true
	end

	Constructor.CreateDistributedQueue = function<T>(instanceId: string?): DistributedQueue<T>
		local proxy, properties = createBaseQueue(instanceId, "DistributedQueue")
		if not properties then return proxy :: DistributedQueue<T> end

		local id = properties.InstanceId

		properties[INDEX_PROTOTYPE] = DistributedQueuePrototype
		properties[INDEX_CALLBACKS] = {} :: Callbacks<T>
		properties[INDEX_MEMORY_STORE_LOCK] = MemoryStoreService:GetSortedMap(MEMORY_STORE_LOCK_FORMAT:format(properties.ClassName, id))
		properties[INDEX_MEMORY_STORE_QUEUE] =
			MemoryStoreService:GetQueue(MEMORY_STORE_QUEUE_FORMAT:format(properties.ClassName, id), INVISIBILITY_TIMEOUT)
		properties[INDEX_SERVER_ROLE] = NON_PROCESSOR_SERVER -- Start as a non-processor

		properties.MaxQueueProcessor = DEFAULT_MAX_QUEUE_PROCESSOR
		properties.CanBeProcessor = DEFAULT_CAN_BE_PROCESSOR
		properties.ProcessorCandidacyPriority = DEFAULT_PROCESSOR_CANDIDACY_PRIORITY

		return proxy :: DistributedQueue<T>
	end
end

do -- CentralizedQueue
	--[[
		A CentralizedQueue is a queue where a single "main" server is elected to process all items.
		This is useful for tasks that must not be run in parallel. Other servers can add items
		to this queue, but only the elected main server will read and process them.
	]]

	local CentralizedQueuePrototype = {} :: CentralizedQueue<any>
	setmetatable(CentralizedQueuePrototype, BaseQueueMetatable)

	local function becomeMainServer(properties: BaseProperties): ()
		if properties[INDEX_SERVER_ROLE] == MAIN_SERVER then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Already the main server.`, WARNING_SEVERITY, true)
			return
		end

		if not properties.CanBeMainServer then
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Cannot become main server. CanBeMainServer is false.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		properties[INDEX_SERVER_ROLE] = MAIN_SERVER
		properties[INDEX_MAIN_SERVER_ID] = ServerId -- Mark this server as the main server.
		if properties.AutomateLifecycle then
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Lifecycle automation is enabled. Starting update thread.`,
				INFO_SEVERITY
			)
			startUpdateThread(properties)
		else
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Lifecycle automation is disabled. Not starting update thread.`,
				INFO_SEVERITY
			)
		end
	end

	function CentralizedQueuePrototype:ReleaseLock(immediateRelease: boolean?)
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		if properties[INDEX_SERVER_ROLE] ~= MAIN_SERVER then
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Attempted to release lock, but this is not the main server.`,
				WARNING_SEVERITY,
				true
			)
			return
		end

		stopUpdateThread(properties)
		properties[INDEX_SERVER_ROLE] = NON_PROCESSOR_SERVER -- Reset server role to non-processor
		properties[INDEX_MAIN_SERVER_ID] = nil

		if immediateRelease == false then
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Lock ownership lost locally. Waiting for session to expire in MemoryStore.`,
				INFO_SEVERITY
			)
			return
		end

		logMessage(`[CentralizedQueue][{properties.InstanceId}] Releasing main server lock...`, INFO_SEVERITY)
		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreHashMap
		local instanceId = properties.InstanceId
		local success, errorMsg: string? = retryAsync(memoryStore.RemoveAsync, memoryStore, "MainServer")
		if success then
			logMessage(`[CentralizedQueue][{instanceId}] Main server lock released.`, INFO_SEVERITY)

			-- Notify other servers that they can now attempt to claim the main server role.
			logMessage(`[CentralizedQueue][{instanceId}] Notifying other servers of main server release.`, INFO_SEVERITY)
			retryAsync(MessagingService.PublishAsync, MessagingService, CENTRALIZED_QUEUE_CHANGES_TOPIC, {
				Event = "MainServerReleased",
				InstanceId = instanceId,
				PreviousServer = ServerId,
			})
		else
			logMessage(
				`[CentralizedQueue][{instanceId}] Failed to remove main server lock from MemoryStore during release: {errorMsg}`,
				MINOR_ISSUE_SEVERITY
			)
		end
	end

	-- Attempts to claim or refresh the main server lock using a transactional update.
	-- This function is the core of the main server election process.
	function CentralizedQueuePrototype:UpdateSession(): boolean
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		if properties[INDEX_UPDATE_FLAG] then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Session is being updated.`, INFO_SEVERITY)
			return true -- Session is being updated
		end

		properties[INDEX_UPDATE_FLAG] = coroutine.running() -- Set the flag to prevent re-entrancy.

		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreHashMap
		local instanceId = properties.InstanceId

		if not properties.CanBeMainServer then -- Not eligible to be main server.
			logMessage(`[CentralizedQueue][{instanceId}] This server cannot be the main server.`, INFO_SEVERITY)
			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return true
		end

		local currentMainServerId: string
		local releaseLock = false

		local wasMainServer = self:IsMainServer()

		-- UpdateAsync provides an atomic way to read and write the "MainServer" key.
		local success, errorMsg = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
			currentMainServerId = value or ServerId

			if not properties.CanBeMainServer then
				releaseLock = true
				return nil -- Abort update, this server cannot be main.
			end

			-- If no server is main, or if this server is already main, claim/refresh the lock.
			if not value or value == ServerId then return ServerId end

			-- Otherwise, another server holds the lock. Do not change it.
			return nil -- Abort the update operation.
		end, SESSION_TIMEOUT)

		if wasMainServer and releaseLock then
			logMessage(`[CentralizedQueue][{instanceId}] This server cannot be the main server. Releasing lock.`, INFO_SEVERITY)
			-- Relinquish role if it cannot be main server.
			-- Publish a message only if the lock wasn't held by other server.
			self:ReleaseLock(currentMainServerId == ServerId)
			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return true
		end

		if not success then
			logMessage(`[CentralizedQueue][{instanceId}] Failed to update session lock: {errorMsg}`, MINOR_ISSUE_SEVERITY)
			if wasMainServer then self:ReleaseLock(false) end
			properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
			return false
		end

		-- Update local state based on the outcome.
		local isMainServer = currentMainServerId == ServerId

		if isMainServer and not wasMainServer then
			logMessage(`[CentralizedQueue][{instanceId}] This server has become the main server.`, IMPORTANT_INFO_SEVERITY)

			if not properties.CanBeMainServer then
				logMessage(`[CentralizedQueue][{instanceId}] This server cannot be the main server. Releasing lock.`, INFO_SEVERITY)
				properties[INDEX_SERVER_ROLE] = MAIN_SERVER -- Mark this server as the main server.
				self:ReleaseLock(true) -- Relinquish role if it cannot be main server.
				properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
				return true
			end

			becomeMainServer(properties)
		elseif not isMainServer then
			if wasMainServer then
				logMessage(`[CentralizedQueue][{instanceId}] This server has lost its main server status.`, IMPORTANT_INFO_SEVERITY)
				self:ReleaseLock(false)
			else
				properties[INDEX_MAIN_SERVER_ID] = currentMainServerId
			end
		end

		properties[INDEX_UPDATE_FLAG] = false -- Reset the update flag.
		return true
	end

	function CentralizedQueuePrototype:SetLifecycleAutomation(automateLifecycle: boolean)
		local properties = getQueueProperties(self)
		if type(automateLifecycle) ~= "boolean" then error("AutomateLifecycle must be a boolean.", 2) end

		local previousLifecycleAutomation = properties.AutomateLifecycle
		if automateLifecycle == previousLifecycleAutomation then
			logMessage(
				`[CentralizedQueue][{properties.InstanceId}] Lifecycle automation already set to {automateLifecycle}.`,
				INFO_SEVERITY
			)
			return
		end

		properties.AutomateLifecycle = automateLifecycle
		if automateLifecycle then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Lifecycle automation enabled.`, INFO_SEVERITY)
			if properties[INDEX_SERVER_ROLE] == MAIN_SERVER then startUpdateThread(properties) end
			local index = table.find(ActiveSessionQueues, self)
			if not index then
				table.insert(ActiveSessionQueues, self) -- Add to active session queues if not already present.
			end
		else
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Lifecycle automation disabled.`, INFO_SEVERITY)
			if properties[INDEX_SERVER_ROLE] == MAIN_SERVER then stopUpdateThread(properties) end
			local index = table.find(ActiveSessionQueues, self)
			if index then
				table.remove(ActiveSessionQueues, index) -- Remove from active session queues.
			end
		end
	end

	function CentralizedQueuePrototype:Init()
		local properties = getQueueProperties(self)
		if properties[INDEX_INITIALIZED] then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] CentralizedQueue is already initialized.`, WARNING_SEVERITY, true)
			return false
		end
		logMessage(`[CentralizedQueue][{properties.InstanceId}] Initializing.`, INFO_SEVERITY)
		properties[INDEX_INITIALIZED] = true

		local instanceId = properties.InstanceId
		-- Subscribe to messages indicating the main server lock has been released.
		-- This allows for a faster takeover than waiting for the session lock to expire.
		local success, subscription = retryAsync(
			MessagingService.SubscribeAsync,
			MessagingService,
			CENTRALIZED_QUEUE_CHANGES_TOPIC,
			function(message)
				local messageData = message.Data
				if messageData.InstanceId ~= instanceId then return end
				if messageData.Event == "MainServerReleased" then
					if messageData.PreviousServer == ServerId then return end
					logMessage(
						`[CentralizedQueue][{instanceId}] Received release message. Attempting to become main server.`,
						INFO_SEVERITY
					)
					self:UpdateSession()
				end
			end
		)

		if not success then
			logMessage(
				`[CentralizedQueue][{instanceId}] Failed to subscribe to CentralizedQueue changes: {subscription}`,
				IMPORTANT_INFO_SEVERITY
			)
			properties[INDEX_INITIALIZED] = false
			return false
		end

		properties[INDEX_SUBSCRIPTION] = subscription -- Store the subscription for later cleanup.

		if properties.AutomateLifecycle then
			logMessage(`[CentralizedQueue][{instanceId}] Lifecycle automation is enabled. Starting session update.`, INFO_SEVERITY)
			-- Immediately try to become the main server on initialization.
			task.spawn(self.UpdateSession, self)
			-- Start a persistent thread to periodically refresh the session lock.
			table.insert(ActiveSessionQueues, self)
		end

		logMessage(`[CentralizedQueue][{instanceId}] Initialized successfully.`, INFO_SEVERITY)
		return true
	end

	Constructor.CreateCentralizedQueue = function<T>(instanceId: string?): CentralizedQueue<T>
		local proxy, properties = createBaseQueue(instanceId, "CentralizedQueue")
		if not properties then return proxy :: CentralizedQueue<T> end

		local id = properties.InstanceId

		properties[INDEX_PROTOTYPE] = CentralizedQueuePrototype
		properties[INDEX_CALLBACKS] = {} :: Callbacks<T>
		properties[INDEX_SERVER_ROLE] = NON_PROCESSOR_SERVER -- Start as a non-processor
		properties[INDEX_MEMORY_STORE_LOCK] = MemoryStoreService:GetHashMap(MEMORY_STORE_LOCK_FORMAT:format(properties.ClassName, id))
		properties[INDEX_MEMORY_STORE_QUEUE] =
			MemoryStoreService:GetQueue(MEMORY_STORE_QUEUE_FORMAT:format(properties.ClassName, id), INVISIBILITY_TIMEOUT)

		return proxy :: CentralizedQueue<T>
	end
end

--// Global Update & Shutdown Hooks \\--

-- This background thread periodically calls UpdateSession for all active queues
-- to refresh locks and handle state transitions.
local GlobalSessionUpdateThread = task.spawn(function()
	while true do
		task.wait(SESSION_REFRESH_INTERVAL)
		logMessage(`[CentralizedServer] Periodically refreshing session locks for {#ActiveSessionQueues} active queues.`, INFO_SEVERITY)
		for _, queue: BaseQueue<any, any> in ActiveSessionQueues do
			task.spawn(queue.UpdateSession, queue)
		end
	end
end)

-- Ensures graceful shutdown by releasing all held locks when the server shuts down.
game:BindToClose(function()
	if #ActiveSessionQueues == 0 then return end
	logMessage(`[CentralizedServer] Server shutting down. Releasing locks for {#ActiveSessionQueues} active queues.`, INFO_SEVERITY)

	local nThreads = #ActiveSessionQueues
	local mainThread = coroutine.running()
	task.cancel(GlobalSessionUpdateThread)

	local function shutdownQueue(queue: BaseQueue<any, any>)
		local queueName = tostring(queue)
		logMessage(`[CentralizedServer] Shutting down queue {queueName}.`, INFO_SEVERITY)
		queue:Shutdown()
		logMessage(`[CentralizedServer] Queue {queueName} has been shut down.`, INFO_SEVERITY)
		nThreads -= 1
		if nThreads <= 0 then
			logMessage("[CentralizedServer] All shutdown threads completed.", INFO_SEVERITY)
			coroutine.resume(mainThread) -- Resume the main thread to continue shutdown.
		end
	end

	for _, queue: BaseQueue<any, any> in ActiveSessionQueues do
		task.defer(shutdownQueue, queue)
	end

	-- Wait for all shutdown threads to complete.
	if nThreads > 0 then
		logMessage("[CentralizedServer] Waiting for all shutdown threads to complete...", INFO_SEVERITY)
		coroutine.yield()
	end
	logMessage("[CentralizedServer] All queues have been shut down.", IMPORTANT_INFO_SEVERITY)
end)

--// Constructor \\--

Constructor.SetServerId = function(serverId: string)
	-- Set the server ID for the current server.
	if type(serverId) ~= "string" then error("Server ID must be a string.", 2) end
	if serverId == "" then error("Server ID cannot be an empty string.", 2) end

	ServerId = serverId
	logMessage(`[Constructor] Server ID set to {serverId}.`, INFO_SEVERITY)
end

Constructor.SetRetryableErrors = function(errors: { string })
	-- Set the retryable errors for the retryAsync function.
	if type(errors) ~= "table" then error("Expected a table of retryable errors.", 2) end
	if getmetatable(errors :: any) ~= nil then error("Retryable errors must be a plain table without metatable.", 2) end

	local nErrors = #errors
	if nErrors > 0 and next(errors, nErrors) ~= nil then error("Retryable errors must be a plain array without keys.", 2) end

	local lastIndex: number

	for i, value in ipairs(errors) do
		if type(value) ~= "string" then error("Retryable errors must be an array of string.", 2) end
		lastIndex = i + 0
	end

	if lastIndex ~= nErrors then error("Retryable errors must be a plain array without gaps.", 2) end

	RetryableNetworkErrors = errors
	logMessage("[Constructor] Retryable errors set successfully.", INFO_SEVERITY)
end

Constructor.SuppressLogLevel = function(level: number)
	-- Suppress logging for the specified severity level.
	if type(level) ~= "number" then error("Log level must be a number.", 2) end
	if level < 1 or level > 5 or level // 1 ~= level then error("Log level must be an integer between 1 and 5.", 2) end

	SuppressedLogLevels[level] = true
	logMessage(`[Constructor] Log level {level} is now suppressed.`, INFO_SEVERITY)
end

Constructor.AddLogCallback = function(callback: (message: string, severity: number) -> ())
	-- Add a callback to be called on every log message.
	if type(callback) ~= "function" then error("Log callback must be a function.", 2) end

	table.insert(LogCallbacks, callback)
	logMessage("[Constructor] Log callback added successfully.", INFO_SEVERITY)

	local disconnected = false
	return function()
		if disconnected then
			logMessage("[Constructor] Log callback already disconnected.", WARNING_SEVERITY, true)
			return
		end

		local index = table.find(LogCallbacks, callback)
		if index then
			logMessage("[Constructor] Disconnecting log callback.", INFO_SEVERITY)
			table.remove(LogCallbacks, index)
		end

		disconnected = true
	end
end

--// Return \\--

return table.freeze(Constructor)
