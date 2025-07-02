--!strict

--[[
	Centralized Server & Distributed Queue System
	---------------------------------------------
	Author: @meifeng_ft
	Date: 2025-07-03 (Last Updated)
	Version: 2.0.0

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
	- **Fault Tolerance**: Implements retry mechanisms with exponential backoff for
	  MemoryStore operations, enhancing resilience against transient network issues. 
	- **Graceful Shutdown**: Ensures the main server or processor lock is released upon server shutdown,
	  allowing another server to quickly take over. 
	- **Dynamic Callbacks**: Provides a mechanism for registering and unregistering callback functions
	  for processing items from any queue. 

	Important Notes:
	- Relies heavily on `MemoryStoreService`. Ensure quotas are sufficient for your use case. 
	- `MessagingService` is used for rapid main server and processor recovery and is highly recommended. 
	- The `instanceId` parameter in constructors creates independent centralized server/queue systems. 
	- `INVISIBILITY_TIMEOUT` and retry logic assertions are critical for preventing
	  duplicate processing of queue items. 
]]

--// Types \\--

-- A function that processes a value from the queue.
type Callback<T> = (value: T) -> ()
-- A list of callback functions.
type Callbacks<T> = { Callback<T> }

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
}

-- The base interface for all queue objects.
type BaseQueue<self, T> = {
	-- Queues a value to be processed.
	-- For CentralizedQueues, only the main server will process it.
	-- For DistributedQueues, any active processor server can process it.
	-- @returns True if the item was successfully queued, false otherwise.
	AddToQueue: (self: self, value: T, priority: number?) -> (boolean, string?),

	-- Registers a callback to execute when an item from this queue is processed.
	-- @returns A 'disconnect' function that, when called, removes the callback.
	AddCallback: (self: self, callback: (value: T) -> ()) -> () -> (),

	-- Attempts to acquire or refresh the session lock for this server.
	-- For CentralizedQueues, this is the main server election process.
	-- For DistributedQueues, this determines if the server is a main or regular processor.
	UpdateSession: (self: self) -> boolean,

	-- Returns true if the current server instance holds the main server lock for this queue.
	IsMainServer: (self: self) -> boolean,

	-- Voluntarily relinquishes the main server or processor role, allowing another server to take over.
	-- This should be called during graceful shutdown.
	ReleaseLock: (self: self, immediateRelease: boolean?) -> (),

	-- Sets the maximum number of items to read from the queue in a single API call.
	SetReadCount: (self: self, readCount: number) -> (),
	-- Sets the maximum total number of items to read from the queue within one PollInterval.
	SetMaxReadPerInterval: (self: self, maxReadPerInterval: number) -> (),
	-- Sets the interval (in seconds) at which the queue is polled for new items.
	SetPollInterval: (self: self, pollInterval: number) -> (),

	-- Enables or disables batching for queue reads.
	SetBatchEnabled: (self: self, shouldBatch: boolean) -> (),
	-- Sets the maximum wait time (in seconds) for a batch of items to be ready before processing.
	SetBatchWaitTime: (self: self, waitTime: number) -> (),

	-- Sets the expiration time (in seconds) for items in the queue.
	-- Items will be automatically removed after this time if not processed.
	SetItemExpirationTime: (self: self, expiration: number) -> (),

	-- Sets whether this server is eligible to become the main server. If set to false on the current
	-- main server, it will release its lock.
	SetCanBeMainServer: (self: self, canBeMainServer: boolean) -> (),

	-- Gracefully shuts down the queue on this server, releasing any locks held.
	Shutdown: (self: self) -> (),

	-- Initializes the queue, starting session management and processing threads if applicable.
	Init: (self: self) -> boolean,
} & BaseProperties

-- Properties specific to a DistributedQueue.
type DistributedQueueProperties = {
	ClassName: "DistributedQueue",

	-- The maximum number of servers that can process the queue simultaneously.
	-- A value of -1 means there is no limit.
	MaxQueueProcessor: number,
	-- Determines if this server is eligible to become a processor for the queue.
	CanBeProcessor: boolean,
}

-- A queue that can be processed by multiple servers simultaneously.
export type DistributedQueue<T> = {
	-- Returns true if the current server is an active processor (either main or regular) for the queue.
	IsActiveProcessor: (self: DistributedQueue<T>) -> boolean,

	-- Sets the maximum number of servers that can process the queue simultaneously.
	-- A value of -1 means there is no limit.
	SetMaxQueueProcessor: (self: DistributedQueue<T>, concurrency: number) -> (),

	-- Sets whether this server is eligible to become a processor. If set to false on an active
	-- processor, it will release its lock.
	SetCanBeProcessor: (self: DistributedQueue<T>, canBeProcessor: boolean) -> (),
} & BaseQueue<DistributedQueue<T>, T> & DistributedQueueProperties

-- A queue that is processed by a single, elected main server.
export type CentralizedQueue<T> = {
	ClassName: "CentralizedQueue",

	-- Retrieves the JobId of the server currently designated as the main server.
	-- Returns nil if no main server is currently elected.
	GetMainServerId: (self: CentralizedQueue<T>) -> string?,
} & BaseQueue<DistributedQueue<T>, T>

-- The public constructor interface for creating queues.
type Constructor = {
	-- Creates a centralized server instance where only one server processes the queue.
	-- An optional `instanceId` can be provided to manage multiple independent systems.
	CreateCentralizedQueue: <T>(instanceId: string?) -> CentralizedQueue<T>,

	-- Creates a distributed queue instance that can be processed by multiple servers.
	CreateDistributedQueue: <T>(instanceId: string) -> DistributedQueue<T>,

	-- Sets the list of network errors that should be retried.
	SetRetryableErrors: (errors: { [string]: boolean}) -> (),

	-- Suppresses logging for a specific severity level.
	SurpressLogLevel: (logSeverity: number) -> (),
}

type proxy = typeof(newproxy(true))

--// Services \\--

local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")
local HttpService = game:GetService("HttpService")

--// Variables \\--

-- A unique identifier to prevent conflicts with other MemoryStore and MessagingService users in the experience.
local UNIQUE_KEY = "54fe96ac-e121-4eb9-a4c6-d2c22f1ffde4"
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

-- The maximum number of items that can be read from a MemoryStore queue in a single operation.
local MAX_MEMORY_STORE_READ_COUNT = 100
-- The number of items to retrieve from a queue in a single read operation.
local DEFAULT_READ_COUNT = MAX_MEMORY_STORE_READ_COUNT
-- The default priority for items added to a queue if not specified.
local DEFAULT_QUEUE_PRIORITY = 0
-- The maximum number of items that can be read from a queue in a single interval.
-- This is set to -1 by default, meaning no limit on the number of items read.
local DEFAULT_MAX_READ_PER_INTERVAL = -1
-- The default batch processing behavior for distributed queues.
local ENABLE_BATCH_BY_DEFAULT = false
-- The default wait time (in seconds) for batch processing in distributed queues.
local DEFAULT_BATCH_WAIT_TIME = 30

-- This assertion ensures the read count does not exceed the MemoryStoreQueue API limit.
assert(DEFAULT_READ_COUNT <= MAX_MEMORY_STORE_READ_COUNT, "DEFAULT_READ_COUNT must be less than or equal to MAX_MEMORY_STORE_READ_COUNT")

-- MessagingService topics for notifying servers of state changes.
local DISTRIBUTED_QUEUE_CHANGES_TOPIC = `DistributedQueue_ServerChanges_{UNIQUE_KEY}`
local CENTRALIZED_QUEUE_CHANGES_TOPIC = `CentralizedQueue_ServerChanges_{UNIQUE_KEY}`

-- Default settings for server eligibility.
local DEFAULT_CAN_BE_MAIN_SERVER = true -- Default to allowing servers to become the main server.
local DEFAULT_MAX_QUEUE_PROCESSOR = -1 -- Default to no limit on the number of servers processing the queue.
local DEFAULT_CAN_BE_PROCESSOR = true -- Default to allowing servers to process the queue.

--// Mapping & State

-- Internal indices for accessing properties within the proxy's internal table.
local INDEX_PROTOTYPE = 1 -- The prototype table for the queue object.
local INDEX_CALLBACKS = 2 -- The list of registered callback functions.
local INDEX_MEMORY_STORE_LOCK = 3 -- The MemoryStore (HashMap or SortedMap) used for session locking.
local INDEX_MEMORY_STORE_QUEUE = 4 -- The MemoryStoreQueue used for items.
local INDEX_SERVER_ID = 5 -- The role of the current server (e.g., MAIN_SERVER, PROCESSOR_SERVER).
local INDEX_UPDATE_THREAD = 6 -- The coroutine handling queue processing.
local INDEX_INITIALIZED = 7 -- A boolean flag indicating if Init() has been called.

-- Represents the different roles a server can have for a given queue.
local NON_PROCESSOR_SERVER = 0 -- Server does not process the queue.
local PROCESSOR_SERVER = 1 -- Server is a regular processor for a DistributedQueue.
local MAIN_SERVER = 2 -- Server is the main processor for a DistributedQueue or CentralizedQueue.

-- Severity levels for logging messages.
local INFO_SEVERITY = 1 -- Informational messages.
local IMPORTANT_INFO_SEVERITY = 2 -- Important informational messages that may require attention.
local MINOR_ISSUE_SEVERITY = 3 -- Minor issues that do not affect functionality.
local WARNING_SEVERITY = 4 -- Warnings that may indicate potential issues.
local ERROR_SEVERITY = 5 -- Critical errors that require attention.

-- The unique identifier for the current server instance.
local ServerId = game.JobId
-- Maps a queue proxy object to its internal properties table.
local ProxyMap: { [proxy]: BaseProperties & DistributedQueueProperties } = {}
-- Maps a unique queue name (ClassName_InstanceId) to its proxy object to prevent duplicates.
local BaseQueueMap: { [string]: CentralizedQueue<any> | DistributedQueue<any> } = {}

-- The constructor object that will be returned by the module.
local Constructor: Constructor = {} :: Constructor

-- A list of all active queues that require periodic session updates.
local ActiveSessionQueues: { CentralizedQueue<any> | DistributedQueue<any> } = {}

-- A list of specific error message substrings that signify transient network issues, which are safe to retry.
-- This is currently unpopulated as Roblox does not provide specific error codes for MemoryStoreService and MessagingService.
-- It can be populated by the user to include any known retryable errors.
local RetryableNetworkErrors
local SurpressedLogLevels: { [number]: boolean } = {
	[INFO_SEVERITY] = false,
	[IMPORTANT_INFO_SEVERITY] = false,
	[MINOR_ISSUE_SEVERITY] = false,
	[WARNING_SEVERITY] = false,
	[ERROR_SEVERITY] = false,
}

--// Private Functions \\--

-- A utility function to create an error output that can be used without stopping the thread.
local function createErrorOutput(msg: string)
	task.spawn(error, msg)
end

-- A utility function to log messages with optional severity and traceback.
local function logMessage(message: string, severity: number?, includeTraceback: boolean?)
	local messageSeverity = severity or INFO_SEVERITY

	if SurpressedLogLevels[messageSeverity] then return end -- Skip logging if this severity is suppressed.

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
		error("Invalid severity level. Must be 1 (Info), 2 (Issue), 3 (Warning), or 4 (Error).", 2)
	end

	if includeTraceback then output = output .. `\nTraceback: {debug.traceback()}` end

	outputFunc(output)
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
		if RetryableNetworkErrors and next(RetryableNetworkErrors) ~= nil then
			local shouldRetry = false

			for errorCode in RetryableNetworkErrors do
				if errorMsg:find(errorCode) then
					shouldRetry = true
					break
				end
			end

			if not shouldRetry then
				logMessage(`[Retry] Non-retryable error for function '{func}': {errorMsg}`, ERROR_SEVERITY, true)
				return false, errorMsg -- Non-retryable error.
			end
		end

		logMessage(`[Retry] Network operation failed for function '{func}', attempt ({retryAttempt}/{MAX_ATTEMPTS}): {errorMsg}`, MINOR_ISSUE_SEVERITY)

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

-- Creates and starts the main processing thread for a queue.
local function createUpdateThread(properties: BaseProperties & DistributedQueueProperties)
	if properties[INDEX_UPDATE_THREAD] then return end -- Avoid starting multiple threads.
	if properties.MaxQueueProcessor ~= -1 and properties[INDEX_SERVER_ID] == NON_PROCESSOR_SERVER then
		logMessage(`[{properties.ClassName}] Attempted to start update thread, but this is not a processor server.`, WARNING_SEVERITY, true)
		return
	end

	local memoryStoreQueue = properties[INDEX_MEMORY_STORE_QUEUE] :: MemoryStoreQueue
	logMessage(`[{properties.ClassName}][{properties.InstanceId}] This server is now an active processor. Starting queue processing.`, INFO_SEVERITY)

	local callbacks = properties[INDEX_CALLBACKS]

	properties[INDEX_UPDATE_THREAD] = task.spawn(function()
		while true do
			-- Read a batch of items from the queue.
			local readCount = properties.ReadCount
			local maxReadPerInterval = properties.MaxReadPerInterval
			local batchWaitTime = if properties.BatchEnabled then properties.BatchWaitTime else 0
			local itemsProcessedThisInterval = 0

			while maxReadPerInterval == -1 or itemsProcessedThisInterval < maxReadPerInterval do
				local readAmount = if maxReadPerInterval == -1
					then readCount
					else math.min(readCount, maxReadPerInterval - itemsProcessedThisInterval)
				if readAmount <= 0 then break end

				local success, items, id: any = retryAsync(memoryStoreQueue.ReadAsync, memoryStoreQueue, readAmount, false, batchWaitTime)

				if success then
					local nItems = #items
					if nItems == 0 then break end -- Queue is empty.

					itemsProcessedThisInterval += nItems

					-- If read is successful, permanently remove the items to prevent reprocessing.
					local removeSuccess, removeError = retryAsync(memoryStoreQueue.RemoveAsync, memoryStoreQueue, id)
					if not removeSuccess then
						logMessage(
							`[{properties.ClassName}][{properties.InstanceId}] Failed to remove items from queue after reading. Items may be processed again. Error: {removeError}`,
							MINOR_ISSUE_SEVERITY
						)
						break
					end

					executeCallbacks(callbacks, items)
					logMessage(`[{properties.ClassName}][{properties.InstanceId}] Processed {nItems} items from the queue.`, MINOR_ISSUE_SEVERITY)

					-- If we read fewer items than requested, it indicates the queue is now empty.
					if nItems < readAmount then break end
				else
					logMessage(`[{properties.ClassName}][{properties.InstanceId}] Failed to read from queue: {items}`, INFO_SEVERITY)
					-- Break on read failure to avoid a tight loop of failed reads.
					break
				end
			end

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
	__index = function(self: proxy, key)
		local properties = getQueueProperties(self)
		local value = properties[key]
		if value then return value end

		return properties[INDEX_PROTOTYPE][key]
	end,
	__tostring = function(self: proxy)
		local properties = getQueueProperties(self)
		return `{properties.ClassName}_{properties and properties.InstanceId or "Unknown"}`
	end,
	__metatable = "Locked.",
}

-- Metatable for queue objects after they have been destroyed to prevent use-after-destroy errors.
local DestroyedObjectMetatable = {
	__index = function(self: proxy, key)
		error(`Attempted to access a destroyed object: {self} - {key}`, 2)
	end,
	__tostring = function(self: proxy)
		return `DestroyedQueue: {self}`
	end,
	__metatable = "Locked.",
}

-- The default template for creating the internal properties table of a new queue instance.
local BASE_QUEUE_TEMPLATE: { [any]: any } = table.freeze({
	[INDEX_PROTOTYPE] = nil,
	[INDEX_CALLBACKS] = nil,
	[INDEX_MEMORY_STORE_LOCK] = nil,
	[INDEX_MEMORY_STORE_QUEUE] = nil,
	[INDEX_SERVER_ID] = nil,
	[INDEX_UPDATE_THREAD] = nil,
	[INDEX_INITIALIZED] = nil,

	InstanceId = "global",
	ClassName = "DistributedQueue",
	ReadCount = DEFAULT_READ_COUNT,
	MaxReadPerInterval = DEFAULT_MAX_READ_PER_INTERVAL,
	PollInterval = DEFAULT_QUEUE_POLL_INTERVAL,
	BatchEnabled = ENABLE_BATCH_BY_DEFAULT,
	BatchWaitTime = DEFAULT_BATCH_WAIT_TIME,
	ItemExpirationTime = DEFAULT_QUEUE_EXPIRATION,
	CanBeMainServer = DEFAULT_CAN_BE_MAIN_SERVER,
})

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

	local success, errorMsg = retryAsync(queue.AddAsync, queue, value, priority or DEFAULT_QUEUE_PRIORITY, properties.ItemExpirationTime)
	if not success then
		logMessage(`[{self.ClassName}][{self.InstanceId}] Failed to add item to queue: {errorMsg}`, MINOR_ISSUE_SEVERITY)
		return false, errorMsg
	end

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
			logMessage(`[{self.ClassName}][{self.InstanceId}] Callback already disconnected.`, WARNING_SEVERITY)
			return
		end
		disconnected = true

		local index = table.find(callbacks, callback)
		if index then
			logMessage(`[{self.ClassName}][{self.InstanceId}] Disconnecting a callback.`, INFO_SEVERITY)
			table.remove(callbacks, index)
		end
	end
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
	if maxReadPerInterval ~= -1 and maxReadPerInterval < 1 then error("Invalid max read per interval.", 2) end
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
	properties.BatchWaitTime = waitTime or 0
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

	if not canBeMainServer and properties[INDEX_SERVER_ID] == MAIN_SERVER then
		logMessage(`[{self.ClassName}][{self.InstanceId}] CanBeMainServer set to false. Releasing main server lock.`, INFO_SEVERITY)
		self:ReleaseLock(true)
	end
end

function BaseQueuePrototype:Shutdown()
	local properties = getQueueProperties(self)
	logMessage(`[{self.ClassName}][{self.InstanceId}] Shutting down...`, INFO_SEVERITY)

	local index = table.find(ActiveSessionQueues, self)
	if index then table.remove(ActiveSessionQueues, index) end

	if properties[INDEX_SERVER_ID] == ServerId then self:ReleaseLock(true) end

	stopUpdateThread(properties) -- Ensure update thread is stopped

	logMessage(`[{self.ClassName}][{self.InstanceId}] Shutdown complete.`, INFO_SEVERITY)
	setmetatable(self, DestroyedObjectMetatable) -- Prevent further access to the queue.
end

table.freeze(BaseQueuePrototype)

--// Constructors \\--

-- Internal function to create a new base queue object, or return an existing one.
local function createBaseQueue<T>(
	instanceId: string?,
	className: string
): (BaseQueue<proxy, T>, string, (BaseProperties & DistributedQueueProperties)?)
	local id = instanceId or "global"
	local entryName = `{className}_{id}`
	local existingQueue = BaseQueueMap[entryName]

	if existingQueue then
		logMessage(`{className} with instanceId "{id}" already exists. Using existing instance.`, WARNING_SEVERITY)
		return existingQueue, id
	end

	logMessage(`Creating new {className} with instanceId "{id}".`, INFO_SEVERITY)
	local proxy = newproxy(true)
	local properties = table.clone(BASE_QUEUE_TEMPLATE)

	if instanceId then properties.InstanceId = instanceId end
	properties.ClassName = className

	BaseQueueMap[entryName] = proxy
	ProxyMap[proxy] = properties

	return setmetatable(proxy, ClassMetatable), id, properties
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
	local function createProcessor(memoryStoreQueue: MemoryStoreSortedMap, serverId: string, instanceId: string)
		local success, reason = retryAsync(memoryStoreQueue.SetAsync, memoryStoreQueue, serverId, true, SESSION_TIMEOUT)
		if not success then
			logMessage(`[DistributedQueue][{instanceId}] Failed to create processor for server {serverId}: {reason}`, MINOR_ISSUE_SEVERITY)
			return false
		end

		logMessage(`[DistributedQueue][{instanceId}] Server {serverId} is now a processor.`, INFO_SEVERITY)
		return true
	end

	function DistributedQueuePrototype:IsMainServer()
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		return properties[INDEX_SERVER_ID] == MAIN_SERVER
	end

	function DistributedQueuePrototype:IsActiveProcessor()
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		return properties[INDEX_SERVER_ID] ~= NON_PROCESSOR_SERVER
	end

	function DistributedQueuePrototype:SetMaxQueueProcessor(concurrency: number)
		local properties = getQueueProperties(self)
		if properties[INDEX_INITIALIZED] then error("Cannot change MaxQueueProcessor after initialization.", 2) end

		if type(concurrency) ~= "number" then error("Concurrency must be a number.", 2) end
		if concurrency ~= -1 and concurrency < 1 then error("Invalid Concurrency. Must be -1 or >= 1.", 2) end

		properties.MaxQueueProcessor = concurrency
	end

	function DistributedQueuePrototype:SetCanBeProcessor(canBeProcessor: boolean)
		local properties = getQueueProperties(self)
		if type(canBeProcessor) ~= "boolean" then error("Can be processor must be a boolean.", 2) end
		properties.CanBeProcessor = canBeProcessor

		if not canBeProcessor and properties[INDEX_SERVER_ID] ~= NON_PROCESSOR_SERVER then
			logMessage(`[{self.ClassName}][{self.InstanceId}] CanBeProcessor set to false. Releasing processor lock.`, INFO_SEVERITY)
			self:ReleaseLock(true)
		end
	end

	function DistributedQueuePrototype:ReleaseLock(immediateRelease: boolean?)
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		local currentServerType = properties[INDEX_SERVER_ID]

		if properties.MaxQueueProcessor == -1 then
			logMessage("[DistributedQueue] No limit on queue processors. Lock release is not applicable.", WARNING_SEVERITY, true)
			return
		end

		if currentServerType == NON_PROCESSOR_SERVER then
			logMessage("[DistributedQueue] This server is not an active processor. No lock to release.", WARNING_SEVERITY, true)
			return
		end

		stopUpdateThread(properties)
		properties[INDEX_SERVER_ID] = NON_PROCESSOR_SERVER

		if immediateRelease == false then
			logMessage(`[{self.ClassName}][{self.InstanceId}] Lock ownership lost locally. Waiting for session to expire in MemoryStore.`, INFO_SEVERITY)
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
				logMessage(`[{self.ClassName}][{instanceId}] Failed to remove main server lock from MemoryStore during release.`, MINOR_ISSUE_SEVERITY)
			end
		else -- currentServerType == PROCESSOR_SERVER
			logMessage(`[{self.ClassName}][{instanceId}] Releasing processor lock...`, INFO_SEVERITY)
			-- Remove the processor lock from MemoryStore.
			local success = retryAsync(memoryStore.RemoveAsync, memoryStore, ServerId)
			if success then
				logMessage(`[{self.ClassName}][{instanceId}] Processor lock released.`, INFO_SEVERITY)

				-- Notify other servers via MessagingService that this server has released its processor role.
				logMessage(`[{self.ClassName}][{instanceId}] Notifying other servers of processor slot availability., INFO_SEVERITY`)
				retryAsync(MessagingService.PublishAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, {
					Event = "ProcessorReleased",
					ServerId = ServerId,
					InstanceId = instanceId,
				})
			else
				logMessage(`[{self.ClassName}][{instanceId}] Failed to remove processor lock from MemoryStore during release.`, MINOR_ISSUE_SEVERITY)
			end
		end
	end

	function DistributedQueuePrototype:UpdateSession(): boolean
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap
		local serverLevel = properties[INDEX_SERVER_ID]
		local instanceId = properties.InstanceId

		local maxQueueProcessor = properties.MaxQueueProcessor
		if maxQueueProcessor == -1 then
			-- No limit on the number of servers processing this queue. This server will always process.
			return true
		end

		if serverLevel == MAIN_SERVER then
			-- This server is the main server. Attempt to refresh the lock.
			local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
				if not value or value == ServerId then return ServerId end
				return nil -- Abort update, another server took the lock.
			end, SESSION_TIMEOUT)

			if success and updateResult then return true end -- Refresh successful.

			logMessage(`[DistributedQueue][{instanceId}] Failed to refresh main server lock. Relinquishing role.`, IMPORTANT_INFO_SEVERITY)
			-- If the atomic update fails, this server must assume it has lost the lock.
			self:ReleaseLock(false)
			return false
		elseif serverLevel == PROCESSOR_SERVER then
			-- This server is a regular processor. Attempt to refresh the lock.
			local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, ServerId, function(value): boolean?
				if value then return true end -- Lock exists, refresh it.
				return nil -- Lock expired, abort refresh.
			end, SESSION_TIMEOUT)

			if success and updateResult then return true end -- Refresh successful.

			logMessage(`[DistributedQueue][{instanceId}] Session expired or failed to refresh. This server is no longer a processor.`, IMPORTANT_INFO_SEVERITY)
			self:ReleaseLock(success) -- If refresh failed due to network, don't publish release.
			return false
		elseif serverLevel == NON_PROCESSOR_SERVER then
			-- This server is not a processor. Check if it can become one.
			if not properties.CanBeProcessor then return true end -- Not eligible to be a processor.

			local getSizeSuccess, currentProcessorsAmount = retryAsync(memoryStore.GetSizeAsync, memoryStore)
			if not getSizeSuccess then
				logMessage(`[DistributedQueue][{instanceId}] Failed to get current processor count.`, MINOR_ISSUE_SEVERITY)
				return false
			end

			-- If there are no active processors, attempt to become the main processor.
			if currentProcessorsAmount == 0 then
				logMessage(`[DistributedQueue][{instanceId}] No active processors. Attempting to become main processor.`, INFO_SEVERITY)
				local success, updateResult = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
					if not value then return ServerId end -- No main server, claim it.
					return nil -- Another server claimed it first.
				end, SESSION_TIMEOUT)

				if success and updateResult then
					logMessage(`[DistributedQueue][{instanceId}] Successfully became the main processor.`, IMPORTANT_INFO_SEVERITY)
					properties[INDEX_SERVER_ID] = MAIN_SERVER
					createUpdateThread(properties)
					return true
				end
			end

			-- If capacity is full, do nothing.
			if currentProcessorsAmount >= maxQueueProcessor then return true end

			-- Request to become a processor.
			logMessage(`[DistributedQueue][{instanceId}] Requesting to become a processor...`, INFO_SEVERITY)
			retryAsync(MessagingService.PublishAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, {
				Event = "ProcessorRequest",
				InstanceId = instanceId,
				ServerId = ServerId,
			})
		end
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

		if properties.MaxQueueProcessor == -1 then
			-- No limit on processors. Every server can process.
			logMessage(`[DistributedQueue][{properties.InstanceId}] MaxQueueProcessor is -1. This server will process the queue.`, INFO_SEVERITY)
			properties[INDEX_SERVER_ID] = PROCESSOR_SERVER -- Mark as a processor, though no lock is held.
			createUpdateThread(properties)
			return true
		end

		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreSortedMap

		local instanceId = properties.InstanceId
		local processorCount: number

		-- Subscribe to messages for faster state changes.
		retryAsync(MessagingService.SubscribeAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, function(message)
			local messageData = message.Data
			if messageData.InstanceId ~= instanceId then return end -- Ignore messages for other instances.

			if messageData.Event == "ProcessorReleased" or messageData.Event == "MainServerReleased" then
				if not properties.CanBeProcessor or self:IsActiveProcessor() then return end
				-- A slot has opened up. Try to take it.
				logMessage(`[DistributedQueue][{instanceId}] Received release message. Checking for available processor slots.`, INFO_SEVERITY)
				self:UpdateSession()
			elseif messageData.Event == "CreateProcessor" then
				if messageData.ServerId ~= ServerId then return end
				-- This server's request was approved by the main server.
				logMessage(`[DistributedQueue][{instanceId}] Processor request approved. This server is now a processor.`, IMPORTANT_INFO_SEVERITY)
				properties[INDEX_SERVER_ID] = PROCESSOR_SERVER
				createUpdateThread(properties)
			elseif messageData.Event == "ProcessorRequest" then
				-- Another server is requesting to become a processor. Only the main server can act on this.
				if not self:IsMainServer() then return end

				-- This block ensures processorCount is initialized atomically and serves as a source of truth.
				-- As the main server is the only one creating processors, this count is reliable.
				if not processorCount then
					logMessage(`[DistributedQueue][{instanceId}] Main server is initializing local processor count...`, INFO_SEVERITY)
					local getSizeSuccess, currentCount = retryAsync(memoryStore.GetSizeAsync, memoryStore)
					if not getSizeSuccess then
						logMessage(`[DistributedQueue][{instanceId}] Failed to get current processor count to handle request.`, MINOR_ISSUE_SEVERITY)
						return
					end
					processorCount = currentCount
					logMessage(`[DistributedQueue][{instanceId}] Local processor count initialized to {processorCount}.`, INFO_SEVERITY)
				end

				if processorCount >= properties.MaxQueueProcessor then
					logMessage(`[DistributedQueue][{instanceId}] Processor request denied for {messageData.ServerId}. At capacity.`, WARNING_SEVERITY)
					return
				end

				-- Reserve a slot and approve the request.
				processorCount = processorCount + 1
				logMessage(
					`[DistributedQueue][{instanceId}] Approving processor request for {messageData.ServerId}. New count: {processorCount}`,
					INFO_SEVERITY
				)

				local targetServerId = messageData.ServerId
				local createProcessorSuccess = createProcessor(memoryStore, targetServerId, instanceId)

				if createProcessorSuccess then
					retryAsync(MessagingService.PublishAsync, MessagingService, DISTRIBUTED_QUEUE_CHANGES_TOPIC, {
						Event = "CreateProcessor",
						ServerId = targetServerId,
						InstanceId = instanceId,
					})
				else
					-- If creating the processor fails, decrement the count.
					processorCount = processorCount - 1
					logMessage(
						`[DistributedQueue][{instanceId}] Failed to create processor for {targetServerId}. Reverting count to {processorCount}.`,
						MINOR_ISSUE_SEVERITY
					)
				end
			end
		end)

		-- Immediately try to become a processor on initialization.
		self:UpdateSession()
		-- Start a persistent thread to periodically refresh the session lock.
		table.insert(ActiveSessionQueues, self)

		logMessage(`[DistributedQueue][{instanceId}] Initialized successfully.`, INFO_SEVERITY)
		return true
	end

	Constructor.CreateDistributedQueue = function<T>(instanceId: string?): DistributedQueue<T>
		local proxy, id, properties = createBaseQueue(instanceId, "DistributedQueue")
		if not properties then return proxy :: DistributedQueue<T> end

		properties[INDEX_PROTOTYPE] = DistributedQueuePrototype
		properties[INDEX_CALLBACKS] = {} :: Callbacks<T>
		properties[INDEX_MEMORY_STORE_LOCK] = MemoryStoreService:GetSortedMap(`DISTRIBUTED_QUEUE_{UNIQUE_KEY}_Lock_{id}`)
		properties[INDEX_MEMORY_STORE_QUEUE] = MemoryStoreService:GetQueue(`DISTRIBUTED_QUEUE_{UNIQUE_KEY}_QUEUE_{id}`)
		properties[INDEX_SERVER_ID] = NON_PROCESSOR_SERVER -- Start as a non-processor

		properties.MaxQueueProcessor = DEFAULT_MAX_QUEUE_PROCESSOR
		properties.CanBeProcessor = DEFAULT_CAN_BE_PROCESSOR

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

	function CentralizedQueuePrototype:GetMainServerId(): string?
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)
		return properties[INDEX_SERVER_ID]
	end

	function CentralizedQueuePrototype:IsMainServer(): boolean
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)
		return properties[INDEX_SERVER_ID] == ServerId
	end

	function CentralizedQueuePrototype:ReleaseLock(immediateRelease: boolean?)
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		if properties[INDEX_SERVER_ID] ~= ServerId then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Attempted to release lock, but this is not the main server.`, WARNING_SEVERITY, true)
			return
		end

		stopUpdateThread(properties)
		properties[INDEX_SERVER_ID] = nil

		if immediateRelease == false then
			logMessage(`[CentralizedQueue][{properties.InstanceId}] Lock ownership lost locally. Waiting for session to expire in MemoryStore.`, INFO_SEVERITY)
			return
		end

		logMessage(`[CentralizedQueue][{properties.InstanceId}] Releasing main server lock...`, INFO_SEVERITY)
		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreHashMap
		local instanceId = properties.InstanceId
		local success, err = retryAsync(memoryStore.RemoveAsync, memoryStore, "MainServer")
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
			logMessage(`[CentralizedQueue][{instanceId}] Failed to remove main server lock from MemoryStore during release: {err}`, MINOR_ISSUE_SEVERITY)
		end
	end

	-- Attempts to claim or refresh the main server lock using a transactional update.
	-- This function is the core of the main server election process.
	function CentralizedQueuePrototype:UpdateSession(): boolean
		local properties = getQueueProperties(self)
		checkInit(properties[INDEX_INITIALIZED], 2)

		local memoryStore = properties[INDEX_MEMORY_STORE_LOCK] :: MemoryStoreHashMap
		local instanceId = properties.InstanceId
		if not properties.CanBeMainServer then return true end -- Not eligible to be main server.

		local currentMainServerId: string

		-- UpdateAsync provides an atomic way to read and write the "MainServer" key.
		local success, reason = retryAsync(memoryStore.UpdateAsync, memoryStore, "MainServer", function(value): string?
			-- If no server is main, or if this server is already main, claim/refresh the lock.
			if not value or value == ServerId then
				currentMainServerId = ServerId
				return ServerId
			end
			-- Otherwise, another server holds the lock. Do not change it.
			currentMainServerId = value
			return nil -- Abort the update operation.
		end, SESSION_TIMEOUT)

		local wasMainServer = self:IsMainServer()

		if not success then
			logMessage(`[CentralizedQueue][{instanceId}] Failed to update session lock: {reason}`, MINOR_ISSUE_SEVERITY)
			if wasMainServer then self:ReleaseLock(false) end
			return false
		end

		-- Update local state based on the outcome.
		properties[INDEX_SERVER_ID] = currentMainServerId
		local isMainServer = self:IsMainServer()

		if isMainServer and not wasMainServer then
			logMessage(`[CentralizedQueue][{instanceId}] This server has become the main server.`, IMPORTANT_INFO_SEVERITY)
			createUpdateThread(properties)
		elseif not isMainServer and wasMainServer then
			logMessage(`[CentralizedQueue][{instanceId}] This server has lost its main server status.`, IMPORTANT_INFO_SEVERITY)
			stopUpdateThread(properties)
		end

		return true
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
		retryAsync(MessagingService.SubscribeAsync, MessagingService, CENTRALIZED_QUEUE_CHANGES_TOPIC, function(message)
			local messageData = message.Data
			if messageData.InstanceId ~= instanceId then return end
			if messageData.Event == "MainServerReleased" then
				if messageData.PreviousServer == ServerId then return end
				logMessage(`[CentralizedQueue][{instanceId}] Received release message. Attempting to become main server.`, INFO_SEVERITY)
				self:UpdateSession()
			end
		end)

		-- On initialization, immediately try to become the main server.
		self:UpdateSession()
		-- Start a persistent thread to periodically refresh the session lock.
		table.insert(ActiveSessionQueues, self)

		logMessage(`[CentralizedQueue][{instanceId}] Initialized successfully.`, INFO_SEVERITY)
		return true
	end

	Constructor.CreateCentralizedQueue = function<T>(instanceId: string?): CentralizedQueue<T>
		local proxy, id, properties = createBaseQueue(instanceId, "CentralizedQueue")
		if not properties then return proxy :: CentralizedQueue<T> end

		properties[INDEX_PROTOTYPE] = CentralizedQueuePrototype
		properties[INDEX_CALLBACKS] = {} :: Callbacks<T>
		properties[INDEX_MEMORY_STORE_LOCK] = MemoryStoreService:GetHashMap(`CENTRALIZED_QUEUE_{UNIQUE_KEY}_Lock_{id}`)
		properties[INDEX_MEMORY_STORE_QUEUE] = MemoryStoreService:GetQueue(`CENTRALIZED_QUEUE_{UNIQUE_KEY}_QUEUE_{id}`)

		return proxy :: CentralizedQueue<T>
	end
end

--// Global Update & Shutdown Hooks \\--

-- This background thread periodically calls UpdateSession for all active queues
-- to refresh locks and handle state transitions.
task.spawn(function()
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

	local threads = {}
	for _, queue: BaseQueue<any, any> in ActiveSessionQueues do
		table.insert(threads, task.spawn(queue.Shutdown, queue))
	end

	-- Wait for all shutdown threads to complete.
	for _, thread in threads do
		coroutine.yield(thread)
	end

	logMessage("[CentralizedServer] All queues have been shut down.", INFO_SEVERITY)
end)

--// Constructor \\--

Constructor.SetRetryableErrors = function(errors: { [string]: boolean })
	-- Set the retryable errors for the retryAsync function.
	if type(errors) ~= "table" then error("Expected a table of retryable errors.", 2) end
	if getmetatable(errors :: any) ~= nil then error("Retryable errors must be a plain table without metatable.", 2) end

	for key, value in errors do
		if type(key) ~= "string" or type(value) ~= "boolean" then
			error("Retryable errors must be a table of string keys and boolean values.", 2)
		end
	end

	RetryableNetworkErrors = errors
	logMessage("[Constructor] Retryable errors set successfully.", INFO_SEVERITY)
end

Constructor.SurpressLogLevel = function(level: number)
	-- Suppress logging for the specified severity level.
	if type(level) ~= "number" then error("Log level must be a number.", 2) end
	if level < 1 or level > 5 or level // 1 ~= level then error("Log level must an integer between 1 and 5.", 2) end

	SurpressedLogLevels[level] = true
	logMessage(`[Constructor] Log level {level} is now suppressed.`, INFO_SEVERITY)
end

--// Return \\--

return table.freeze(Constructor)
