--!strict

-- Made by your lovely @meifeng_ft

--[[
	Centralized Server & Distributed Queue System
	---------------------------------------------
	Author: @meifeng_ft
	Date: 2025-07-02 (Last Updated)

	Description:
	This module provides a robust solution for managing a single "main" server
	within a Roblox experience and a distributed queue for processing tasks.
	It leverages Roblox's MemoryStoreService for server election (locking) and
	queue management, and MessagingService for faster main server failover.

	Features:
	- **Main Server Election**: Automatically elects and maintains a single main server
	  responsible for processing queued tasks.
	- **Distributed Queue**: Allows any server to queue values, which are then processed
	  by the main server.
	- **Fault Tolerance**: Implements retry mechanisms with exponential backoff for
	  MemoryStore operations, enhancing resilience against transient network issues.
	- **Graceful Shutdown**: Ensures the main server lock is released upon server shutdown,
	  allowing another server to quickly take over.
	- **Callbacks**: Provides a mechanism for custom logic to be executed on the main server
	  when a queued item is processed.

	Important Notes:
	- Relies heavily on `MemoryStoreService` for coordination and state management.
	- `MessagingService` is optional but recommended for rapid main server recovery.
	- Designed for scenarios where a single, authoritative server is needed to handle
	  specific tasks (e.g., global events, data saving, etc.).
	- `INVISIBILITY_TIMEOUT` and retry logic assertions are critical for preventing
	  duplicate processing of queue items.
]]

--// Types \\--

type CentralizedServer = {
	--// Public Functions \\--

	-- Queues a value to be processed by the main server. If the current server is the main server,
	-- it processes the value directly. Otherwise, it adds the value to a shared MemoryStoreQueue.
	QueueValue: <T>(value: T, priority: number?) -> (boolean, string?),

	-- Retrieves the JobId of the server currently designated as the main server.
	-- Returns nil if no main server is currently elected.
	GetMainServerId: () -> string?,

	-- Registers a function to be executed on the main server whenever a value is processed from the queue.
	-- This allows for custom logic to be triggered by queued items.
	-- @returns A 'disconnect' function that, when called, will remove the callback from the list,
	-- preventing it from being executed in the future.
	AddMainServerCallback: <T>(callback: (value: T) -> ()) -> (() -> ()),

	-- Returns true if the current server instance holds the main server lock, false otherwise.
	IsMainServer: () -> boolean,

	-- Voluntarily relinquishes the main server role, allowing another server instance to take over.
	-- This should be called during graceful shutdown of the main server.
	ReleaseServer: () -> (),

	-- Sets up the CentralizedServer, starts the main server election process,
	-- and initializes the necessary background threads for session management.
	Init: () -> boolean,
}

--// Services \\--

local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")

--// Variables \\--

-- A unique identifier to prevent conflicts with other MemoryStore keys used in the experience.
local UNIQUE_KEY = "54fe96ac-e121-4eb9-a4c6-d2c22f1ffde4"
-- The duration (in seconds) that items will persist in the queue before expiring. (Default: 24 hours)
local QUEUE_EXPIRATION = 86400
-- The duration (in seconds) that an item read from the queue is hidden from other read attempts.
-- This prevents the same item from being processed by multiple servers simultaneously.
local INVISIBILITY_TIMEOUT = 30
-- The delay (in seconds) for the main server's loop that processes items from the queue.
local QUEUE_UPDATE_INTERVAL = 1

-- An estimate of the maximum time it could take to remove an item from the queue after processing.
-- Used in assertion checks to ensure system stability.
local AVERAGE_MAX_ITEM_REMOVAL_TIME = 0.5

-- The invisibility timeout must be longer than the maximum time it takes to process and remove an item.
-- This is a critical safeguard against race conditions and duplicate processing.

-- The time (in seconds) the main server lock is held before it is no longer valid.
local SESSION_TIMEOUT = 30
-- The interval (in seconds) at which the main server refreshes its lock. This must be less than SESSION_TIMEOUT.
local SESSION_CHECK_INTERVAL = 12

-- Ensures the session refresh happens before the lock expires, preventing accidental loss of main server status.
assert(SESSION_CHECK_INTERVAL < SESSION_TIMEOUT, "SESSION_CHECK_INTERVAL must be less than SESSION_TIMEOUT")

-- The maximum number of times to retry a failing MemoryStore operation.
local MAX_ATTEMPTS = 5
-- The initial wait time (in seconds) before the first retry.
local RETRY_INTERVAL = 0.5
-- The multiplier for increasing the wait time between retries, creating an exponential backoff.
local EXPONENTIAL_FALLBACK = 2

-- This assertion verifies that the system's retry logic and processing time won't exceed the invisibility timeout,
-- which is crucial for preventing data processing errors e.g., if the queue is read multiple times before an item is removed.
assert(
	RETRY_INTERVAL * EXPONENTIAL_FALLBACK ^ MAX_ATTEMPTS
	- RETRY_INTERVAL -- Maximum retry duration
		+ AVERAGE_MAX_ITEM_REMOVAL_TIME * MAX_ATTEMPTS
		< INVISIBILITY_TIMEOUT,
	"INVISIBILITY_TIMEOUT must be greater than the maximum retry duration plus the average item removal time."
)

-- The number of items to retrieve from the queue in a single read operation.
local DEFAULT_READ_COUNT = 100
-- The default priority for items added to the queue if not specified.
local DEFAULT_QUEUE_PRIORITY = 0

-- A list of specific error messages that signify transient network issues, which are safe to retry.
local NETWORK_ERROR_LIST = {}

-- The unique identifier for the current server instance.
local ServerId = game.JobId
-- Stores the ID of the main server. It's updated through the session management logic.
local MainServerId: string? = nil

-- A boolean flag indicating if this server instance is the main one.
local IsMainServer = false
-- A unique topic name for MessagingService to broadcast and listen for server status changes.
local ServerChangesTopic = "CentralizedServer_ServerChangesTopic_" .. UNIQUE_KEY

-- A table to hold callback functions that will be executed by the main server upon processing a queued item.
local Callbacks: { (any) -> () } = {}

-- Threads for handling queue processing and session refreshing.
local UpdateThread: thread? = nil
local SessionThread: thread? = nil

local Initialized = false

--// Memory Store \\--

-- A HashMap used to store the lock key for the main server election.
local CentralizedMemoryStore = MemoryStoreService:GetHashMap("CentralizedServer_" .. UNIQUE_KEY)
-- A Queue used to store values that need to be processed by the main server.
local CentralizedQueue = MemoryStoreService:GetQueue("CentralizedQueue_" .. UNIQUE_KEY, INVISIBILITY_TIMEOUT)

--// Class \\--

local CentralizedServer = {} :: CentralizedServer
Initialized = false

--// Private Functions \\--

-- A utility function to safely execute a function within a coroutine and yield its results.
-- This is used by retryAsync to handle pcall results without breaking the main thread.
local function captureResult<T...>(boolean, ...: T...): T...
	coroutine.yield(boolean)
	coroutine.yield(...)
end

-- A robust wrapper for MemoryStore operations that automatically retries on failure.
-- It employs an exponential backoff strategy to avoid overwhelming the service during outages.
local function retryAsync<T..., R...>(func: (T...) -> R..., ...: T...): (boolean, R...)
	for retryAttempt = 1, MAX_ATTEMPTS do
		local thread = coroutine.create(captureResult)

		-- Execute the function in a protected call to catch errors.
		local _, success = coroutine.resume(thread, pcall(func, ...))
		if success then
			return coroutine.resume(thread) -- Return the successful result.
		end

		local _, errorMsg: any = coroutine.resume(thread)
		-- Decide if the error is retryable based on the NETWORK_ERROR_LIST.
		local shouldRetry = not (NETWORK_ERROR_LIST and next(NETWORK_ERROR_LIST) ~= nil)

		if not shouldRetry then
			for errorCode in NETWORK_ERROR_LIST do
				if errorMsg:find(errorCode) then
					shouldRetry = true
					break
				end
			end

			if not shouldRetry then
				print(`Network operation failed: {errorMsg}`)
				return false, errorMsg -- Non-retryable error.
			end
		end

		warn(`Network operation failed, attempting ({retryAttempt}/{MAX_ATTEMPTS}): {errorMsg}`)
		-- Wait longer before the next attempt.
		task.wait(RETRY_INTERVAL * retryAttempt ^ EXPONENTIAL_FALLBACK)
	end

	-- If all attempts fail, return the result of the final attempt.
	return pcall(func, ...)
end

-- Checks if the module has been initialized before allowing public functions to be called.
local function checkInit(level: number?)
	local currentLevel = level or 1
	if not Initialized then
		error("CentralizedServer is not initialized. Call CentralizedServer.Init() first.", currentLevel + 1)
	end
end

-- Starts the background thread responsible for processing items from the queue.
-- This is only ever called on the main server.
local function startUpdateThread()
	if UpdateThread then
		return
	end -- Avoid starting multiple threads.
	if not IsMainServer then
		warn("[CentralizedServer] Attempted to start update thread, but this is not the main server.")
		return
	end

	UpdateThread = task.spawn(function()
		print("[CentralizedServer] This server is now the main server. Starting queue processing.")
		repeat
			-- Read a batch of items from the queue.
			local success, items, id: any =
				retryAsync(CentralizedQueue.ReadAsync, CentralizedQueue, DEFAULT_READ_COUNT, false, 0)

			if success and items and #items > 0 then
				-- If read is successful, permanently remove the items to prevent reprocessing.
				local removeSuccess = retryAsync(CentralizedQueue.RemoveAsync, CentralizedQueue, id)
				if removeSuccess then
					-- Execute all registered callbacks for each processed item.
					for _, item in items do
						for _, callback in Callbacks do
							task.spawn(callback, item)
						end
					end
				else
					print("[CentralizedServer] Failed to remove items from queue after reading.")
				end
			elseif not success then
				print(`[CentralizedServer] Failed to read from queue: {items}`)
			end

			task.wait(QUEUE_UPDATE_INTERVAL) -- Wait before processing the next batch.
		until not IsMainServer -- Stop the loop if this server loses main server status.
	end)
end

-- Stops the queue processing thread. Called when this server is no longer the main server.
local function stopUpdateThread()
	if UpdateThread then
		task.cancel(UpdateThread)
		UpdateThread = nil
		print("[CentralizedServer] This server is no longer the main server. Stopping queue processing.")
	end
end

-- Attempts to claim or refresh the main server lock using a transactional update.
-- This function is the core of the main server election process.
local function updateSession(): boolean
	local currentMainServerId: string

	-- UpdateAsync provides an atomic way to read and write the "MainServer" key.
	local success = retryAsync(
		CentralizedMemoryStore.UpdateAsync,
		CentralizedMemoryStore,
		"MainServer",
		function(value): string?
			-- If no server is main, or if this server is already the main server, claim/refresh the lock.
			if not value or value == ServerId then
				currentMainServerId = ServerId
				return ServerId
			end
			-- Otherwise, another server holds the lock. Do not change it.
			currentMainServerId = value
			return nil -- Abort the update operation.
		end,
		SESSION_TIMEOUT
	)

	if not success then
		-- If the atomic update fails, this server must assume it has lost the lock as a safety measure.
		if IsMainServer then
			MainServerId = nil
			IsMainServer = false
			stopUpdateThread()
		end
		print("[CentralizedServer] Failed to update session lock.")
		return false
	end

	-- Update local state based on the outcome of the session update.
	MainServerId = currentMainServerId
	local wasMainServer = IsMainServer
	IsMainServer = (currentMainServerId == ServerId)

	if IsMainServer and not wasMainServer then
		-- This server has just become the main server.
		startUpdateThread()
	elseif not IsMainServer and wasMainServer then
		-- This server has just lost its main server status.
		stopUpdateThread()
	end

	return true
end

-- Gracefully shuts down the server's roles, releasing the main server lock if held.
local function shutdown()
	if IsMainServer and Initialized then
		CentralizedServer.ReleaseServer()
	end
	-- Clean up background threads.
	if SessionThread then
		task.cancel(SessionThread)
	end
	if UpdateThread then
		task.cancel(UpdateThread)
	end
end

--// Public Functions \\--

function CentralizedServer.QueueValue(value: any, priority: number?)
	checkInit(2)
	-- If this is the main server, process the value immediately instead of sending it through the queue.
	if IsMainServer then
		for _, callback in Callbacks do
			task.spawn(callback, value)
		end
		return true
	else
		-- Otherwise, add the value to the distributed queue for the main server to process.
		return retryAsync(
			CentralizedQueue.AddAsync,
			CentralizedQueue,
			value,
			QUEUE_EXPIRATION,
			priority or DEFAULT_QUEUE_PRIORITY
		)
	end
end

function CentralizedServer.GetMainServerId()
	checkInit(2)
	return MainServerId
end

function CentralizedServer.IsMainServer()
	checkInit(2)
	return IsMainServer
end

function CentralizedServer.AddMainServerCallback(callback: (value: any) -> ())
	if type(callback) ~= "function" then
		error("Callback must be a function.", 2)
	end
	table.insert(Callbacks, callback)

	-- Create and return a function that can be used to disconnect the callback.
	local disconnected = false
	return function()
		if disconnected then
			return -- Prevent disconnecting more than once.
		end
		disconnected = true

		-- Find and remove the callback from the list.
		local index = table.find(Callbacks, callback)
		if index then
			table.remove(Callbacks, index)
		end
	end
end

function CentralizedServer.ReleaseServer()
	checkInit(2)
	if not IsMainServer then
		warn("[CentralizedServer] Attempted to release server, but this is not the main server.")
		return
	end

	stopUpdateThread()
	MainServerId = nil
	IsMainServer = false

	-- Immediately remove the lock from MemoryStore instead of waiting for it to expire.
	local success = retryAsync(CentralizedMemoryStore.RemoveAsync, CentralizedMemoryStore, "MainServer")
	if success then
		-- Notify other servers via MessagingService that they can now attempt to claim the main server role.
		retryAsync(MessagingService.PublishAsync, MessagingService, ServerChangesTopic, "MainServerReleased")
	end

	print("[CentralizedServer] Main server lock released.")
end

function CentralizedServer.Init()
	if Initialized then
		warn("CentralizedServer is already initialized.")
		return false
	end
	Initialized = true

	-- On initialization, immediately try to become the main server.
	updateSession()

	-- Subscribe to messages indicating the main server lock has been released.
	-- This allows for a faster takeover than waiting for the session lock to expire.
	retryAsync(MessagingService.SubscribeAsync, MessagingService, ServerChangesTopic, function(message)
		if message.Data == "MainServerReleased" and not IsMainServer then
			print("[CentralizedServer] Received release message. Attempting to become main server.")
			updateSession()
		end
	end)

	-- Start a persistent thread to periodically refresh the session lock.
	SessionThread = task.spawn(function()
		while true do
			task.wait(SESSION_CHECK_INTERVAL)
			updateSession()
		end
	end)

	print("[CentralizedServer] Initialized successfully.")
	return true
end

--// Connections \\--

-- Ensure graceful shutdown and lock release when the server instance is shut down.
game:BindToClose(shutdown)

--// Return \\--

-- Freeze the public table to prevent accidental modification.
return table.freeze(CentralizedServer)
