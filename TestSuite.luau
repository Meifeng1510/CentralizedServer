--[[
    Test Suite for Centralized & Distributed Queue System
    -----------------------------------------------------
    Description:
    This script contains a series of tests for the Queue System API.

    The tests are organized into suites for the CentralizedQueue and
    the DistributedQueue, covering features like:
    - Initialization and shutdown
    - Server/processor election and lock management
    - Adding and processing queue items
    - Callback functionality
    - Dynamic configuration setters
]]

local CentralizedServerModule = script:FindFirstChild("CentralizedServer")
if not CentralizedServerModule then
	error("CentralizedServer module not found. Please ensure it is in the correct location.")
end

--// TEST FRAMEWORK

local function describe(suite_name, fn)
	print(string.format("\n--- Running Suite: %s ---", suite_name))
	fn()
end

local function it(test_name, fn)
	local success, err = pcall(fn)
	if success then
		print(string.format("  [PASS] %s", test_name))
	else
		print(string.format("  [FAIL] %s\n    ERROR: %s", test_name, tostring(err)))
	end
end

local function assert_equal(a, b, message)
	if a ~= b then error(string.format("%s: Expected %s, got %s", message or "Assertion failed", tostring(b), tostring(a))) end
end

local function assert_true(val, message)
	assert_equal(val, true, message or "Expected value to be true")
end

local function assert_false(val, message)
	assert_equal(val, false, message or "Expected value to be false")
end

--// TEST SUITES

describe("CentralizedQueue", function()
	it("should initialize and elect a main server", function()
		local CentralizedServer = require(CentralizedServerModule:Clone())
		CentralizedServer.SetServerId("server1")
		local cq = CentralizedServer.CreateCentralizedQueue("test-central")
		cq:SetLifecycleAutomation(false)
		cq:Init()

		-- Simulate the update loop
		cq:UpdateSession()

		assert_true(cq:IsMainServer(), "Server 1 should be elected as the main server")
		assert_equal(cq:GetMainServerId(), "server1", "Main server ID should be 'server1'")

		cq:Shutdown()
	end)

	it("should prevent a second server from becoming main server", function()
		-- Server 1 becomes main
		local CentralizedServer1 = require(CentralizedServerModule:Clone())
		CentralizedServer1.SetServerId("server1")
		local cq1 = CentralizedServer1.CreateCentralizedQueue("test-central-2")
		cq1:SetLifecycleAutomation(false)
		cq1:Init()
		cq1:UpdateSession()
		assert_true(cq1:IsMainServer(), "Server 1 should be main")

		-- Server 2 tries to become main
		local CentralizedServer2 = require(CentralizedServerModule:Clone())
		CentralizedServer2.SetServerId("server2")
		local cq2 = CentralizedServer2.CreateCentralizedQueue("test-central-2")
		cq2:SetLifecycleAutomation(false)
		cq2:Init()
		cq2:UpdateSession()
		assert_false(cq2:IsMainServer(), "Server 2 should not be main")

		print(cq2:GetMainServerId())
		assert_equal(cq2:GetMainServerId(), "server1", "Server 2 should see Server 1 as main")

		cq1:Shutdown()
		cq2:Shutdown()
	end)

	it("should add and process items from the queue", function()
		local processed_value = nil
		local callback_fired = false

		local CentralizedServer = require(CentralizedServerModule:Clone())
		CentralizedServer.SetServerId("server-proc")
		local cq = CentralizedServer.CreateCentralizedQueue("test-proc")
		cq:SetLifecycleAutomation(false)
		cq:Init()
		cq:UpdateSession() -- Become main server

		cq:AddCallback(function(value)
			processed_value = value
			callback_fired = true
		end)

		local item_to_queue = { message = "hello world", data = 123 }
		cq:AddToQueue(item_to_queue)

		-- Check queue size before processing
		local size = cq:GetQueueSize()
		assert_equal(size, 1, "Queue size should be 1 after adding an item")

		cq:ProcessQueue() -- Process the item

		assert_true(callback_fired, "Callback should have been fired")
		assert_equal(processed_value.message, item_to_queue.message, "Processed value message should match")
		assert_equal(processed_value.data, item_to_queue.data, "Processed value data should match")

		cq:Shutdown()
	end)

	it("should respect CanBeMainServer setting", function()
		local CentralizedServer = require(CentralizedServerModule:Clone())
		CentralizedServer.SetServerId("server-canset")
		local cq = CentralizedServer.CreateCentralizedQueue("test-canset")
		cq:SetLifecycleAutomation(false)
		cq:Init()
		cq:UpdateSession()
		assert_true(cq:IsMainServer(), "Should initially be main server")

		cq:SetCanBeMainServer(false)
		assert_false(cq:IsMainServer(), "Should release main server lock after SetCanBeMainServer(false)")

		cq:Shutdown()
	end)
end)

describe("DistributedQueue", function()
	it("should initialize and elect processors based on MaxQueueProcessor", function()
		local MAX_PROC = 2
		local servers = {}

		-- Create 3 servers
		for i = 1, 3 do
			local CentralizedServer = require(CentralizedServerModule:Clone())
			CentralizedServer.SetServerId("dist-server-" .. i)
			servers[i] = CentralizedServer.CreateDistributedQueue("test-dist-1")
			servers[i]:SetLifecycleAutomation(false)
			servers[i]:SetMaxQueueProcessor(MAX_PROC)
			servers[i]:Init()
			servers[i]:UpdateSession() -- Attempt to become a processor
		end

		assert_equal(servers[1]:GetActiveProcessorCount(), 1, "Should not elect any new servers")
		print(servers[1]:GetActiveProcessorList())
		servers[1]:UpdateProcessorDistribution()
		print(servers[1]:GetActiveProcessorList())

		local active_processors = servers[1]:GetActiveProcessorCount()
		assert_equal(active_processors, MAX_PROC, "Should elect exactly MaxQueueProcessor servers")

		local processor_count = 0
		for _, server in ipairs(servers) do
			if server:IsActiveProcessor() then processor_count = processor_count + 1 end
		end
		assert_equal(processor_count, MAX_PROC, "Exactly two servers should report as active processors")

		for _, server in ipairs(servers) do
			server:Shutdown()
		end
	end)

	it("should elect processors based on CandidacyPriority", function()
		local servers = {}
		-- Server 3 will have the highest priority (higher number)
		local priorities = { 10, 5, 15 }

		for i = 1, 3 do
			local CentralizedServer = require(CentralizedServerModule:Clone())
			CentralizedServer.SetServerId("dist-server-priority-" .. i)
			servers[i] = CentralizedServer.CreateDistributedQueue("test-dist-priority")
			servers[i]:SetLifecycleAutomation(false)
			servers[i]:SetMaxQueueProcessor(2)
			servers[i]:SetProcessorCandidacyPriority(priorities[i])
			servers[i]:Init()
			servers[i]:UpdateSession()
		end

		-- Main server updates distribution, which considers priority
		for _, s in ipairs(servers) do
			if s:IsMainServer() then
				s:UpdateProcessorDistribution()
				break
			end
		end
		for _, s in ipairs(servers) do
			s:UpdateSession()
		end

		assert_true(servers[1]:IsMainServer(), "Server with priority 10 should be a main server (first session update)")
		assert_true(servers[3]:IsActiveProcessor(), "Server with highest priority (15) should be a processor")
		assert_false(servers[2]:IsActiveProcessor(), "Server with lowest priority (5) should not be a processor")

		for _, server in ipairs(servers) do
			server:Shutdown()
		end
	end)

	it("should allow multiple processors to process items", function()
		local processed_count = 0
		local callback = function(_)
			processed_count = processed_count + 1
		end
		local servers = {}

		-- Create 2 servers
		for i = 1, 2 do
			local CentralizedServer = require(CentralizedServerModule:Clone())
			CentralizedServer.SetServerId("dist-proc-" .. i)
			servers[i] = CentralizedServer.CreateDistributedQueue("test-dist-proc")
			servers[i]:SetLifecycleAutomation(false)
			servers[i]:SetMaxQueueProcessor(2)
			servers[i]:SetMaxReadPerInterval(1)
			servers[i]:Init()
			servers[i]:AddCallback(callback)
			servers[i]:UpdateSession()
		end

		for i = 1, servers[1]:GetQueueSize() do
			servers[1]:ProcessQueue() -- clearing out queue
		end

		processed_count = 0
		servers[1]:UpdateProcessorDistribution() -- Both become processors

		assert_equal(servers[1]:GetQueueSize(), 0, "Queue should contain 0 items")

		-- Both servers add an item to the queue
		servers[1]:AddToQueue({ task = 1 })
		servers[2]:AddToQueue({ task = 2 })

		assert_equal(servers[1]:GetQueueSize(), 2, "Queue should contain 2 items")

		-- Both servers attempt to process the queue
		servers[1]:ProcessQueue()

		assert_equal(servers[1]:GetQueueSize(), 1, "Queue should contain 1 items")

		servers[2]:ProcessQueue()

		assert_equal(processed_count, 2, "Both items should have been processed by the two servers")

		for _, server in ipairs(servers) do
			server:Shutdown()
		end
	end)

	it("should respect CanBeProcessor setting", function()
		local CentralizedServer = require(CentralizedServerModule:Clone())
		CentralizedServer.SetServerId("dist-canset-proc")
		local dq = CentralizedServer.CreateDistributedQueue("test-dist-canset")
		dq:SetLifecycleAutomation(false)
		dq:SetMaxQueueProcessor(1)
		dq:Init()
		dq:UpdateSession()
		assert_true(dq:IsActiveProcessor(), "Should initially be an active processor")

		dq:SetCanBeProcessor(false)
		assert_false(dq:IsActiveProcessor(), "Should release processor lock after SetCanBeProcessor(false)")

		dq:Shutdown()
	end)
end)
