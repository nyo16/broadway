defmodule Broadway.Topology.ScaleProcessorsTest do
  use ExUnit.Case, async: true

  alias Broadway.{Message, CallerAcknowledger}

  defmodule ManualProducer do
    @behaviour Broadway.Producer
    use GenStage

    @impl true
    def init(%{test_pid: test_pid} = state) do
      send(test_pid, {:producer_initialized, self()})
      {:producer, state}
    end

    def init(_args), do: {:producer, %{}}

    @impl true
    def handle_demand(_demand, state), do: {:noreply, [], state}

    @impl true
    def handle_info({:push_messages, messages}, state) do
      {:noreply, messages, state}
    end
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, self()})
      message
    end

    def handle_batch(batcher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, batcher, messages})
      messages
    end
  end

  defmodule ForwarderWithoutBatchers do
    use Broadway

    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, self()})
      message
    end
  end

  defmodule ForwarderWithPartitionBy do
    use Broadway

    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, self()})
      message
    end

    def handle_batch(batcher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, batcher, messages})
      messages
    end
  end

  describe "scale_processors/4 with DemandDispatcher (no partition_by)" do
    test "scales up processors dynamically" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      assert get_processor_count(broadway) == 2
      assert count_processor_children(broadway) == 2

      # Scale up
      assert :ok = Broadway.Topology.scale_processors(broadway, :default, 4)

      assert get_processor_count(broadway) == 4
      assert count_processor_children(broadway) == 4

      # Verify all processor processes exist
      for i <- 0..3 do
        assert Process.whereis(get_processor_name(broadway, i)) != nil
      end
    end

    test "scales down processors with graceful draining" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 4)

      assert get_processor_count(broadway) == 4
      assert count_processor_children(broadway) == 4

      # Scale down with short drain timeout for tests
      assert :ok = Broadway.Topology.scale_processors(broadway, :default, 2, drain_timeout: 100)

      assert get_processor_count(broadway) == 2
      assert count_processor_children(broadway) == 2

      # Verify removed processor processes don't exist
      assert Process.whereis(get_processor_name(broadway, 2)) == nil
      assert Process.whereis(get_processor_name(broadway, 3)) == nil

      # Verify remaining processors still exist
      assert Process.whereis(get_processor_name(broadway, 0)) != nil
      assert Process.whereis(get_processor_name(broadway, 1)) != nil
    end

    test "scales down with immediate strategy" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 4)

      assert :ok =
               Broadway.Topology.scale_processors(broadway, :default, 2,
                 strategy: :immediate,
                 drain_timeout: 0
               )

      assert get_processor_count(broadway) == 2
      assert count_processor_children(broadway) == 2
    end

    test "returns error when count unchanged" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      assert {:error, :no_change_required} =
               Broadway.Topology.scale_processors(broadway, :default, 2)
    end

    test "returns error for invalid processor key" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      assert {:error, :processor_not_found} =
               Broadway.Topology.scale_processors(broadway, :nonexistent, 4)
    end

    test "new processors automatically subscribe to producers" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      # Scale up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)

      # Give time for subscriptions to establish
      Process.sleep(100)

      # Verify all 4 processors exist and are alive
      for i <- 0..3 do
        pid = Process.whereis(get_processor_name(broadway, i))
        assert pid != nil, "Processor #{i} should exist"
        assert Process.alive?(pid), "Processor #{i} should be alive"
      end

      # Push messages and verify they are all processed
      producer = get_producer_pid(broadway)
      push_messages(producer, 1..8)

      # All 8 messages should be handled (by any combination of processors)
      messages = collect_messages(8)
      handled_data = Enum.map(messages, fn {:message_handled, data, _} -> data end) |> Enum.sort()
      assert handled_data == [1, 2, 3, 4, 5, 6, 7, 8]
    end

    test "multiple scale operations work correctly" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      # Scale up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)
      assert get_processor_count(broadway) == 4

      # Scale up more
      :ok = Broadway.Topology.scale_processors(broadway, :default, 6)
      assert get_processor_count(broadway) == 6

      # Scale down
      :ok = Broadway.Topology.scale_processors(broadway, :default, 3, drain_timeout: 100)
      assert get_processor_count(broadway) == 3

      # Scale back up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 5)
      assert get_processor_count(broadway) == 5
    end

    test "emits telemetry events during scaling" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        ref,
        [:broadway, :scale, :start],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:telemetry, :start, metadata})
        end,
        nil
      )

      :telemetry.attach(
        {ref, :stop},
        [:broadway, :scale, :stop],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:telemetry, :stop, metadata})
        end,
        nil
      )

      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)

      assert_receive {:telemetry, :start,
                      %{broadway: ^broadway, processor_key: :default, from_count: 2, to_count: 4}}

      assert_receive {:telemetry, :stop,
                      %{
                        broadway: ^broadway,
                        processor_key: :default,
                        strategy: :dynamic_children,
                        result: :ok
                      }}

      :telemetry.detach(ref)
      :telemetry.detach({ref, :stop})
    end
  end

  describe "scale_processors/4 with batchers" do
    test "batchers subscribe to new processors when scaling up" do
      broadway = start_broadway(Forwarder, concurrency: 2, with_batchers: true)

      # Scale up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)

      # Give time for subscriptions
      Process.sleep(100)

      # Push messages and verify batches are received
      producer = get_producer_pid(broadway)
      push_messages(producer, 1..4)

      # Should receive batches from batcher (batch_size is 2)
      assert_receive {:batch_handled, :default, messages1}, 5000
      assert_receive {:batch_handled, :default, messages2}, 5000
      assert length(messages1) + length(messages2) == 4
    end

    test "messages still flow through pipeline after scaling" do
      broadway = start_broadway(Forwarder, concurrency: 2, with_batchers: true)

      # Process initial batch
      producer = get_producer_pid(broadway)
      push_messages(producer, 1..2)
      assert_receive {:batch_handled, :default, _}, 5000

      # Scale up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)
      Process.sleep(100)

      # Process another batch
      push_messages(producer, 3..4)
      assert_receive {:batch_handled, :default, _}, 5000

      # Scale down
      :ok = Broadway.Topology.scale_processors(broadway, :default, 2, drain_timeout: 100)
      Process.sleep(100)

      # Process yet another batch
      push_messages(producer, 5..6)
      assert_receive {:batch_handled, :default, _}, 5000
    end
  end

  describe "scale_processors/4 with PartitionDispatcher (with partition_by, no max)" do
    test "returns error when scaling beyond default max_processor_concurrency" do
      broadway = start_broadway_with_partition_by(ForwarderWithPartitionBy, concurrency: 2)

      # Without max_processor_concurrency, defaults to concurrency (2)
      # Cannot scale beyond that limit
      assert {:error, {:exceeds_max_processor_concurrency, 2}} =
               Broadway.Topology.scale_processors(broadway, :default, 4)

      # Count should remain unchanged
      assert get_processor_count(broadway) == 2
    end

    test "pipeline continues working after failed scale attempt" do
      broadway = start_broadway_with_partition_by(ForwarderWithPartitionBy, concurrency: 2)

      # Attempt to scale (will fail)
      {:error, _} = Broadway.Topology.scale_processors(broadway, :default, 4)

      # Pipeline should still work
      producer = get_producer_pid(broadway)
      push_messages(producer, 1..4)
      assert_receive {:batch_handled, :default, _}, 5000
      assert_receive {:batch_handled, :default, _}, 5000
    end
  end

  describe "scale_processors/4 with PartitionDispatcher and max_processor_concurrency" do
    test "raises if max_processor_concurrency < concurrency" do
      assert_raise ArgumentError,
                   ~r/:max_processor_concurrency \(2\) must be >= :concurrency \(4\)/,
                   fn ->
                     Broadway.start_link(ForwarderWithPartitionBy,
                       name: new_unique_name(),
                       producer: [module: {ManualProducer, %{test_pid: self()}}],
                       processors: [
                         default: [
                           concurrency: 4,
                           max_processor_concurrency: 2,
                           partition_by: fn msg -> msg.data end
                         ]
                       ],
                       batchers: [default: [batch_size: 2]],
                       context: %{test_pid: self()}
                     )
                   end
    end

    test "scales up when max_processor_concurrency allows" do
      broadway =
        start_broadway_with_partition_by_and_max(ForwarderWithPartitionBy,
          concurrency: 2,
          max_processor_concurrency: 8
        )

      assert :ok = Broadway.Topology.scale_processors(broadway, :default, 4)
      assert get_processor_count(broadway) == 4
      assert count_processor_children(broadway) == 4
    end

    test "scales down within max_processor_concurrency" do
      broadway =
        start_broadway_with_partition_by_and_max(ForwarderWithPartitionBy,
          concurrency: 4,
          max_processor_concurrency: 8
        )

      assert :ok = Broadway.Topology.scale_processors(broadway, :default, 2, drain_timeout: 100)
      assert get_processor_count(broadway) == 2
      assert count_processor_children(broadway) == 2
    end

    test "returns error when scaling beyond max_processor_concurrency" do
      broadway =
        start_broadway_with_partition_by_and_max(ForwarderWithPartitionBy,
          concurrency: 2,
          max_processor_concurrency: 4
        )

      assert {:error, {:exceeds_max_processor_concurrency, 4}} =
               Broadway.Topology.scale_processors(broadway, :default, 6)

      # Count should remain unchanged
      assert get_processor_count(broadway) == 2
    end

    test "messages flow correctly after scaling with partition_by" do
      broadway =
        start_broadway_with_partition_by_and_max(ForwarderWithPartitionBy,
          concurrency: 2,
          max_processor_concurrency: 8
        )

      # Push initial messages - use values that map to active partitions (0, 1)
      # With max_processors=8, rem(0,8)=0, rem(1,8)=1, rem(8,8)=0, rem(9,8)=1
      producer = get_producer_pid(broadway)
      push_messages(producer, [0, 1, 8, 9])
      assert_receive {:batch_handled, :default, _}, 5000
      assert_receive {:batch_handled, :default, _}, 5000

      # Scale up to 4 processors
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)
      Process.sleep(100)

      # Now partitions 0, 1, 2, 3 are active
      # rem(2,8)=2, rem(3,8)=3, rem(10,8)=2, rem(11,8)=3
      push_messages(producer, [2, 3, 10, 11])
      assert_receive {:batch_handled, :default, _}, 5000
      assert_receive {:batch_handled, :default, _}, 5000
    end

    test "multiple scale operations within max" do
      broadway =
        start_broadway_with_partition_by_and_max(ForwarderWithPartitionBy,
          concurrency: 2,
          max_processor_concurrency: 8
        )

      # Scale up
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)
      assert get_processor_count(broadway) == 4

      # Scale up more
      :ok = Broadway.Topology.scale_processors(broadway, :default, 6)
      assert get_processor_count(broadway) == 6

      # Scale down
      :ok = Broadway.Topology.scale_processors(broadway, :default, 3, drain_timeout: 100)
      assert get_processor_count(broadway) == 3

      # Scale back up to max
      :ok = Broadway.Topology.scale_processors(broadway, :default, 8)
      assert get_processor_count(broadway) == 8

      # Cannot exceed max
      assert {:error, {:exceeds_max_processor_concurrency, 8}} =
               Broadway.Topology.scale_processors(broadway, :default, 10)
    end
  end

  describe "get_processor_count/2" do
    test "returns current processor count" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 3)

      assert {:ok, 3} = Broadway.Topology.get_processor_count(broadway, :default)
    end

    test "returns error for unknown processor key" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      assert {:error, :processor_not_found} =
               Broadway.Topology.get_processor_count(broadway, :nonexistent)
    end

    test "reflects changes after scaling" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)

      assert {:ok, 2} = Broadway.Topology.get_processor_count(broadway, :default)

      :ok = Broadway.Topology.scale_processors(broadway, :default, 5)

      assert {:ok, 5} = Broadway.Topology.get_processor_count(broadway, :default)
    end
  end

  describe "message integrity during scaling" do
    test "no messages lost during scale up" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 2)
      producer = get_producer_pid(broadway)

      # Start pushing messages in background
      test_pid = self()

      spawn(fn ->
        for i <- 1..100 do
          push_messages(producer, [i])
          Process.sleep(10)
        end

        send(test_pid, :done_pushing)
      end)

      # Scale up during message processing
      Process.sleep(50)
      :ok = Broadway.Topology.scale_processors(broadway, :default, 4)

      receive do
        :done_pushing -> :ok
      after
        5000 -> flunk("Timeout waiting for messages to be pushed")
      end

      # Collect all handled messages
      messages = collect_all_messages(1000)
      handled_data = Enum.map(messages, fn {:message_handled, data, _} -> data end) |> Enum.sort()

      # Verify all messages were processed
      assert handled_data == Enum.to_list(1..100)
    end

    test "most messages processed during scale down with graceful draining" do
      broadway = start_broadway(ForwarderWithoutBatchers, concurrency: 4)
      producer = get_producer_pid(broadway)

      # Start pushing messages in background
      test_pid = self()

      spawn(fn ->
        for i <- 1..100 do
          push_messages(producer, [i])
          Process.sleep(10)
        end

        send(test_pid, :done_pushing)
      end)

      # Scale down during message processing with graceful draining
      Process.sleep(50)
      :ok = Broadway.Topology.scale_processors(broadway, :default, 2, drain_timeout: 500)

      receive do
        :done_pushing -> :ok
      after
        5000 -> flunk("Timeout waiting for messages to be pushed")
      end

      # Collect all handled messages
      messages = collect_all_messages(1000)
      handled_count = length(messages)

      # With graceful draining, most messages should be processed.
      # Some messages may be lost if they're in-flight during termination.
      # This is a known limitation - proper acknowledgement-based draining
      # would require changes to the producer/acknowledger protocol.
      assert handled_count >= 95,
             "Expected at least 95 messages, got #{handled_count}. " <>
               "Some in-flight messages may be lost during processor termination."
    end
  end

  ## Helpers

  defp start_broadway(module, opts) do
    broadway = new_unique_name()
    concurrency = Keyword.get(opts, :concurrency, 2)
    with_batchers = Keyword.get(opts, :with_batchers, false)

    batchers =
      if with_batchers do
        [default: [batch_size: 2, batch_timeout: 100]]
      else
        []
      end

    {:ok, _pid} =
      Broadway.start_link(module,
        name: broadway,
        producer: [module: {ManualProducer, %{test_pid: self()}}],
        processors: [default: [concurrency: concurrency]],
        batchers: batchers,
        context: %{test_pid: self()}
      )

    # Wait for producer to be initialized
    assert_receive {:producer_initialized, _pid}

    broadway
  end

  defp start_broadway_with_partition_by(module, opts) do
    broadway = new_unique_name()
    concurrency = Keyword.get(opts, :concurrency, 2)

    {:ok, _pid} =
      Broadway.start_link(module,
        name: broadway,
        producer: [module: {ManualProducer, %{test_pid: self()}}],
        processors: [
          default: [
            concurrency: concurrency,
            partition_by: fn msg -> msg.data end
          ]
        ],
        batchers: [default: [batch_size: 2, batch_timeout: 100]],
        context: %{test_pid: self()}
      )

    assert_receive {:producer_initialized, _pid}

    broadway
  end

  defp start_broadway_with_partition_by_and_max(module, opts) do
    broadway = new_unique_name()
    concurrency = Keyword.get(opts, :concurrency, 2)
    max_concurrency = Keyword.get(opts, :max_processor_concurrency, concurrency)

    {:ok, _pid} =
      Broadway.start_link(module,
        name: broadway,
        producer: [module: {ManualProducer, %{test_pid: self()}}],
        processors: [
          default: [
            concurrency: concurrency,
            max_processor_concurrency: max_concurrency,
            partition_by: fn msg -> msg.data end
          ]
        ],
        batchers: [default: [batch_size: 2, batch_timeout: 100]],
        context: %{test_pid: self()}
      )

    assert_receive {:producer_initialized, _pid}

    broadway
  end

  defp new_unique_name do
    :"Elixir.Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_processor_count(broadway) do
    {:ok, count} = Broadway.Topology.get_processor_count(broadway, :default)
    count
  end

  defp count_processor_children(broadway) do
    supervisor = :"#{broadway}.Broadway.ProcessorSupervisor"
    Supervisor.count_children(supervisor).workers
  end

  defp get_processor_name(broadway, index) do
    :"#{broadway}.Broadway.Processor_default_#{index}"
  end

  defp get_producer_pid(broadway) do
    :"#{broadway}.Broadway.Producer_0"
    |> Process.whereis()
  end

  defp push_messages(producer, range) do
    messages =
      Enum.map(range, fn data ->
        %Message{
          data: data,
          acknowledger: {CallerAcknowledger, {self(), make_ref()}, :ok}
        }
      end)

    send(producer, {:push_messages, messages})
  end

  defp collect_messages(count, timeout \\ 5000) do
    Enum.map(1..count, fn _ ->
      receive do
        {:message_handled, _, _} = msg -> msg
      after
        timeout -> flunk("Timeout waiting for message")
      end
    end)
  end

  defp collect_all_messages(timeout) do
    collect_all_messages([], timeout)
  end

  defp collect_all_messages(acc, timeout) do
    receive do
      {:message_handled, _, _} = msg ->
        collect_all_messages([msg | acc], timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end
