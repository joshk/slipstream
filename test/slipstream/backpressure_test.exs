defmodule Slipstream.BackpressureTest do
  use ExUnit.Case, async: true

  @moduledoc """
  With `:max_client_queue_length`, a client which handles messages more
  slowly than the server sends them does not accumulate an unbounded queue
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @messages 1_000
  @limit 50

  defmodule SlowClient do
    @moduledoc false
    use Slipstream

    def start_link(opts), do: Slipstream.start_link(__MODULE__, opts)

    @impl Slipstream
    def init(opts) do
      {test_proc, opts} = Keyword.pop!(opts, :pid)

      socket =
        opts |> connect!() |> assign(test_proc: test_proc, received: 0)

      {:ok, socket}
    end

    @impl Slipstream
    def handle_connect(socket), do: {:ok, join(socket, "rooms:lobby")}

    @impl Slipstream
    def handle_message(_topic, _event, _payload, socket) do
      Process.sleep(1)
      received = socket.assigns.received + 1

      if received == 1_000,
        do: send(socket.assigns.test_proc, {__MODULE__, :received_all})

      {:ok, assign(socket, :received, received)}
    end

    @impl Slipstream
    def handle_disconnect(reason, socket) do
      send(socket.assigns.test_proc, {__MODULE__, :disconnected, reason})
      {:ok, socket}
    end
  end

  setup do
    payload = %{"data" => String.duplicate("x", 4_096)}

    uri =
      Server.start(fn sock ->
        Server.serve(sock, fn
          sock, {join_ref, ref, topic, "phx_join", _params} ->
            Server.reply(sock, join_ref, ref, topic, "ok")

            for _ <- 1..@messages,
                do: Server.send_message(sock, [nil, nil, topic, "e", payload])

            :ok

          _sock, _message ->
            :default
        end)
      end)

    [uri: uri]
  end

  defp max_queue_length(pid, until_ms) do
    deadline = System.monotonic_time(:millisecond) + until_ms

    Stream.repeatedly(fn ->
      Process.sleep(5)
      {:message_queue_len, length} = Process.info(pid, :message_queue_len)
      {length, System.monotonic_time(:millisecond) < deadline}
    end)
    |> Enum.take_while(fn {_length, before_deadline?} -> before_deadline? end)
    |> Enum.map(fn {length, _} -> length end)
    |> Enum.max()
  end

  test "the client's queue stays bounded and every message arrives", c do
    client =
      start_supervised!(
        {SlowClient,
         pid: self(),
         uri: c.uri,
         heartbeat_interval_msec: 20,
         max_client_queue_length: @limit}
      )

    # a single read from the socket may hold up to a receive buffer's worth
    # of frames (a few hundred KB, so on the order of 100 of these messages),
    # so the queue can exceed the limit by that much
    assert max_queue_length(client, 500) < @limit + 200

    assert_receive {SlowClient, :received_all}, 10_000
    refute_received {SlowClient, :disconnected, _reason}
  end

  test "without a limit the client's queue grows with the backlog", c do
    client =
      start_supervised!(
        {SlowClient, pid: self(), uri: c.uri, heartbeat_interval_msec: 0}
      )

    assert max_queue_length(client, 500) > 500
  end
end
