defmodule Slipstream.ScheduledRejoinTest do
  use ExUnit.Case, async: true

  @moduledoc """
  A rejoin scheduled by `rejoin/3` is sent on the client's current
  connection, even if the client has reconnected since it was scheduled
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  defmodule Client do
    @moduledoc false
    use Slipstream

    def start_link(opts), do: Slipstream.start_link(__MODULE__, opts)

    @impl Slipstream
    def init(opts) do
      {test_proc, opts} = Keyword.pop!(opts, :pid)
      {:ok, opts |> connect!() |> assign(:test_proc, test_proc)}
    end

    @impl Slipstream
    def handle_connect(socket) do
      send(socket.assigns.test_proc, {:connected, socket.channel_pid})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_join(topic, _response, socket) do
      send(socket.assigns.test_proc, {:joined, topic, socket.channel_pid})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_topic_close(topic, _reason, socket) do
      {:ok, socket} = rejoin(socket, topic)
      send(socket.assigns.test_proc, {:rejoin_scheduled, topic})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_cast({:join, topic}, socket), do: {:noreply, join(socket, topic)}

    def handle_cast({:push, topic, event}, socket) do
      {:ok, _ref} = push(socket, topic, event, %{})
      {:noreply, socket}
    end
  end

  defp start_server do
    Server.start(fn sock ->
      Server.serve(sock, fn
        # the channel crashes and then the whole connection is lost
        sock, {join_ref, _ref, topic, "crash", _params} ->
          Server.send_message(sock, [
            join_ref,
            join_ref,
            topic,
            "phx_error",
            %{}
          ])

          Process.sleep(50)
          :gen_tcp.close(sock)

        _sock, _message ->
          :default
      end)
    end)
  end

  test "a rejoin scheduled before a reconnect is sent on the new connection" do
    uri = start_server()

    client =
      start_supervised!(
        {Client,
         pid: self(),
         uri: uri,
         heartbeat_interval_msec: 0,
         reconnect_after_msec: [10],
         rejoin_after_msec: [300]}
      )

    assert_receive {:connected, first}
    GenServer.cast(client, {:join, "rooms:lobby"})
    assert_receive {:joined, "rooms:lobby", ^first}

    GenServer.cast(client, {:push, "rooms:lobby", "crash"})
    assert_receive {:rejoin_scheduled, "rooms:lobby"}
    assert_receive {:connected, second}, 1_000
    refute second == first

    assert_receive {:joined, "rooms:lobby", ^second}, 1_000
  end

  test "a rejoin which comes due while disconnected is not sent" do
    client =
      start_supervised!({
        Client,
        # comes due after the server has closed the connection
        pid: self(),
        uri: start_server(),
        heartbeat_interval_msec: 0,
        reconnect_after_msec: [60_000],
        rejoin_after_msec: [200]
      })

    assert_receive {:connected, _pid}
    GenServer.cast(client, {:join, "rooms:lobby"})
    assert_receive {:joined, "rooms:lobby", _pid}

    GenServer.cast(client, {:push, "rooms:lobby", "crash"})
    assert_receive {:rejoin_scheduled, "rooms:lobby"}
    refute_receive {:joined, "rooms:lobby", _pid}, 500

    socket = :sys.get_state(client)
    refute Slipstream.Socket.connected?(socket)
    assert Slipstream.Socket.join_status(socket, "rooms:lobby") == :closed
  end
end
