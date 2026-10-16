defmodule Slipstream.SingleConnectionTest do
  use ExUnit.Case, async: true

  @moduledoc """
  A client has at most one connection process: starting a new connection
  replaces any connection process previously started for the socket
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  defmodule Client do
    @moduledoc false
    use Slipstream

    def start_link(opts), do: Slipstream.start_link(__MODULE__, opts)

    @impl Slipstream
    def init(test_proc), do: {:ok, assign(new_socket(), :test_proc, test_proc)}

    @impl Slipstream
    def handle_connect(socket) do
      send(socket.assigns.test_proc, {:connected, socket.channel_pid})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_disconnect(reason, socket) do
      send(socket.assigns.test_proc, {:disconnected, reason})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_call({:connect, opts}, _from, socket) do
      socket = connect!(socket, opts)
      {:reply, socket.connection_pid, socket}
    end

    def handle_call({:await_connect_then_reconnect, timeout}, _from, socket) do
      result = await_connect(socket, timeout)
      {:ok, socket} = reconnect(socket)
      {:reply, result, socket}
    end

    # the first connection's events arrive while sleeping, before the second
    # connection is started
    def handle_call({:connect_twice, first, second}, _from, socket) do
      socket = connect!(socket, first)
      Process.sleep(200)
      socket = connect!(socket, second)
      {:reply, socket.connection_pid, socket}
    end

    def handle_call({:connect_twice_and_await, opts}, _from, socket) do
      socket = connect!(socket, opts)
      Process.sleep(200)
      socket = connect!(socket, opts)
      {:ok, socket} = await_connect(socket)
      {:reply, socket, socket}
    end

    def handle_call(:reconnect_twice, _from, socket) do
      {:ok, socket} = reconnect(socket)
      {:ok, socket} = reconnect(socket)
      {:reply, :ok, socket}
    end

    def handle_call(:disconnect, _from, socket) do
      {:ok, socket} = socket |> disconnect() |> await_disconnect()
      {:reply, :ok, socket}
    end

    def handle_call(:socket, _from, socket), do: {:reply, socket, socket}
  end

  setup do
    client = start_supervised!({Client, self()})
    [client: client, uri: Server.start(&Server.serve/1)]
  end

  defp connections(client) do
    Slipstream.ConnectionSupervisor
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn {_id, pid, _type, _modules} -> pid end)
    |> Enum.filter(fn pid ->
      try do
        match?(%{client_pid: ^client}, :sys.get_state(pid))
      catch
        # connection processes of other (async) tests may exit meanwhile
        :exit, _reason -> false
      end
    end)
  end

  defp opts(uri, extra \\ []),
    do: [uri: uri, heartbeat_interval_msec: 0] ++ extra

  test "reconnecting after await_connect/2 times out leaves one connection",
       c do
    slow_uri =
      Server.start(fn sock ->
        key = Server.read_request(sock)
        Process.sleep(300)
        :ok = :gen_tcp.send(sock, Server.upgrade_response(key))
        Process.sleep(:infinity)
      end)

    first = GenServer.call(c.client, {:connect, opts(slow_uri)})

    assert GenServer.call(c.client, {:await_connect_then_reconnect, 100}) ==
             {:error, :timeout}

    assert_receive {:connected, second}, 1_000
    refute_receive {:connected, _pid}, 500

    refute Process.alive?(first)
    assert connections(c.client) == [second]
  end

  test "connecting again replaces a connected connection", c do
    first = GenServer.call(c.client, {:connect, opts(c.uri)})
    assert_receive {:connected, ^first}

    second = GenServer.call(c.client, {:connect, opts(c.uri)})
    assert_receive {:connected, ^second}

    refute Process.alive?(first)
    assert connections(c.client) == [second]
    refute_received {:disconnected, _reason}
  end

  test "events from a replaced connection are ignored", c do
    second =
      GenServer.call(
        c.client,
        {:connect_twice, opts(c.uri), opts(c.uri)}
      )

    assert_receive {:connected, ^second}
    refute_receive {:connected, _pid}
  end

  test "a failure to connect from a replaced connection is ignored", c do
    {:ok, lsock} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(lsock)
    :ok = :gen_tcp.close(lsock)
    refused_uri = "ws://127.0.0.1:#{port}/socket/websocket"

    second =
      GenServer.call(
        c.client,
        {:connect_twice, opts(refused_uri), opts(c.uri)}
      )

    assert_receive {:connected, ^second}
    refute_received {:disconnected, _reason}
  end

  test "await_connect/2 ignores events from a replaced connection", c do
    socket = GenServer.call(c.client, {:connect_twice_and_await, opts(c.uri)})

    assert socket.channel_pid == socket.connection_pid
    assert connections(c.client) == [socket.channel_pid]
  end

  test "a scheduled reconnect is skipped once connected", c do
    first =
      GenServer.call(
        c.client,
        {:connect, opts(c.uri, reconnect_after_msec: [10, 300])}
      )

    assert_receive {:connected, ^first}
    :ok = GenServer.call(c.client, :disconnect)

    # schedules reconnects after 10ms and 300ms
    :ok = GenServer.call(c.client, :reconnect_twice)

    assert_receive {:connected, second}
    refute_receive {:connected, _pid}, 500
    assert connections(c.client) == [second]
  end
end
