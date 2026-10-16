defmodule Slipstream.UnknownMessagesTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Messages from the server which cannot be decoded or which Slipstream does
  not understand are not dispatched to the client
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  # handle_info/2 only handles this client's own messages, like many
  # GenServers do
  defmodule StrictClient do
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
      send(socket.assigns.test_proc, {__MODULE__, :connected})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_message(topic, event, payload, socket) do
      send(socket.assigns.test_proc, {__MODULE__, topic, event, payload})
      {:ok, socket}
    end

    @impl Slipstream
    def handle_info(:tick, socket), do: {:noreply, socket}
  end

  test "undecodable and unknown messages are dropped" do
    test_proc = self()

    uri =
      Server.start(fn sock ->
        Server.handshake(sock)
        send(test_proc, {:server, self()})
        receive(do: (:go -> :ok))
        # not JSON
        Server.send_frame(sock, :text, "not json")
        # a reply which is not a Phoenix reply
        Server.send_message(sock, [nil, "1", "t", "phx_reply", %{"no" => 1}])
        # a phx_close with a payload
        Server.send_message(sock, ["1", "1", "t", "phx_close", %{"a" => 1}])
        Server.send_message(sock, [nil, nil, "t", "still here", %{}])
        Process.sleep(:infinity)
      end)

    client =
      start_supervised!(
        {StrictClient, pid: self(), uri: uri, heartbeat_interval_msec: 0}
      )

    assert_receive {StrictClient, :connected}
    assert_receive {:server, server}
    send(server, :go)

    assert_receive {StrictClient, "t", "still here", %{}}
    assert Process.alive?(client)
  end
end
