defmodule Slipstream.TopicStateTest do
  use ExUnit.Case, async: true

  @moduledoc """
  State kept per topic is released once it is no longer needed, so that a
  long-lived connection which joins and leaves many topics does not grow
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample

  setup do
    uri =
      Server.start(fn sock ->
        Server.serve(sock, fn
          sock, {join_ref, ref, "bad:" <> _ = topic, "phx_join", _params} ->
            Server.reply(sock, join_ref, ref, topic, "error")

          # never replies
          _sock, {_join_ref, _ref, "slow:" <> _, "phx_join", _params} ->
            :ok

          sock, {join_ref, _ref, topic, "close with stale ref", _params} ->
            Server.send_message(sock, [join_ref, "999", topic, "phx_close", %{}])

          sock, {_join_ref, _ref, _topic, "close another topic", _params} ->
            Server.send_message(sock, ["1", "1", "other", "phx_close", %{}])

          _sock, _message ->
            :default
        end)
      end)

    client =
      start_supervised!(
        {@client, pid: self(), uri: uri, heartbeat_interval_msec: 0}
      )

    assert_receive {@client, :connected}
    %Slipstream.Socket{channel_pid: conn} = :sys.get_state(client)

    [client: client, conn: conn]
  end

  test "the connection forgets topics which have been left", c do
    for n <- 1..20 do
      topic = "device:#{n}"
      GenServer.cast(c.client, {:join, topic, %{}})
      assert_receive {@client, :joined, ^topic, %{}}
      GenServer.cast(c.client, {:leave, topic})
      assert_receive {@client, :left, ^topic}
    end

    state = :sys.get_state(c.conn)
    assert state.joins == %{}
    assert state.leaves == %{}
  end

  test "a phx_close for an earlier join does not forget the current join", c do
    GenServer.cast(c.client, {:join, "rooms:lobby", %{}})
    assert_receive {@client, :joined, "rooms:lobby", %{}}

    GenServer.cast(
      c.client,
      {:push, "rooms:lobby", "close with stale ref", %{}}
    )

    assert_receive {@client, :left, "rooms:lobby"}

    assert Map.has_key?(:sys.get_state(c.conn).joins, "rooms:lobby")
  end

  test "a phx_close for a topic which was never joined is ignored", c do
    GenServer.cast(c.client, {:join, "rooms:lobby", %{}})
    assert_receive {@client, :joined, "rooms:lobby", %{}}

    GenServer.cast(c.client, {:push, "rooms:lobby", "close another topic", %{}})
    assert_receive {@client, :left, "other"}

    socket = :sys.get_state(c.client)
    assert Slipstream.Socket.join_status(socket, "other") == nil
    assert Slipstream.Socket.joined?(socket, "rooms:lobby")
  end

  test "telemetry metadata for a failed join is released", c do
    for n <- 1..20 do
      topic = "bad:#{n}"
      GenServer.cast(c.client, {:join, topic, %{}})
      assert_receive {@client, :topic_closed, ^topic, {:failed_to_join, %{}}}
    end

    assert :sys.get_state(c.client).metadata.joins == %{}
  end

  test "telemetry metadata for joins in flight is released on disconnect",
       c do
    GenServer.cast(c.client, {:join, "slow:1", %{}})
    Process.sleep(50)
    assert %{"slow:1" => _} = :sys.get_state(c.client).metadata.joins

    GenServer.cast(c.client, :disconnect)
    assert_receive {@client, :disconnected, :client_disconnect_requested}

    assert :sys.get_state(c.client).metadata.joins == %{}
  end
end
