defmodule Slipstream.AwaitOnDisconnectTest do
  use ExUnit.Case, async: true

  @moduledoc """
  `await_join/3` and `await_leave/3` return when the connection goes away
  rather than waiting for their whole timeout
  """

  import Slipstream

  alias Slipstream.FakeWebSocketServer, as: Server

  setup do
    uri =
      Server.start(fn sock ->
        Server.serve(sock, fn
          # never replies to joins of "slow:" topics
          _sock, {_join_ref, _ref, "slow:" <> _, "phx_join", _params} ->
            :ok

          sock, {join_ref, ref, "bad:" <> _ = topic, "phx_join", _params} ->
            Server.reply(sock, join_ref, ref, topic, "error", %{"no" => 1})

          # replies to leaves but never sends phx_close
          sock, {join_ref, ref, topic, "phx_leave", _params} ->
            Server.reply(sock, join_ref, ref, topic, "ok")

          _sock, _message ->
            :default
        end)
      end)

    socket = connect!(uri: uri, heartbeat_interval_msec: 0) |> await_connect!()

    [socket: socket]
  end

  defp kill_connection_after(socket, msec) do
    pid = socket.channel_pid
    spawn(fn -> Process.sleep(msec) && Process.exit(pid, :kill) end)
  end

  defp timed(fun) do
    {us, result} = :timer.tc(fun)
    {div(us, 1_000), result}
  end

  test "await_join/3 returns when the connection closes", c do
    socket = join(c.socket, "slow:1")
    kill_connection_after(socket, 50)

    assert {ms, {:error, :not_connected}} =
             timed(fn -> await_join(socket, "slow:1", 5_000) end)

    assert ms < 1_000
  end

  test "await_join/3 receives a join which succeeded before the connection closed",
       c do
    socket = join(c.socket, "rooms:lobby")
    Process.sleep(100)
    Process.exit(socket.channel_pid, :kill)
    Process.sleep(50)

    assert {:ok, socket} = await_join(socket, "rooms:lobby", 5_000)
    assert Slipstream.Socket.joined?(socket, "rooms:lobby")
  end

  test "await_join/3 returns a failure to join", c do
    socket = join(c.socket, "bad:1")

    assert await_join(socket, "bad:1") ==
             {:error, {:failed_to_join, %{"no" => 1}}}
  end

  test "await_join/3 times out while connected", c do
    socket = join(c.socket, "slow:1")

    assert await_join(socket, "slow:1", 50) == {:error, :timeout}
  end

  test "await_join/3 does not wait when the socket is not connected" do
    assert {ms, {:error, :not_connected}} =
             timed(fn -> await_join(new_socket(), "rooms:lobby", 5_000) end)

    assert ms < 1_000
  end

  test "await_leave/3 returns when the connection closes", c do
    {:ok, socket} = c.socket |> join("rooms:lobby") |> await_join("rooms:lobby")
    socket = leave(socket, "rooms:lobby")
    kill_connection_after(socket, 50)

    assert {ms, {:error, :not_connected}} =
             timed(fn -> await_leave(socket, "rooms:lobby", 5_000) end)

    assert ms < 1_000
  end

  test "await_leave/3 times out while connected", c do
    {:ok, socket} = c.socket |> join("rooms:lobby") |> await_join("rooms:lobby")
    socket = leave(socket, "rooms:lobby")

    assert await_leave(socket, "rooms:lobby", 50) == {:error, :timeout}
  end
end
