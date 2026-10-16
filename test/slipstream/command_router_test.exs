defmodule Slipstream.CommandRouterTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Commands sent to the connection process do not carry the whole socket
  """

  import Slipstream.Signatures, only: [command: 1]

  alias Slipstream.{CommandRouter, Commands, Socket}

  # stands in for a connection process: forwards casts and calls to the test
  setup do
    test_proc = self()

    channel =
      spawn_link(fn ->
        Stream.repeatedly(fn ->
          receive do
            {:"$gen_call", from, message} ->
              send(test_proc, {:call, message})
              GenServer.reply(from, "1")

            message ->
              send(test_proc, {:info, message})
          end
        end)
        |> Stream.run()
      end)

    socket =
      Socket.new()
      |> Map.put(:channel_pid, channel)
      |> Socket.assign(:cache, Map.new(1..1_000, &{&1, &1}))

    [socket: socket, channel: channel]
  end

  test "forwarded commands only carry the socket's pids", c do
    CommandRouter.route_command(%Commands.JoinTopic{
      socket: c.socket,
      topic: "rooms:lobby",
      payload: %{}
    })

    assert_receive {:info, command(%Commands.JoinTopic{socket: socket})}
    assert socket == %Socket{socket_pid: self(), channel_pid: c.channel}
  end

  test "pushes only carry the socket's pids", c do
    assert {:ok, "1"} =
             CommandRouter.route_command(%Commands.PushMessage{
               socket: c.socket,
               topic: "rooms:lobby",
               event: "event",
               payload: %{},
               timeout: 1_000
             })

    assert_receive {:call, command(%Commands.PushMessage{socket: socket})}
    assert socket == %Socket{socket_pid: self(), channel_pid: c.channel}
  end
end
