defmodule Slipstream.BackoffJitterTest do
  use ExUnit.Case, async: true

  @moduledoc """
  The `:backoff_jitter` option randomises reconnect and rejoin times
  """

  alias Slipstream.{Configuration, Socket}

  defp socket(opts) do
    config =
      Configuration.validate!(
        [
          uri: "ws://localhost:4000/socket/websocket",
          reconnect_after_msec: [1_000],
          rejoin_after_msec: [2_000]
        ] ++ opts
      )

    Socket.new()
    |> Map.put(:channel_config, config)
    |> Socket.put_join_config("rooms:lobby", %{})
  end

  defp reconnect_times(socket),
    do: for(_ <- 1..200, do: socket |> Socket.next_reconnect_time() |> elem(0))

  defp rejoin_times(socket) do
    for _ <- 1..200 do
      socket |> Socket.next_rejoin_time("rooms:lobby") |> elem(0)
    end
  end

  test "without jitter the configured times are used" do
    socket = socket([])

    assert Enum.uniq(reconnect_times(socket)) == [1_000]
    assert Enum.uniq(rejoin_times(socket)) == [2_000]
  end

  test "jitter reduces each time by up to the given fraction" do
    socket = socket(backoff_jitter: 0.5)

    reconnects = reconnect_times(socket)
    assert Enum.all?(reconnects, &(&1 in 500..1_000))
    assert length(Enum.uniq(reconnects)) > 10

    rejoins = rejoin_times(socket)
    assert Enum.all?(rejoins, &(&1 in 1_000..2_000))
    assert length(Enum.uniq(rejoins)) > 10
  end

  test "jitter must be between 0 and 1" do
    for jitter <- [-0.1, 1.5, "0.5"] do
      assert {:error, %NimbleOptions.ValidationError{}} =
               Configuration.validate(
                 uri: "ws://localhost:4000/socket/websocket",
                 backoff_jitter: jitter
               )
    end

    assert {:ok, %Configuration{backoff_jitter: 1}} =
             Configuration.validate(
               uri: "ws://localhost:4000/socket/websocket",
               backoff_jitter: 1
             )
  end
end
