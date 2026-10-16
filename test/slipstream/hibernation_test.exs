defmodule Slipstream.HibernationTest do
  use ExUnit.Case, async: true

  @moduledoc """
  An idle connection process hibernates, releasing large binaries it no
  longer needs
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample
  @messages 20

  setup do
    test_proc = self()

    uri =
      Server.start(fn sock ->
        Server.handshake(sock)
        send(test_proc, {:server, self()})
        receive(do: (:go -> :ok))
        blob = String.duplicate("x", 256 * 1024)

        for n <- 1..@messages,
            do: Server.send_message(sock, [nil, nil, "t", "#{n}", blob])

        Process.sleep(:infinity)
      end)

    [uri: uri]
  end

  defp binary_bytes(pid) do
    {:binary, binaries} = Process.info(pid, :binary)
    binaries |> Enum.map(&elem(&1, 1)) |> Enum.sum()
  end

  # OTP 28 reports :gen_server.loop_hibernate/4, earlier releases
  # :erlang.hibernate/3
  defp hibernating?(pid) do
    {:current_function, {_module, function, _arity}} =
      Process.info(pid, :current_function)

    function in [:hibernate, :loop_hibernate]
  end

  defp receive_messages(c, opts) do
    client =
      start_supervised!(
        {@client, [pid: self(), uri: c.uri, heartbeat_interval_msec: 0] ++ opts}
      )

    assert_receive {@client, :connected}
    assert_receive {:server, server}
    send(server, :go)

    for n <- 1..@messages do
      event = "#{n}"
      assert_receive {@client, :received_message, "t", ^event, _blob}, 2_000
    end

    %Slipstream.Socket{channel_pid: conn} = :sys.get_state(client)
    conn
  end

  test "an idle connection hibernates", c do
    conn = receive_messages(c, hibernate_after_msec: 100)
    Process.sleep(300)

    assert hibernating?(conn)

    assert binary_bytes(conn) < 256 * 1024
  end

  test "hibernation may be disabled", c do
    conn = receive_messages(c, hibernate_after_msec: 0)
    Process.sleep(300)

    refute hibernating?(conn)
  end
end
