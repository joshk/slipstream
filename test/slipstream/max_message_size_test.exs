defmodule Slipstream.MaxMessageSizeTest do
  use ExUnit.Case, async: true

  @moduledoc """
  With `:max_message_bytes` a message from the server which is too large
  fails the connection instead of being buffered
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample
  @limit 100_000

  setup do
    [test_proc: self()]
  end

  defp connect(c, frames, opts \\ []) do
    uri =
      Server.start(
        fn sock ->
          Server.handshake(sock)
          send(c.test_proc, {:server, self()})
          receive(do: (:go -> :ok))

          for frame <- frames do
            :ok = :gen_tcp.send(sock, frame)
            Process.sleep(5)
          end

          Process.sleep(:infinity)
        end,
        nodelay: true
      )

    start_supervised!(
      {@client,
       [pid: self(), uri: uri, heartbeat_interval_msec: 0] ++
         Keyword.merge([max_message_bytes: @limit], opts)}
    )

    assert_receive {@client, :connected}
    assert_receive {:server, server}
    send(server, :go)
  end

  defp message(topic, size),
    do: Jason.encode!([nil, nil, topic, "e", String.duplicate("x", size)])

  # an unmasked text frame (opcode 1) or continuation frame (opcode 0) with
  # a payload of 126 to 65_535 bytes
  defp fragment(opcode, payload, fin?) do
    fin = if fin?, do: 1, else: 0
    <<fin::1, 0::3, opcode::4, 0::1, 126::7, byte_size(payload)::16>> <> payload
  end

  # a frame whose header promises far more data than is sent
  defp truncated_frame(sent_bytes) do
    <<1::1, 0::3, 1::4, 0::1, 127::7, 1_000_000_000::64>> <>
      String.duplicate("x", sent_bytes)
  end

  test "messages within the limit are received", c do
    connect(c, [Server.frame(:text, message("t", 50_000))])

    assert_receive {@client, :received_message, "t", "e", _payload}
  end

  test "a frame which grows beyond the limit fails the connection", c do
    chunks = for _ <- 1..20, do: String.duplicate("x", 10_000)
    connect(c, [truncated_frame(0) | chunks])

    assert_receive {@client, :disconnected, {:message_too_large, @limit}}
  end

  test "a fragmented message beyond the limit fails the connection", c do
    payload = message("t", 150_000)
    <<first::binary-size(50_000), rest::binary>> = payload
    <<second::binary-size(50_000), last::binary>> = rest

    connect(c, [
      fragment(1, first, false),
      fragment(0, second, false),
      fragment(0, last, true)
    ])

    assert_receive {@client, :disconnected, {:message_too_large, @limit}}
  end

  test "without a limit large messages are received", c do
    connect(c, [Server.frame(:text, message("t", 300_000))],
      max_message_bytes: 0
    )

    assert_receive {@client, :received_message, "t", "e", _payload}
  end
end
