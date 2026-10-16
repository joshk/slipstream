defmodule Slipstream.HeartbeatTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Heartbeats are only considered acknowledged by a reply with their ref
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample

  defp start_client(ack_ref) do
    uri =
      Server.start(fn sock ->
        Server.serve(sock, fn
          sock, {_join_ref, ref, "phoenix", "heartbeat", _params} ->
            Server.reply(sock, nil, ack_ref.(ref), "phoenix", "ok")

          _sock, _message ->
            :default
        end)
      end)

    start_supervised!(
      {@client, pid: self(), uri: uri, heartbeat_interval_msec: 30}
    )

    assert_receive {@client, :connected}
  end

  test "heartbeats acknowledged with their ref keep the connection open" do
    start_client(& &1)

    refute_receive {@client, :disconnected, _reason}, 300
  end

  test "acknowledgements with another ref are not counted" do
    start_client(fn _ref -> "0" end)

    assert_receive {@client, :disconnected, :heartbeat_timeout}, 300
  end
end
