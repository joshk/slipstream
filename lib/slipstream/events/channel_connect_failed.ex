defmodule Slipstream.Events.ChannelConnectFailed do
  @moduledoc false

  @type t :: %__MODULE__{}

  # pid is the connection process which failed to connect
  defstruct ~w[pid reason]a
end
