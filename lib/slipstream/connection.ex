defmodule Slipstream.Connection do
  @moduledoc false

  # the Connection _is_ the socket client
  # Connection interfaces with Mint.WebSocket and any module that implements the
  # Slipstream behaviour to offer websocket client functionality

  use GenServer, restart: :temporary

  alias Slipstream.Connection.{State, Pipeline, Telemetry}

  def start_link(%Slipstream.Commands.OpenConnection{config: config} = init_arg) do
    GenServer.start_link(__MODULE__, init_arg,
      hibernate_after: hibernate_after(config)
    )
  end

  # Hibernating garbage collects the process, releasing references to large
  # binaries (e.g. received frames) which an idle connection process would
  # otherwise keep alive until its next garbage collection
  defp hibernate_after(%{hibernate_after_msec: 0}), do: :infinity
  defp hibernate_after(%{hibernate_after_msec: msec}), do: msec

  @impl GenServer
  def init(%Slipstream.Commands.OpenConnection{
        socket: %Slipstream.Socket{socket_pid: socket_pid},
        config: config
      }) do
    %State{} = initial_state = State.new(config, socket_pid)

    metadata = Telemetry.begin(initial_state)
    state = %{initial_state | metadata: metadata}

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    Pipeline.handle(:connect, state)
  end

  @impl GenServer
  def handle_info(message, state) do
    Pipeline.handle(message, state)
  end

  @impl GenServer
  def handle_call(message, _from, state) do
    Pipeline.handle(message, state)
  end

  @impl GenServer
  def terminate(reason, state) do
    Telemetry.conclude(state, reason)

    state
  end
end
