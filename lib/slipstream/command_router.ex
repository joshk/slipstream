defmodule Slipstream.CommandRouter do
  @moduledoc false

  # Implements how a command is routed.
  # Note that many commands are simply forwarded to the connection process
  # for further handling.
  # For other commands like OpenConnection, there is no connection process to
  # send to

  alias Slipstream.Commands.{
    OpenConnection,
    JoinTopic,
    LeaveTopic,
    PushMessage,
    CollectGarbage,
    CloseConnection
  }

  alias Slipstream.Socket

  alias Slipstream.Configuration, as: Config

  import Slipstream.Signatures, only: [command: 1]

  @forwarded_command_types [
    JoinTopic,
    LeaveTopic,
    CollectGarbage,
    CloseConnection
  ]

  @spec route_command(struct()) :: any()
  def route_command(command)

  def route_command(%OpenConnection{config: %Config{test_mode?: true}}) do
    :ok
  end

  def route_command(%OpenConnection{} = cmd) do
    DynamicSupervisor.start_child(
      Slipstream.ConnectionSupervisor,
      {Slipstream.Connection, strip_socket(cmd)}
    )
  end

  # forward the commands to the connection process
  def route_command(%command_type{socket: socket} = cmd)
      when command_type in @forwarded_command_types do
    Socket.send(socket, command(strip_socket(cmd)))
  end

  # this one is a synchronous push with a reply from the connection process
  def route_command(%PushMessage{socket: socket} = cmd) do
    Socket.call(socket, command(strip_socket(cmd)), cmd.timeout)
  end

  # Commands are copied into the connection process's heap when sent to it.
  # The connection process only needs to know which process the socket
  # belongs to, so the rest of the socket (in particular `assigns`, which may
  # be arbitrarily large) is not sent with each command.
  defp strip_socket(%{socket: %Socket{} = socket} = cmd) do
    %{
      cmd
      | socket: %Socket{
          socket_pid: socket.socket_pid,
          channel_pid: socket.channel_pid
        }
    }
  end
end
