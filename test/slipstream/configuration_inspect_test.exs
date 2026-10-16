defmodule Slipstream.ConfigurationInspectTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Credentials in configuration are not shown when it is inspected
  """

  alias Slipstream.Configuration

  test "header values and URI userinfo are redacted" do
    config =
      Configuration.validate!(
        uri: "wss://user:hunter2@example.com/socket/websocket",
        headers: [{"authorization", "Bearer s3cr3t"}]
      )

    inspected = inspect(config)

    refute inspected =~ "s3cr3t"
    refute inspected =~ "hunter2"
    assert inspected =~ "authorization"
    assert inspected =~ "example.com"

    # the configuration itself is unchanged
    assert config.headers == [{"authorization", "Bearer s3cr3t"}]
  end

  test "a configuration without credentials is inspected as usual" do
    config = Configuration.validate!(uri: "ws://localhost:4000/socket")

    assert inspect(config) =~ "%Slipstream.Configuration{"
    assert inspect(config) =~ "localhost"
  end

  test "a configuration built without validation can be inspected" do
    assert inspect(%Configuration{}) =~ "%Slipstream.Configuration{"
  end
end
