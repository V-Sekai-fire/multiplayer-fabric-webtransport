defmodule Wtransport.Native do
  @moduledoc false

  # Manually load the Rust NIF. We do NOT use `use Rustler` because Rustler's
  # compile-time macro defers beam generation to a Mix compiler task that does
  # not run for library dependencies compiled via `mix deps.compile`. The NIF
  # is compiled by the host project's Dockerfile and placed in priv/native/
  # before `mix deps.compile` runs.
  @on_load :__load_nif__
  def __load_nif__ do
    nif_path = Application.app_dir(:wtransport) |> Path.join("priv/native/wtransport_native")
    :erlang.load_nif(String.to_charlist(nif_path), 0)
  end

  defp nif_error, do: :erlang.nif_error(:nif_not_loaded)

  # Server-side NIFs
  def start_runtime(_pid, _host, _port, _cert_chain, _priv_key, _log_network_data),
    do: nif_error()

  def stop_runtime(_runtime), do: nif_error()
  def reply_request(_tx_channel, _result, _pid), do: nif_error()
  def send_data(_tx_channel, _data, _log_network_data), do: nif_error()

  # Client-side NIFs
  def connect_client(_url, _cert_hash_b64, _owner_pid), do: nif_error()
  def send_datagram_client(_client, _data), do: nif_error()
  def disconnect_client(_client), do: nif_error()
end
