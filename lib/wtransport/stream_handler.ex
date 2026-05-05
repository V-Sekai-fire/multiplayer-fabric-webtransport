defmodule Wtransport.StreamHandler do
  alias Wtransport.Connection
  alias Wtransport.StreamRequest
  alias Wtransport.Stream

  @callback handle_stream(stream :: Stream.t(), conn_state :: term()) ::
              {:continue, term()} | :close

  @callback handle_data(data :: String.t(), stream :: Stream.t(), state :: term()) ::
              {:continue, term()} | :close

  @callback handle_close(stream :: Stream.t(), state :: term()) ::
              {:continue, term()} | :close

  @callback handle_error(reason :: String.t(), stream :: Stream.t(), state :: term()) :: :ok

  # Module-level dispatch helpers. These live here (not in the macro-generated
  # code) so that Elixir's type checker and Dialyzer analyse them against the
  # full `{:continue, term()} | :close` union declared in the @spec, rather
  # than narrowing based on a concrete callback implementation. This prevents
  # dead-clause warnings in modules that `use Wtransport.StreamHandler`.

  @spec dispatch_request(
          {:continue, term()} | :close,
          reference(),
          Stream.t(),
          term()
        ) :: {:noreply, {Stream.t(), term()}} | {:stop, :normal, {Stream.t(), term()}}
  def dispatch_request({:continue, new_state}, request_tx, stream, _conn_state) do
    {:ok, {}} = Wtransport.Native.reply_request(request_tx, :ok, self())
    {:noreply, {stream, new_state}}
  end

  def dispatch_request(_, request_tx, stream, conn_state) do
    {:ok, {}} = Wtransport.Native.reply_request(request_tx, :error, self())
    {:stop, :normal, {stream, conn_state}}
  end

  @spec dispatch_data(
          {:continue, term()} | :close,
          Stream.t(),
          term()
        ) :: {:noreply, {Stream.t(), term()}} | {:stop, :normal, {Stream.t(), term()}}
  def dispatch_data({:continue, new_state}, stream, _state) do
    {:noreply, {stream, new_state}}
  end

  def dispatch_data(_, stream, state) do
    {:stop, :normal, {stream, state}}
  end

  @spec dispatch_close(
          {:continue, term()} | :close,
          Stream.t(),
          term()
        ) :: {:noreply, {Stream.t(), term()}} | {:stop, :normal, {Stream.t(), term()}}
  def dispatch_close({:continue, new_state}, stream, _state) do
    {:noreply, {stream, new_state}}
  end

  def dispatch_close(_, stream, state) do
    {:stop, :normal, {stream, state}}
  end

  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour Wtransport.StreamHandler

      @impl Wtransport.StreamHandler
      def handle_stream(%Stream{} = _stream, conn_state), do: {:continue, conn_state}

      @impl Wtransport.StreamHandler
      def handle_data(_data, %Stream{} = _stream, state),
        do: {:continue, state}

      @impl Wtransport.StreamHandler
      def handle_close(%Stream{} = _stream, _state), do: :close

      @impl Wtransport.StreamHandler
      def handle_error(_reason, %Stream{} = _stream, _state), do: :ok

      defoverridable Wtransport.StreamHandler

      use GenServer, restart: :temporary

      require Logger

      # Client

      def start_link(
            {%Connection{} = connection, %StreamRequest{} = request, conn_state, conn_pid}
          ) do
        GenServer.start_link(__MODULE__, {connection, request, conn_state, conn_pid})
      end

      # Server (callbacks)

      @impl true
      def init({%Connection{} = connection, %StreamRequest{} = request, conn_state, conn_pid}) do
        Logger.debug("init")

        monitor_ref = Process.monitor(conn_pid)

        stream =
          struct(
            Stream,
            %{connection: connection, monitor_ref: monitor_ref}
            |> Map.merge(Map.from_struct(request))
          )

        {:ok, {stream, conn_state}, {:continue, :wtransport_stream_request}}
      end

      @impl true
      def terminate(_reason, {%Stream{} = stream, _state}) do
        if stream.request_tx != nil do
          Logger.debug("terminate")

          Wtransport.Native.reply_request(stream.request_tx, :pid_crashed, self())
        end

        :ok
      end

      @impl true
      def handle_continue(:wtransport_stream_request, {%Stream{} = stream, conn_state}) do
        Logger.debug(":wtransport_stream_request")

        Wtransport.StreamHandler.dispatch_request(
          handle_stream(stream, conn_state),
          stream.request_tx,
          stream,
          conn_state
        )
      end

      @impl true
      def handle_info(
            {:DOWN, ref, :process, _object, _reason},
            {%Stream{monitor_ref: monitor_ref} = stream, state}
          )
          when ref == monitor_ref do
        Logger.debug(":DOWN")

        handle_error("pid_crashed", stream, state)

        {:stop, :normal, {stream, state}}
      end

      @impl true
      def handle_info({:wtransport_error, error}, {%Stream{} = stream, state}) do
        Logger.debug(":wtransport_error")

        handle_error(error, stream, state)

        {:stop, :normal, {stream, state}}
      end

      @impl true
      def handle_info({:wtransport_data_received, data}, {%Stream{} = stream, state}) do
        if stream.connection.log_network_data do
          Logger.debug(":wtransport_data_received")
        end

        Wtransport.StreamHandler.dispatch_data(handle_data(data, stream, state), stream, state)
      end

      @impl true
      def handle_info(:wtransport_stream_closed, {%Stream{} = stream, state}) do
        Logger.debug(":wtransport_stream_closed")

        Wtransport.StreamHandler.dispatch_close(handle_close(stream, state), stream, state)
      end
    end
  end
end
