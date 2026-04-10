defmodule Server do
    def broadcaster(socks) do
        receive do
            {:sub, sock} ->
                :io.format("BROADCASTER: ~w subscribed~n", [sock])
                broadcaster([sock|socks])
            {:cast, message} ->
                :io.format("BROADCASTER: message: ~ts", [message])
                broadcaster(
                    :lists.filter(
                        fn sock ->
                            case :gen_tcp.send(sock, message) do
                                :ok ->
                                    true

                                {:error, reason} ->
                                    :io.format(
                                    "BROADCASTER: unsubscribing ~w because of error: ~w~n",
                                    [sock, reason]
                                    )

                                    false
                            end
                        end,
                        socks
                    )
                )
        end
    end

    def client(:greet, broadcaster, sock) do
        message =
            "************************************\n" <>
            "Welcome to noaho's server in Elixir!\n" <>
            "Ported from tsoding/erlang-chat.\n" <>
            "Type your nickname and press Enter.\n" <>
            "************************************\n" <>
            "Nickname: "

        case :gen_tcp.send(sock, message) do
            :ok -> client(:nickname, broadcaster, sock)
            error -> error
        end
    end

    def client(:nickname, broadcaster, sock) do
        receive do
            {:recv, nickname} ->
                case nickname do
                    <<"">> ->
                        :gen_tcp.send(sock, <<"ERROR: empty nickname illegal!\n">>)
                        :gen_tcp.close(sock)
                        :ok
                    _ ->
                        send(broadcaster, {:sub, sock})
                        send(broadcaster, {:cast, :io_lib.format("~ts joined\n", [nickname])})
                        client({:chat, nickname}, broadcaster, sock)
                end
        end
    end

    def client({:chat, nickname}, broadcaster, sock) do
        receive do
            {:recv, message} ->
                send(broadcaster, {:cast, :io_lib.format("<~ts> ~ts\n", [nickname, message])})
                client({:chat, nickname}, broadcaster, sock)
            _ ->
                send(broadcaster, {:cast, :io_lib.format("~ts left\n", [nickname])})
                :ok
        end
    end

    def receiver(sock, client_pid) do
        case :gen_tcp.recv(sock, 0) do
            {:ok, packet} ->
                :io.format("RECEIVER: ~w: PACKET: ~w\n", [sock, packet])
                case :unicode.characters_to_list(packet, :utf8) do
                    {:error, _, _} ->
                        :gen_tcp.send(sock, <<"ERROR: you sent an invalid utf8 sequence!\n">>)
                        :io.format("RECEIVER: ~w: ERROR: received invalid utf8 sequence\n", [sock])
                        :gen_tcp.close(sock)
                        send(client_pid, :disconnect)
                        :ok
                    {:incomplete, _, _} ->
                        :gen_tcp.send(sock, <<"ERROR: you sent an incomplete utf8 sequence!\n">>)
                        :io.format("RECEIVER: ~w: ERROR: received incomplete utf8 sequence\n", [sock])
                        :gen_tcp.close(sock)
                        send(client_pid, :disconnect)
                        :ok
                    _ ->
                        send(client_pid, {:recv, :string.trim(packet, :both)})
                        receiver(sock, client_pid)
                end
            error ->
                :io.format("RECEIVER: ~w: ERROR: ~w\n", [sock, error])
                :gen_tcp.close(sock)
                send(client_pid, :disconnect)
                :ok
        end
    end

    def accepter(l_sock, broadcaster) do
        case :gen_tcp.accept(l_sock) do
            {:ok, sock} ->
                client_pid = spawn fn -> client(:greet, broadcaster, sock) end
                _ = spawn fn -> receiver(sock, client_pid) end
                accepter(l_sock, broadcaster)
            error ->
                :io.format("ACCEPTER: ERROR: ~w\n", [error])
                :ok
        end
    end

    def start do
        broadcaster = spawn fn -> broadcaster([]) end
        {:ok, l_sock} = :gen_tcp.listen(5425, [:binary, packet: 0, active: false, reuseaddr: true])
        accepter(l_sock, broadcaster)
    end

end

Server.start()
