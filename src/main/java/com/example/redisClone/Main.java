package com.example.redisClone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Set;

import com.example.redisClone.rdb.RDBconfig;
import com.example.redisClone.rdb.RDBconfigHandler;

public class Main {
    public static void main(String[] args) throws IOException {

        String directory = "/tmp";
        String dataBaseFileName = "Tdump.rdb";
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--dir")) {
                directory = args[i + 1];
            } else if (args[i].equals("--dbfilename")) {
                dataBaseFileName = args[i + 1];
            }
        }
        RDBconfig rdbConfig = new RDBconfig(directory, dataBaseFileName);

        System.out.println("Logs from your program will appear here!");
        int port = 6379;

        Selector selector = Selector.open();

        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("Stared redis server on port " + port);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        HashMap<SocketChannel, StringBuilder> clientBuffers = new HashMap<>();
        HashMap<String, RedisStoreObject> redisStore = RDBconfigHandler.loadRDB(rdbConfig);
        if (redisStore == null) {
            redisStore = new HashMap<>();
        }

        // event loop
        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();

            for (SelectionKey key : selectedKeys) {

                // Accepting new cleint connection
                if (key.isAcceptable()) {
                    handleAcceptableKeys(clientBuffers, key, selector);
                }

                // Client already exists
                else if (key.isReadable()) {
                    handleReadableKeys(buffer, clientBuffers, key, redisStore, rdbConfig);
                }
            }
        }
    }

    public static void handleAcceptableKeys(HashMap<SocketChannel, StringBuilder> clientBuffers, SelectionKey key,
            Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client != null) {
            client.configureBlocking(false); // non blocking
            client.register(selector, SelectionKey.OP_READ);
            System.out.println("New client connected " + client.getRemoteAddress());
            clientBuffers.put(client, new StringBuilder());
        }
    }

    public static void handleReadableKeys(ByteBuffer buffer, HashMap<SocketChannel, StringBuilder> clientBuffers,
            SelectionKey key, HashMap<String, RedisStoreObject> redisStore, RDBconfig rdbConfig) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        buffer.clear(); // reset buffer for new read

        int bytesRead = client.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Client diconnected " + client.getRemoteAddress());
            client.close();
            clientBuffers.remove(client);
            return;
        }

        buffer.flip(); // prepare buffer to read data from it

        if (buffer.remaining() > 0) {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            String input = new String(data).trim(); // convert byte to string
            clientBuffers.get(client).append(input);

            String fullInputString = clientBuffers.get(client).toString();

            String[] lines = fullInputString.split("\r\n");
            if (lines.length >= 3 && lines[0].startsWith("*")) {
                String command = lines[2].trim().toUpperCase();

                boolean handled = false;

                switch (command) {
                    case "PING" -> {
                        System.out.println("Response to client " + client.getRemoteAddress() + ": " + "PONG");
                        client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
                        handled = true;
                    }

                    case "ECHO" -> {
                        if (lines.length >= 5) {
                            String messageResponse = "$" + lines[4].length() + "\r\n" + lines[4] + "\r\n";
                            System.out.println(
                                    "Response to client " + client.getRemoteAddress() + ": " + messageResponse);
                            client.write(ByteBuffer.wrap(messageResponse.getBytes()));
                            handled = true;
                        }
                    }

                    case "SET" -> {
                        if (lines.length >= 7) {
                            long expiry = Long.MAX_VALUE;
                            if (lines.length >= 11 && lines[8].equalsIgnoreCase("PX")) {
                                expiry = Long.parseLong(lines[10]) + System.currentTimeMillis();
                            }
                            System.out.println("Response to client " + client.getRemoteAddress() + ": "
                                    + "SET: OK ; with expiry: " + expiry);
                            redisStore.put(lines[4], new RedisStoreObject(lines[6], expiry));
                            client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
                            handled = true;
                        }
                    }

                    case "GET" -> {
                        String keyToGet = lines[4];
                        RedisStoreObject storedObject = redisStore.get(keyToGet);
                        String getResponse;

                        if(storedObject == null){ // if there is no key or key does not exist
                            getResponse = "$-1\r\n";
                        } else {
                            long expiryTime = storedObject.expiration;

                            if(expiryTime != Long.MAX_VALUE && expiryTime < System.currentTimeMillis()){
                                //key expired
                                redisStore.remove(keyToGet);
                                getResponse = "$-1\r\n";
                            } else {
                                String value = storedObject.value;
                                getResponse = "$" + value.length() + "\r\n" + value + "\r\n";
                            }
                            client.write(ByteBuffer.wrap(getResponse.getBytes()));
                            handled = true;
                        }
                    }
                    case "CONFIG" -> {
                        if (lines.length >= 7 && lines[4].equalsIgnoreCase("GET")) {
                            String redisKey = lines[6];
                            String value = rdbConfig.get(redisKey);
                            if (value != null) {
                                String resp = "*2\r\n$" + redisKey.length() + "\r\n" + redisKey + "\r\n$"
                                        + value.length()
                                        + "\r\n" + value + "\r\n";
                                client.write(ByteBuffer.wrap(resp.getBytes()));
                                System.out.println(resp);
                            } else {
                                String resp = "*2\r\n$" + redisKey.length() + "\r\n" + redisKey + "\r\n$-1\r\n";
                                client.write(ByteBuffer.wrap(resp.getBytes()));
                                System.out.println(resp);
                            }
                            handled = true;
                        }
                    }
                    case "KEYS" -> {
                        if (lines.length >= 5 && lines[4].equals("*")) {
                            Set<String> keys = redisStore.keySet();
                            StringBuilder responseBuilder = new StringBuilder();

                            responseBuilder.append("*").append(keys.size()).append("\r\n");

                            for (String k : keys) {
                                responseBuilder.append("$").append(k.length()).append("\r\n");
                                responseBuilder.append(k).append("\r\n");
                            }

                            client.write(ByteBuffer.wrap(responseBuilder.toString().getBytes()));
                            System.out.println("KEYS response: " + responseBuilder.toString().replace("\r\n", "\\r\\n"));
                            handled = true;
                        }
                    }
                }

                if (!handled) {
                    String messageErrorResponse = "-ERR unknown or invalid command '" + command + "'\r\n";
                    System.out.println(
                            "Response to client " + client.getRemoteAddress() + ": " + "Error Command: " + command);
                    client.write(ByteBuffer.wrap(messageErrorResponse.getBytes()));
                }

                clientBuffers.get(client).setLength(0);
            }
        }
    }

}