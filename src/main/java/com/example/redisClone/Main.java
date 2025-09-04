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

/**
 * The main entry point for the Redis Clone server.
 * It uses Java NIO (Non-blocking I/O) to handle multiple client connections concurrently.
 */
public class Main {
    /**
     * Main method to start the server.
     * @param args Command-line arguments, specifically --dir and --dbfilename.
     * @throws IOException If an I/O error occurs.
     */
    public static void main(String[] args) throws IOException {

        // --- Configuration Parsing ---
        // Default values for RDB configuration.
        String directory = "/tmp";
        String dataBaseFileName = "Tdump.rdb";
        // Loop through command-line arguments to find --dir and --dbfilename.
        int port = 6379; // Standard Redis port.
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--dir")) {
                directory = args[i + 1];
            } else if (args[i].equals("--dbfilename")) {
                dataBaseFileName = args[i + 1];
            } else if(args[i].equals("--port")){
                port = Integer.parseInt(args[i+1]);
            }
        }
        // Create a configuration object to hold these values.
        RDBconfig rdbConfig = new RDBconfig(directory, dataBaseFileName);

        System.out.println("Logs from your program will appear here!");

        // --- Non-Blocking Server Setup ---
        // Selector allows us to manage multiple channels (connections) with a single thread.
        Selector selector = Selector.open();

        // Create a non-blocking server socket channel.
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(port));
        serverSocket.configureBlocking(false);
        // Register the server socket to accept incoming connections.
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("Stared redis server on port " + port);

        // A reusable buffer for reading data from clients.
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        // --- Data Storage ---
        // A map to hold partial commands from clients.
        HashMap<SocketChannel, StringBuilder> clientBuffers = new HashMap<>();
        // The main in-memory store for Redis data, loaded from the RDB file.
        HashMap<String, RedisStoreObject> redisStore = RDBconfigHandler.loadRDB(rdbConfig);
        if (redisStore == null) { // Safety check in case loading fails.
            redisStore = new HashMap<>();
        }

        // --- Main Event Loop ---
        // This loop continuously waits for and processes I/O events.
        while (true) {
            selector.select(); // Blocks until at least one channel is ready for an I/O operation.
            Set<SelectionKey> selectedKeys = selector.selectedKeys(); // Get the set of ready keys.

            for (SelectionKey key : selectedKeys) {
                // Check if a new client is trying to connect.
                if (key.isAcceptable()) {
                    handleAcceptableKeys(clientBuffers, key, selector);
                }
                // Check if an existing client has sent data.
                else if (key.isReadable()) {
                    handleReadableKeys(buffer, clientBuffers, key, redisStore, rdbConfig);
                }
            }
        }
    }

    /**
     * Handles new client connections.
     * @param clientBuffers Map to store client-specific data buffers.
     * @param key The selection key representing the server socket.
     * @param selector The main selector.
     * @throws IOException If an I/O error occurs.
     */
    public static void handleAcceptableKeys(HashMap<SocketChannel, StringBuilder> clientBuffers, SelectionKey key,
            Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept(); // Accept the new connection.
        if (client != null) {
            client.configureBlocking(false); // Set the client socket to non-blocking.
            client.register(selector, SelectionKey.OP_READ); // Register the client to listen for readable data.
            System.out.println("New client connected " + client.getRemoteAddress());
            clientBuffers.put(client, new StringBuilder()); // Create a buffer for this client's commands.
        }
    }

    /**
     * Handles reading data from a client and processing commands.
     * @param buffer A shared buffer for reading data.
     * @param clientBuffers Map of client-specific command buffers.
     * @param key The selection key for the readable client channel.
     * @param redisStore The main in-memory data store.
     * @param rdbConfig The server's RDB configuration.
     * @throws IOException If an I/O error occurs.
     */
    public static void handleReadableKeys(ByteBuffer buffer, HashMap<SocketChannel, StringBuilder> clientBuffers,
            SelectionKey key, HashMap<String, RedisStoreObject> redisStore, RDBconfig rdbConfig) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        buffer.clear(); // Prepare the buffer for a new read.

        int bytesRead = client.read(buffer);

        // If bytesRead is -1, the client has closed the connection.
        if (bytesRead == -1) {
            System.out.println("Client diconnected " + client.getRemoteAddress());
            client.close();
            clientBuffers.remove(client);
            return;
        }

        buffer.flip(); // Switch buffer from write mode to read mode.

        if (buffer.remaining() > 0) {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            String input = new String(data); // Don't trim here to preserve RESP structure.
            clientBuffers.get(client).append(input);

            String fullInputString = clientBuffers.get(client).toString();

            // Simple RESP parsing: split the command by CRLF.
            String[] lines = fullInputString.split("\r\n");
            // Check if we have a complete command array.
            if (lines.length >= 3 && lines[0].startsWith("*")) {
                String command = lines[2].trim().toUpperCase(); // The command is usually the 3rd line.

                boolean handled = false;

                // --- Command Handling Logic ---
                switch (command) {
                    case "PING" -> {
                        client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
                        handled = true;
                    }

                    case "ECHO" -> {
                        if (lines.length >= 5) {
                            String messageToEcho = lines[4];
                            String messageResponse = "$" + messageToEcho.length() + "\r\n" + messageToEcho + "\r\n";
                            client.write(ByteBuffer.wrap(messageResponse.getBytes()));
                            handled = true;
                        }
                    }

                    case "SET" -> {
                        if (lines.length >= 7) {
                            long expiry = Long.MAX_VALUE; // Default: no expiry.
                            // Check for PX (milliseconds) option.
                            if (lines.length >= 11 && lines[8].equalsIgnoreCase("PX")) {
                                expiry = Long.parseLong(lines[10]) + System.currentTimeMillis();
                            }
                            redisStore.put(lines[4], new RedisStoreObject(lines[6], expiry));
                            client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
                            handled = true;
                        }
                    }

                    case "GET" -> {
                        String keyToGet = lines[4];
                        RedisStoreObject storedObject = redisStore.get(keyToGet);
                        String getResponse;

                        if (storedObject == null) { // Key doesn't exist.
                            getResponse = "$-1\r\n";
                        } else {
                            long expiryTime = storedObject.expiration;
                            // Check if the key has a real expiry and if it's in the past.
                            if (expiryTime != Long.MAX_VALUE && expiryTime < System.currentTimeMillis()) {
                                redisStore.remove(keyToGet); // Remove the expired key.
                                getResponse = "$-1\r\n"; // Respond as if it doesn't exist.
                            } else {
                                String value = storedObject.value;
                                getResponse = "$" + value.length() + "\r\n" + value + "\r\n";
                            }
                        }
                        client.write(ByteBuffer.wrap(getResponse.getBytes()));
                        handled = true;
                    }

                    case "CONFIG" -> {
                        if (lines.length >= 7 && lines[4].equalsIgnoreCase("GET")) {
                            String configKey = lines[6];
                            String value = rdbConfig.get(configKey);
                            String resp;
                            if (value != null) {
                                // Format as a RESP array of [key, value]
                                resp = "*2\r\n$" + configKey.length() + "\r\n" + configKey + "\r\n$"
                                        + value.length() + "\r\n" + value + "\r\n";
                            } else {
                                // Format as a RESP array of [key, NIL]
                                resp = "*2\r\n$" + configKey.length() + "\r\n" + configKey + "\r\n$-1\r\n";
                            }
                            client.write(ByteBuffer.wrap(resp.getBytes()));
                            handled = true;
                        }
                    }

                    case "KEYS" -> {
                        if (lines.length >= 5 && lines[4].equals("*")) {
                            Set<String> keys = redisStore.keySet();
                            StringBuilder responseBuilder = new StringBuilder();
                            // Format as a RESP array of all keys.
                            responseBuilder.append("*").append(keys.size()).append("\r\n");
                            for (String k : keys) {
                                responseBuilder.append("$").append(k.length()).append("\r\n");
                                responseBuilder.append(k).append("\r\n");
                            }
                            client.write(ByteBuffer.wrap(responseBuilder.toString().getBytes()));
                            handled = true;
                        }
                    }
                }

                if (!handled) {
                    // Respond with an error for unknown commands.
                    String messageErrorResponse = "-ERR unknown or invalid command '" + command + "'\r\n";
                    client.write(ByteBuffer.wrap(messageErrorResponse.getBytes()));
                }

                // Clear the client's buffer now that a command has been processed.
                clientBuffers.get(client).setLength(0);
            }
        }
    }
}