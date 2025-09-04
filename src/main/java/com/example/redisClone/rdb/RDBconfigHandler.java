package com.example.redisClone.rdb;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

import com.example.redisClone.RedisStoreObject;

/**
 * Handles the parsing of Redis RDB files to load data into memory on startup.
 */
public class RDBconfigHandler {
    /**
     * Loads key-value pairs from an RDB file specified by the configuration.
     * @param rdbConfig The configuration object containing the directory and filename.
     * @return A HashMap representing the in-memory store.
     */
    public static HashMap<String, RedisStoreObject> loadRDB(RDBconfig rdbConfig) {
        String directory = rdbConfig.get("directory");
        String dataBaseFileName = rdbConfig.get("dbfilename");

        // --- File Handling ---
        File rdbFile = new File(directory, dataBaseFileName);
        if (directory == null || dataBaseFileName == null) {
            System.out.println("Missing --dir or --dbfilename arguments");
            return new HashMap<>();
        }
        if (!rdbFile.exists()) {
            System.out.println("RDB file not found, starting with empty DB.");
            return new HashMap<>();
        }

        System.out.println("Loading RDB from: " + rdbFile.getAbsolutePath());
        HashMap<String, RedisStoreObject> store = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(rdbFile); DataInputStream dis = new DataInputStream(fis)) {
            // --- RDB Header Parsing ---
            // Verify the "REDIS" magic string.
            byte[] magic = new byte[5];
            dis.readFully(magic);
            if (!"REDIS".equals(new String(magic))) {
                throw new IOException("Invalid RDB file format");
            }
            // Skip the 4-byte RDB version number.
            dis.skipBytes(4);

            // --- Main Parsing Loop ---
            long expiryMs = -1; // Temporary variable to hold expiry for the next key.
            int opcode;

            // Read the file byte by byte, interpreting each as an opcode or data.
            while ((opcode = dis.read()) != -1) {
                // An expiry opcode can come BEFORE another opcode.
                if (opcode == 0xFF) { // End Of File marker
                    break;
                }

                // Check for expiry opcodes first.
                if (opcode == 0xFC) { // Expiry in milliseconds (little-endian)
                    expiryMs = readLittleEndianLong(dis);
                    continue; // Loop again to read the value-type opcode that follows.
                } else if (opcode == 0xFD) { // Expiry in seconds (little-endian)
                    expiryMs = readLittleEndianInt(dis) * 1000L;
                    continue; // Loop again to read the next opcode.
                }

                // Handle metadata opcodes.
                if (opcode == 0xFE) { // DB selector
                    readLength(dis);
                    continue;
                }
                if (opcode == 0xFB) { // RESIZEDB hint
                    readLength(dis);
                    readLength(dis);
                    continue;
                }
                if (opcode == 0xFA) { // AUX auxiliary field
                    readString(dis); // Read and discard key
                    readString(dis); // Read and discard value
                    continue;
                }
                
                // Handle actual key-value data (opcode 0 means string encoding).
                if (opcode == 0) {
                    String key = readString(dis);
                    String value = readString(dis);

                    if (expiryMs != -1) {
                        // If we found an expiry, store the key with it.
                        store.put(key, new RedisStoreObject(value, expiryMs));
                        expiryMs = -1; // IMPORTANT: Reset for the next key.
                    } else {
                        store.put(key, new RedisStoreObject(value));
                    }
                }
            }
            System.out.println("Loaded " + store.size() + " keys from RDB");
            return store;

        } catch (IOException e) {
            System.out.println("Error loading RDB file: " + e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Reads a Redis length-prefixed string from the stream.
     */
    private static String readString(DataInputStream dis) throws IOException {
        int length = readLength(dis);
        if (length == 0) return "";
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    /**
     * Reads a Redis variable-length encoded integer.
     */
    private static int readLength(DataInputStream dis) throws IOException {
        int firstByte = dis.read();
        if (firstByte == -1) throw new IOException("Unexpected end of stream");
        int type = (firstByte & 0xC0) >> 6;
        switch (type) {
            case 0b00: return firstByte & 0x3F; // 6-bit length
            case 0b01: return ((firstByte & 0x3F) << 8) | dis.read(); // 14-bit length
            case 0b10: return dis.readInt(); // 32-bit length
            case 0b11: // Special encoded object, not a string length.
                int encoding = firstByte & 0x3F;
                if (encoding <= 2) { // Integer encodings of 1, 2, or 4 bytes.
                    dis.skipBytes(1 << encoding);
                    return 0; // Return 0 because this wasn't a string to read.
                }
                throw new IOException("Unhandled special encoding type: " + encoding);
            default: throw new IOException("Unknown length encoding type");
        }
    }

    /**
     * Reads an 8-byte little-endian long from the stream.
     * Required because RDB timestamps are little-endian, but Java's readLong is big-endian.
     */
    private static long readLittleEndianLong(DataInputStream dis) throws IOException {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) dis.read()) << (i * 8);
        }
        return value;
    }

    /**
     * Reads a 4-byte little-endian integer from the stream.
     */
    private static int readLittleEndianInt(DataInputStream dis) throws IOException {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= dis.read() << (i * 8);
        }
        return value;
    }
}