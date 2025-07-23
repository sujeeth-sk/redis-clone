package com.example.redisClone.rdb;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

import com.example.redisClone.RedisStoreObject;


public class RDBconfigHandler{
    public static HashMap<String, RedisStoreObject> loadRDB(RDBconfig rdbConfig){
        String directory = rdbConfig.get("directory");
        String dataBaseFileName = rdbConfig.get("dataBaseFileName");

        File rdbFile = new File(directory, dataBaseFileName);
        if(directory == null || dataBaseFileName == null){
            System.out.println("Missing --dir or --dbFilename arguments");
            return new HashMap<>();
        }
        
        if(!rdbFile.exists()){
            System.out.println("Loading RDB from: " + rdbFile.getAbsolutePath());
            return new HashMap<>();
        }
        
        System.out.println("Loading RDB from: " + rdbFile.getAbsolutePath());
        HashMap<String, RedisStoreObject> store = new HashMap<>();

        try(FileInputStream fis = new FileInputStream(rdbFile) ; DataInputStream dis = new DataInputStream(fis)){
            byte [] magic = new byte[5];
            dis.readFully(magic);

            if(!"REDIS".equals(new String(magic))){
                throw new IOException("Invalid RDB file: Magic string does not match 'REDIS'");
            }
            dis.skipBytes(4);

            //parse opcode and data until EOF

            int opcode;
            while((opcode = dis.read()) != -1){
                if(opcode == 0xFF){ //end of file
                    break;
                }

                if(opcode == 0xFE){ //database selector
                    readLength(dis);
                    continue;
                }

                if(opcode == 0xFB){ // resizedb
                    readLength(dis); //hash table size 
                    readLength(dis); //expore hash table size
                    continue;
                }

                if(opcode == 0xFA){ // aux field
                    readString(dis); //key
                    readString(dis); //value
                    continue;    
                }

                if(opcode == 0){
                    String key = readString(dis);
                    String value = readString(dis);
                    store.put(key, new RedisStoreObject(value));
                }

            }
            System.out.println("loaded " + store.size() + " keys form rdb");
            return store;

        } catch(IOException e){
            System.out.println("error lading RDB file: " + e.getMessage());
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = readLength(dis);
        if(length == 0) return "";
        byte [] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    private static int readLength(DataInputStream dis) throws IOException {
        int firstByte = dis.read();
        if(firstByte == -1){
            throw new IOException("unexpected end of stream while reading length");
        }
        int type = (firstByte & 0xC0) >> 6;
        switch (type) {
            case 0b00 -> {
                // The next 6 bits represent the length
                return firstByte & 0x3F;
            }
            case 0b01 -> {
                // The next 14 bits represent the length
                int secondByte = dis.read();
                return ((firstByte & 0x3F) << 8) | secondByte;
            }
            case 0b10 -> {
                // The next 4 bytes are a 32-bit integer length
                return dis.readInt();
            }
            case 0b11 -> {
                // Special format, not a length
                int encoding = firstByte & 0x3F;
                // For this stage, we only need to skip these values.
                // 0, 1, 2 represent integers of 1, 2, or 4 bytes.
                if (encoding == 0 || encoding == 1 || encoding == 2) {
                    dis.skipBytes(1 << encoding); // Skip 1, 2, or 4 bytes
                    return 0; // Return 0 as this was not a string length
                }
                // For other special types (like compressed strings), we would need more logic,
                // but this is enough to pass the current stage.
                throw new IOException("Unhandled special encoding type: " + encoding);
            }
            default -> throw new IOException("Unknown length encoding type");
        }
    }
}
