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

        if(directory == null || dataBaseFileName == null){
            System.out.println("Missing --dir or --dbFilename arguments");
            return new HashMap<>();
        }
        File rdbFile = new File(directory, dataBaseFileName);
        
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
            return new HashMap<>();
        }
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = readLength(dis);
        byte [] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    private static int readLength(DataInputStream dis) throws IOException {
        int firstByte = dis.read();
        int type = (firstByte & 0xC0) >> 6;
        if(type == 0){
            return firstByte & 0x3F;
        } else if(type == 1){
            int secondByte = dis.read();
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else {
            return dis.readInt();
        }
    }
}
