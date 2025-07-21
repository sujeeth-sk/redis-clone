package com.example.redisClone.rdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

import com.example.redisClone.RedisStoreObject;

public class RDBconfigHandler {

    public static HashMap<String, RedisStoreObject> loadRDB(RDBconfig rdbConfig) {
        String directory = rdbConfig.get("directory");
        String dataBaseFileName = rdbConfig.get("dataBaseFileName");

        if (directory == null || dataBaseFileName == null) {
            System.out.println("Missing --dir or --dbFilename arguments");
            return null;
        }

        String filePath = directory + "/" + dataBaseFileName;
        File rdbFile = new File(filePath);

        if (!rdbFile.exists()) {
            System.out.println("RDF file not found: " + filePath);
            return null;
        }

        System.out.println("Loading RDB from: " + filePath);

        HashMap<String, RedisStoreObject> store = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(rdbFile)) {
            byte[] header = new byte[9];
            fis.read(header);

            String headerStr = new String(header);
            if (!headerStr.startsWith("REDIS")) {
                System.out.println("Invalid RDB file format");
                return null;
            }

            while (fis.available() > 0) {
                int keyLen = fis.read();
                if (keyLen == -1)
                    break;

                byte[] keyByte = new byte[keyLen];
                int keyRead = fis.read(keyByte);
                if (keyRead != keyLen)
                    break; // Ensures full key is read

                String key = new String(keyByte);

                int valLen = fis.read();
                if (valLen == -1)
                    break;

                byte[] valBytes = new byte[valLen];
                int valRead = fis.read(valBytes);
                if (valRead != valLen)
                    break; // Ensures full value is read

                String value = new String(valBytes);

                store.put(key, new RedisStoreObject(value));
            }

            System.out.println("Loaded " + store.size() + " keys from RDB");
            return store;

        } catch (IOException e) {
            System.out.println("Error loading RDB file: " + e.getMessage());
            return null;
        }
    }

}
