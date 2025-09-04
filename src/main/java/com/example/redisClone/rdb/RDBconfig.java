package com.example.redisClone.rdb;

/**
 * A simple data class to hold the RDB configuration passed via command-line arguments.
 */
public class RDBconfig {
    public String directory;
    public String dataBaseFileName;

    public RDBconfig(String directory, String dataBaseFileName) {
        this.directory = directory;
        this.dataBaseFileName = dataBaseFileName;
    }

    /**
     * Gets a configuration value by its key.
     * Handles both internal names ("directory") and Redis command names ("dir").
     * @param redisKey The name of the configuration parameter.
     * @return The configuration value, or null if not found.
     */
    public String get(String redisKey) {
        if (redisKey.equalsIgnoreCase("dir") || redisKey.equalsIgnoreCase("directory")) {
            return directory;
        }
        if (redisKey.equalsIgnoreCase("dbfilename")) {
            return dataBaseFileName;
        }
        return null;
    }
}