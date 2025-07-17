package com.example.redisClone.rdb;

public class RDBconfig {
    public String directory;
    public String dataBaseFileName;

    public RDBconfig(String directory, String dataBaseFileName){
        this.directory = directory;
        this.dataBaseFileName = dataBaseFileName;
    }

    public String get(String redisKey){
        if(redisKey.equalsIgnoreCase("dir")) return directory;
        if(redisKey.equalsIgnoreCase("dataBaseFileName")) return dataBaseFileName;
        return null;
    }

}
