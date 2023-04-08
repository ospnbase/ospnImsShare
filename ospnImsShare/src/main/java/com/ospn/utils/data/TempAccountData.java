package com.ospn.utils.data;

public class TempAccountData {
    public String osnID;
    public String osnKey;
    public long staleTime;

    public static long ms_hour = 3600000;
    public static long ms_minute = 60000;
    public static long cycleTime = ms_minute * 10;

    public TempAccountData(){}

    public TempAccountData(String id){
        osnID = id;
        // 默认有效期，10分钟
        staleTime = System.currentTimeMillis() + cycleTime;
    }
    public TempAccountData(String id, String key){
        osnID = id;
        osnKey = key;
        // 默认有效期，10分钟
        staleTime = System.currentTimeMillis() + cycleTime;
    }

}
