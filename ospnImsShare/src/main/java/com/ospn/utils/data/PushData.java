package com.ospn.utils.data;

import com.alibaba.fastjson.JSONObject;


public class PushData {
    public String owner2;
    public String user;
    public String type;
    public String msgHash;
    public long time;

    public JSONObject toJson(){

        JSONObject json = new JSONObject();
        json.put("owner2", owner2);
        json.put("user", user);
        json.put("type", type);
        json.put("msgHash", msgHash);
        json.put("time", time);
        return json;
    }
}
