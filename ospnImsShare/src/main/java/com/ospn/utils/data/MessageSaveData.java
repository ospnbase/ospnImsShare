package com.ospn.utils.data;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class MessageSaveData {
    public List<JSONObject> msgList;
    public String MQ;

    public MessageSaveData(List<JSONObject> msgList, String MQ){
        this.msgList = msgList;
        this.MQ = MQ;
    }

    public MessageSaveData(JSONObject msg, String MQ){
        this.MQ = MQ;
        msgList = new ArrayList<>();
        msgList.add(msg);
    }
}
