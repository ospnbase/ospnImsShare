package com.ospn.utils.data;

import com.alibaba.fastjson.JSONObject;

import static com.ospn.utils.db.DBMessage.deltaTime;

public class RedisMessageInfo {
    public int state = -1;  // 0 时间未到， 1 时间到了
    public long time;
    public String hash;
    public String msgInfo;

    public RedisMessageInfo(){

    }
    public RedisMessageInfo(String msgInfo){

        this.msgInfo = msgInfo;
        try {
            JSONObject infoJson = JSONObject.parseObject(msgInfo);
            hash = infoJson.getString("hash");
            time = infoJson.getLong("time");

            // 判断时间是否超过
            long currentTime = System.currentTimeMillis();
            if (time + deltaTime < currentTime){
                // 转存时间到了
                state = 1;
            } else {
                state = 0;
            }

        } catch (Exception e) {

        }

    }

    public String RedisMessageInfoString(){
        JSONObject json = new JSONObject();
        json.put("time", time);
        json.put("hash", hash);
        return json.toString();
    }
}
