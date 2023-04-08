package com.ospn.server;


import com.alibaba.fastjson.JSONObject;
import com.ospn.data.MessageData;
import com.ospn.data.UserData;
import com.ospn.utils.data.MessageSaveData;
import com.ospn.utils.data.PushData;
import com.ospn.utils.data.RedisMessageInfo;
import com.ospn.utils.data.ResendData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ospn.OsnIMServer.db;
import static com.ospn.core.IMData.getUserData;


@Slf4j
public class MessageSaveServer {

    public static final ConcurrentLinkedQueue<MessageSaveData> dataList = new ConcurrentLinkedQueue<>();
    public static boolean stopFlag = false; //redis 转 db
    public static boolean stopFlag2 = false; // 存redis


    public static int getQueueSize(){
        return dataList.size();
    }

    public static void run(){

        while(true){
            try{
                if(dataList.isEmpty()){

                    synchronized (dataList){
                        dataList.wait();
                    }
                }

                //log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("Thread save server : get jedis failed.");
                    Thread.sleep(1000);
                    continue;
                }

                //Jedis jedis2 = db.message.getJedisLoop();

                MessageSaveData data = dataList.poll();
                if (data != null) {
                    if (!saveToRedis(jedis, data)){
                        // 存入redis 失败
                        log.info("error. saveToRedis");
                    }

                }


                //log.info("close redis.");
                db.message.closeJedis(jedis);

                // 将hash扔到重发队列中
                /*for (JSONObject json : data.msgList) {
                    ResendData resendData = new ResendData();
                    resendData.createTime = System.currentTimeMillis();
                    resendData.MQ = data.MQ;
                    resendData.hash = json.getString("hash");
                    MessageResendServer.push(resendData);
                }*/



            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    public static void push(MessageSaveData data){

        if (stopFlag2) {
            return;
        }

        dataList.offer(data);
        synchronized (dataList){
            dataList.notify();
        }
    }

    //public static Jedis jedisGlobal = null;
    public static void MessageRedisToDB(String MQ){

        /*if (jedisGlobal == null) {
            jedisGlobal = db.message.getJedis();
            if (jedisGlobal == null) {
                log.info("error, jedis is null. ");
                return;
            }
        }*/

        // 如果发生错误，可以暂时不存数据库
        //log.info("get redis.");
        Jedis jedis = db.message.getJedis();
        if (jedis == null) {
            //log.error("error : get jedis failed.");
            return;
        }


        try {

            RedisMessageInfo msgInfo = null;
            do {

                // 1. 从redis队列中取数据
                msgInfo = db.message.getNeedSaveMessage(jedis, MQ);
                if (msgInfo == null) {
                    // 队列中没有数据了
                    return;
                }
                if (msgInfo.state == -1) {
                    // 解析出错， continue
                    log.info("[error] analyse RedisMessageInfo error.");
                    continue;
                }
                // 2. 验证时间
                if (msgInfo.state == 0) {
                    // 时间没到，退出
                    return;
                }

                // 3. 存入db
                RedisToDB(jedis, msgInfo, MQ);



            } while (msgInfo != null);

        } catch (Exception e) {
            log.info(e.getMessage());
        } finally {

            //log.info("close redis.");
            db.message.closeJedis(jedis);
        }




    }

    static void RedisToDB(Jedis jedis, RedisMessageInfo infoMessage, String MQ) throws Exception {
        // 1. 获取数据
        MessageData msgData = db.message.readFromRedis(jedis, infoMessage.hash);
        if (msgData == null) {
            return;
        }

        // 2. 存入数据库
        if (!db.message.insert(jedis, msgData.toJson(), MQ)){
            // 存回redis队列
            db.message.backToRedisMQ(jedis, MQ, infoMessage.msgInfo);
            return;
        }

        // 3. 删除redis
        db.message.deleteRedisMessage(jedis, infoMessage.hash, msgData.toID);

        try {

            // 4.发送通知

            if (MQ.equalsIgnoreCase(db.message.MQCode)) {

                //JSONObject msg = JSONObject.parseObject(msgData.data);
                //String command = msg.getString("Command");

                if (msgData.cmd.equalsIgnoreCase("Message")
                        || msgData.cmd.equalsIgnoreCase("AddFriend")
                        || msgData.cmd.equalsIgnoreCase("JoinGroup")
                ) {

                    UserData udTo = getUserData(msgData.toID);
                    if (udTo.owner2 != null) {
                        if (!udTo.owner2.equalsIgnoreCase("")) {
                            PushData pushData = new PushData();
                            pushData.time = System.currentTimeMillis();
                            pushData.owner2 = udTo.owner2;
                            pushData.user = udTo.osnID;
                            pushData.type = msgData.cmd;
                            pushData.msgHash = msgData.hash;
                            log.info("[Push] MQ : "+MQ);
                            log.info("[Push] PushServer user:"+udTo.osnID);
                            PushServer.push(pushData);
                        }
                    }

                }



            }


        } catch (Exception e) {
            log.info("[Push] exception");
            log.info(e.getMessage());
        }



    }



    static boolean saveToRedis(Jedis jedis, MessageSaveData data){
        try {
            return db.message.insertMessagesInRedis(jedis, data.msgList, data.MQ);
        } catch (Exception e) {
            log.info(e.getMessage());
        }

        return false;
    }


}
