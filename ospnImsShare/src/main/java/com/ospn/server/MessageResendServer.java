package com.ospn.server;


import com.alibaba.fastjson.JSONObject;
import com.ospn.data.MessageData;
import com.ospn.data.RunnerData;
import com.ospn.data.UserData;
import com.ospn.utils.data.ResendData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ospn.OsnIMServer.db;


import static com.ospn.core.IMData.getUserData;
import static com.ospn.service.MessageService.sendClientMessageNoSave;
import static com.ospn.service.MessageService.sendOsxMessageNoSave;
import static com.ospn.utils.db.DBMessage.MQCode;
import static com.ospn.utils.db.DBMessage.MQOSX;

@Slf4j
public class MessageResendServer {

    static final ConcurrentLinkedQueue<ResendData> msgList = new ConcurrentLinkedQueue<>();
    static final ConcurrentLinkedQueue<ResendData> osxList = new ConcurrentLinkedQueue<>();
    static ResendData current;
    static ResendData current2;

    // 目前是不转发状态
    public static boolean stopFlag = true;
    public static boolean stopFlag2 = true;

    public static int getQueueSize1(){
        return msgList.size();
    }
    public static int getQueueSize2(){
        return osxList.size();
    }

    public static void init(){
        new Thread(MessageResendServer::run).start();
    }

    public static void push(ResendData msg){

        if (msg.MQ.equalsIgnoreCase(MQCode)){
            if (stopFlag) {
                return;
            }
            msgList.offer(msg);
            synchronized (msgList){
                msgList.notify();
            }
        } else if (msg.MQ.equalsIgnoreCase(MQOSX)) {
            if (stopFlag2) {
                return;
            }
            osxList.offer(msg);
            synchronized (osxList){
                osxList.notify();
            }
        }
    }

    public static void run(){



        // 重发队列规则，消息15秒没有被取走，需要进行重发

        while(true){
            try{
                if(msgList.isEmpty()){
                    synchronized (msgList){
                        msgList.wait();
                    }
                }

                log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("Thread resend : get jedis failed.");
                    Thread.sleep(1000);
                    continue;
                }
                //Jedis jedis2 = db.message.getJedisLoop();

                ResendData resendData = msgList.poll();
                if (resendData != null) {

                    // 15秒没有取走，则进行重发
                    if (resendData.createTime + 1000 * 15 > System.currentTimeMillis()){
                        Thread.sleep(1000 * 15);
                    }

                    resend(jedis, resendData.hash);
                }



                log.info("get redis.");
                db.message.closeJedis(jedis);

            }catch (Exception e){
                log.error("", e);
            }
        }
    }


    /**
     * ResendRedisMessage 重发osx数据
     * **/
    static Jedis jedisGlobal = null;
    public static void ResendRedisMessage(){

        if (jedisGlobal == null) {
            jedisGlobal = db.message.getJedis();
            if (jedisGlobal == null) {
                log.info("error, jedis is null. ");
                return;
            }
        }


        try {

            if (current2 != null){
                if (current2.createTime + 1000 * 60 * 5 < System.currentTimeMillis()){
                    //resend
                    resendOsx(jedisGlobal, current2.hash);
                    // 移除current
                    current2 = null;
                } else {
                    // 第一个的时间都没到，肯定后面的时间也没到
                    return;
                }
            }

            while (true) {

                if (osxList.isEmpty()){
                    break;
                }

                ResendData data = osxList.poll();
                // 每5分钟重发一次
                if (data.createTime + 1000 * 60 * 5 > System.currentTimeMillis()){
                    // 时间没到，不用转发
                    current2 = data;
                    break;
                }
                resendOsx(jedisGlobal, data.hash);
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }

    }

    static void resend(Jedis jedis, String hash) {

        // 重发的机制，当user在线，但是数据没有被取走时，重发
        try {
            MessageData messageData = db.message.readFromRedis(jedis, hash);
            if (messageData != null){
                // 检查 用户是否在线
                UserData userData = getUserData(messageData.toID);
                if (userData != null){
                    if (userData.session != null) {
                        if (userData.session.isAlive) {
                            // 用户在线
                            JSONObject json = JSONObject.parseObject(messageData.data);
                            sendClientMessageNoSave(userData.session, json);
                            // 重发以后，扔到msgList队列中，直到complete完成
                            ResendData resendData = new ResendData();
                            resendData.hash = hash;
                            resendData.createTime = System.currentTimeMillis();
                            resendData.MQ = MQCode;
                            push(resendData);
                        }

                    }
                }
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }

    }

    static void resendOsx(Jedis jedis, String hash) {
        try {
            MessageData messageData = db.message.readFromRedis(jedis, hash);
            if (messageData != null){

                log.info("[resned] osx : " + messageData.toID);

                JSONObject json = JSONObject.parseObject(messageData.data);
                sendOsxMessageNoSave(json);
                ResendData resendData = new ResendData();
                resendData.hash = hash;
                resendData.createTime = System.currentTimeMillis();
                resendData.MQ = MQOSX;
                push(resendData);

            }
        } catch (Exception e) {

        }
    }

}
