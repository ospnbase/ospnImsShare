package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.command2.CmdPushInfo;
import com.ospn.data.CommandData;
import com.ospn.data.MessageData;
import com.ospn.data.UserData;
import com.ospn.service.MessageService;
import com.ospn.utils.data.PushData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.ospn.OsnIMServer.db;


import static com.ospn.core.IMData.getUserData;
import static com.ospn.core.IMData.service;

@Slf4j
public class PushServer {

    //public static final ConcurrentLinkedQueue<PushData> msgList = new ConcurrentLinkedQueue<>();
    public static final ConcurrentLinkedQueue<CmdPushInfo> msgList2 = new ConcurrentLinkedQueue<>();
    /**
     * String : osnID
     * int : message count
     * **/
    public static final ConcurrentHashMap<String, Integer> unLoginMap = new ConcurrentHashMap<>();

    public static PushData currentPushData = null;

    public static long ms_second = 1000;
    public static long ms_minute = ms_second * 60;


    public static boolean stopFlag = false;


    public static int getQueueSize1(){
        return unLoginMap.size();
    }
    public static int getQueueSize2(){
        return msgList2.size();
    }

    public static void run(){
        while(true){
            try{
                if(msgList2.isEmpty()){
                    synchronized (msgList2){
                        msgList2.wait();
                    }
                }

                CmdPushInfo pushData = msgList2.poll();
                if (pushData == null) {
                    //log.info("error, queue poll is null");
                    continue;
                }

                // 发送指令给owner2
                //CmdPushInfo cmd = new CmdPushInfo(pushData.owner2, pushData.user, pushData.type);
                log.info("PUSH user:" + pushData.user+"  count:" + pushData.count);
                JSONObject msg = pushData.genMessage(service.osnID, pushData.owner2, service.osnKey, null);

                MessageService.sendOsxMessageNoSave(msg);


            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    //static Jedis jedisGlobal = null;
    public static void pushMessage(){

        log.info("PUSH BEGIN");

        Set<String> userSet = new HashSet<>();
        for (String user : unLoginMap.keySet()) {
            userSet.add(user);
        }

        for (String user : userSet) {
            Integer count = unLoginMap.get(user);
            if (count != null) {
                unLoginMap.remove(user);

                log.info("PUSH USER : " + user);

                try {

                    UserData userData = getUserData(user);
                    // 存入发送队列
                    CmdPushInfo cmd = new CmdPushInfo(userData.owner2, user, "Message", count.intValue());
                    sendPushInfo(cmd);

                } catch (Exception e) {
                    log.info(e.getMessage());
                }


            }
        }



        /*if (jedisGlobal == null) {
            jedisGlobal = db.message.getJedis();
            if (jedisGlobal == null) {
                log.info("error, jedis is null. ");
                return;
            }
        }

        if (currentPushData != null) {
            // 处理第一条消息
            if (!processPushData(jedisGlobal, currentPushData)){
                // 第一条消息都处理失败了，后面的就不处理了
                return;
            }
            // 处理完了需要把current 位置让出来
            currentPushData = null;
        }

        // 处理消息链
        processPushInfo(jedisGlobal);*/

    }

    public static boolean isUserOnline(String osnID) {
        UserData user = getUserData(osnID);
        if (user == null) {
            return false;
        }

        return isUserOnline(user);
    }
    public static boolean isUserOnline(UserData user) {
        if (user.session != null) {
            return user.session.isAlive;
        }
        return false;
    }

    public static void push(PushData data){

        if (stopFlag) {
            return;
        }

        // user 不在线，才发送推送消息
        if (!isUserOnline(data.user)){
            Integer count = unLoginMap.get(data.user);
            if (count == null) {
                count = new Integer(0);
            }
            count ++;
            log.info("PUSH to map user:" + data.user);
            unLoginMap.put(data.user, count);
        }
    }


    static void sendPushInfo(CmdPushInfo data){
        msgList2.offer(data);
        synchronized (msgList2){
            msgList2.notify();
        }
    }

    static boolean checkPushDataTime(PushData pushData){
        long current = System.currentTimeMillis();
        if (pushData.time + ms_second * 120 < current) {
            // 时间到了
            return true;
        }
        return false;
    }

    /*static void processPushInfo(Jedis jedis){

        while (true) {

            if (msgList.isEmpty()) {
                return;
            }
            PushData pushData = msgList.poll();
            if (pushData == null) {
                return;
            }
            if (!processPushData(jedis, pushData)){
                currentPushData = pushData;
                return;
            }
        }

    }*/

    /*static boolean processPushData(Jedis jedis, PushData pushData) {
        if (checkPushDataTime(pushData)){
            // 时间到了 检查消息是否被取走
            try {

                MessageData messageData = db.message.readFromRedis(jedis, pushData.msgHash);
                if (messageData != null) {
                    // 发送推送
                    //log.info("need push message." + pushData.toJson());
                    sendPushInfo(pushData);
                }
                return true;
            } catch (Exception e) {
                // 如果redis出错
                log.info(e.getMessage());
            }

        }
        return false;
    }*/





}
