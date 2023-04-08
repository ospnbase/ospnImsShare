package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.data.*;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ospn.OsnIMServer.db;


import static com.ospn.core.IMData.*;
import static com.ospn.service.MessageService.sendClientMessageNoSave;

@Slf4j
public class MessageSyncServer {

    public static final ConcurrentLinkedQueue<String> msgSyncList = new ConcurrentLinkedQueue<>();
    public static final ConcurrentLinkedQueue<String> msgSyncList2 = new ConcurrentLinkedQueue<>();

    public static int getQueueSize1(){
        return msgSyncList.size();
    }
    public static int getQueueSize2(){
        return msgSyncList2.size();
    }

    public static void init(){
        new Thread(MessageSyncServer::run).start();
        new Thread(MessageSyncServer::run2).start();
    }

    public static void run(){



        while(true){

            try{

                if (msgSyncList.isEmpty()) {

                    // 队列为空时释放jedis

                    synchronized (msgSyncList){
                        msgSyncList.wait();
                    }
                }

                //log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("Thread sync from redis : get jedis failed.");
                    Thread.sleep(1000);
                    continue;
                }

                //Jedis jedis2 = db.message.getJedisLoop();



                String user = msgSyncList.poll();
                if (user != null){
                    //log.info("sync redis : " + msgSyncList.size());
                    syncMessageFromRedis(user, jedis);
                }

                //log.info("close redis.");
                db.message.closeJedis(jedis);
                //Thread.sleep(10);

            }catch (Exception e){
                log.error("error", e);
            }
        }
    }

    public static void run2(){

        while(true){

            try{
                if(msgSyncList2.isEmpty()){

                    // 队列为空时释放

                    synchronized (msgSyncList2){
                        msgSyncList2.wait();
                    }
                }

                //log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("Thread sync from db : get jedis failed.");
                    Thread.sleep(1000);
                    continue;
                }
                //Jedis jedis2 = db.message.getJedisLoop();
                //log.info("sync db : " + msgSyncList2.size());

                String user = msgSyncList2.poll();
                if (user != null){
                    log.info("sync db begin : " + user);

                    syncMessageFromDB(jedis, user);

                    log.info("sync db end : " + user);
                }

                //log.info("close redis.");
                db.message.closeJedis(jedis);
                //



            }catch (Exception e){
                log.error("", e);
            }
        }
    }



    static void syncMessageFromRedis(String user, Jedis jedis) {

        log.info("sync user : " + user);
        /*if (jedis != null) {
            return;
        }*/

        try {

            UserData userData = getUserData(user);
            if (userData == null){
                return;
            }


            SessionData session = userData.session;
            if (session == null){
                return;
            }

            if (!session.isAlive){
                return;
            }

            if (session.syncTime + 1000 * 10 > System.currentTimeMillis()){
                // 每条链接同步时间是5分钟
                return;
            }
            session.syncTime = System.currentTimeMillis();

            Set<String> hashSet = db.message.listMessageHashFromRedis(jedis, user);
            if (hashSet == null){
                return;
            }

            for (String hash : hashSet) {

                MessageData msg = db.message.readFromRedis(jedis, hash);
                if (msg != null){
                    sendToUser(session, msg);
                }

            }




        } catch (Exception e) {

            log.info(e.getMessage());

        }

    }

    static void syncMessageFromDB(Jedis jedis, String user) {

        try {

            UserData userData = getUserData(user);
            if (userData == null){
                log.info("no user : " + user);
                return;
            }
            SessionData session = userData.session;
            if (session == null){
                log.info("user not online");
                return;
            }

            if (!session.isAlive){
                log.info("user not online 2");
                return;
            }

            /*if (session.sync){
                // 已经同步过的，就不再同步了
                return;
            }
            session.sync = true;*/

            if (session.syncDbTime != 0) {
                log.info("you have already sync.");
                return;
            }

            session.syncDbTime = System.currentTimeMillis();

            // 1小时以内已经同步过了，就不再从数据库中同步了
            /*if (userData.syncTime + 1000 * 60 * 15 > System.currentTimeMillis()){
                return;
            }
            userData.syncTime = System.currentTimeMillis();*/


            Set<String> hashList = db.message.listRedisUserMessageMap(jedis, user);
            if (hashList != null) {
                for (String hash : hashList) {

                    try {

                        MessageData msgData = db.message.read(hash);
                        if (msgData != null) {
                            sendToUser(session, msgData);
                        } else {
                            // 删除redis里的数据
                            db.message.deleteRedisMessageMap(jedis, user, hash);
                        }

                    } catch (Exception e) {
                        log.info(e.getMessage());
                    }

                }
            }


            //log.info("user id : " + userData.osnID);


            /*int count = 100;
            int offset = 0;

            while (true){

                long time1 = System.currentTimeMillis();

                //log.info("offset : " + offset);
                List<MessageData> messages = db.message.listUnreadPage(user, offset, count);

                //log.info("page read (" + (System.currentTimeMillis()-time1) +")");

                //log.info("message count : " + messages.size());
                if (messages.size() > 0){
                    offset = sendToUser(session, messages);
                    //log.info("offset2 : " + offset);

                    *//*if (offset == 0) {
                        break;
                    }*//*
                }
                if (messages.size() < count) {
                    break;
                }
            }*/

            /*List<MessageData> messages = db.message.listFromRedis(user);
            if (messages.size() > 0){
                sendToUser(session, messages);
            }*/

        } catch (Exception e) {

            log.info(e.getMessage());

            /*msgSyncList.offer(user);
            synchronized (msgSyncList){
                msgSyncList.notify();
            }*/
        }

    }


    static int sendToUser(SessionData session, List<MessageData> messages) {

        long maxId = 0;
        for (MessageData msg : messages){
            if (msg.id > maxId){
                maxId = msg.id;
            }
            if (msg.data != null){
                JSONObject data = JSONObject.parseObject(msg.data);
                if (data != null){
                    sendClientMessageNoSave(session, data);
                    //Thread.sleep(5);
                } else {
                    log.info("[sendToUser] error : data != null");
                }

            } else {
                log.info("[sendToUser] error : msg.data != null");
            }
        }
        //log.info("max id : " + maxId);
        return (int)maxId;
    }

    static void sendToUser(SessionData session, MessageData msg) {

        try {

            JSONObject data = JSONObject.parseObject(msg.data);
            if (data != null){
                sendClientMessageNoSave(session, data);
            } else {
                log.info("[sendToUser] error : data != null");
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }

    }

    public static void push(String user){

        if (!msgSyncList.contains(user)){
            msgSyncList.offer(user);
            synchronized (msgSyncList){
                msgSyncList.notify();
            }
        }

        if (!msgSyncList2.contains(user)){
            msgSyncList2.offer(user);
            synchronized (msgSyncList2){
                msgSyncList2.notify();
            }
        }

    }

    public static void pushOnlyCache(String user){

        if (!msgSyncList.contains(user)){
            msgSyncList.offer(user);
            synchronized (msgSyncList){
                msgSyncList.notify();
            }
        }
    }


}
