package com.ospn.server;


import com.alibaba.fastjson.JSONObject;
import com.ospn.command.CmdComplete;
import com.ospn.command.MsgData;
import com.ospn.common.ECUtils;
import com.ospn.data.MessageData;
import com.ospn.data.RunnerData;
import com.ospn.utils.data.CompleteData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ospn.OsnIMServer.db;
import static com.ospn.OsnIMServer.isInThisNode;


import static com.ospn.core.IMData.service;
import static com.ospn.service.MessageService.*;
import static com.ospn.utils.CryptUtils.wrapMessage;

@Slf4j
public class MessageProcessServer {

    public static final ConcurrentLinkedQueue<RunnerData> msgList = new ConcurrentLinkedQueue<>();
    public static final ConcurrentLinkedQueue<CompleteData> msgList2 = new ConcurrentLinkedQueue<>();

    public static boolean stopFlag = false; // stop push
    public static boolean stopFlag2 = false; // stop 从redis中进行complete
    public static boolean stopFlag3 = false; // 不进行签名校验

    public static int getQueueSize1(){
        return msgList.size();
    }
    public static int getQueueSize2(){
        return msgList2.size();
    }

    public static void push(RunnerData msg){

        if (stopFlag) {
            return;
        }

        msgList.offer(msg);
        synchronized (msgList){
            msgList.notify();
        }
    }

    public static boolean match(RunnerData runnerData) {

        try {

            String command = runnerData.json.getString("command");

            /*if (!runnerData.sessionData.remote) {
                sendReplyCode(runnerData, null, null);
            }*/
            //String to = runnerData.json.getString("to");

            switch (command) {
                case "Complete":
                    //log.info("Forward to complete, command : " + command);
                    push(runnerData);
                    return true;

                default:
                    break;
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }

        return false;
    }




    private static void run(){



        while(true){
            try{
                if(msgList.isEmpty()){

                    /*if (jedis != null) {
                        db.message.closeJedis(jedis);
                        jedis = null;
                    }*/

                    synchronized (msgList){
                        msgList.wait();
                    }
                }

                //log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("Thread complete : get jedis failed.");
                    Thread.sleep(1000);
                    continue;
                }
                //Jedis jedis2 = db.message.getJedisLoop();


                RunnerData runnerData = msgList.poll();
                if (runnerData != null){

                    String command = runnerData.json.getString("command");
                    switch (command) {
                        case "Complete":
                            complete(jedis, runnerData);
                            break;
                        default:
                            log.info("Unknown message : " + command);
                            break;
                    }
                }

                //log.info("close jedis.");
                db.message.closeJedis(jedis);

            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    private static void run2(){

        while(true){
            try{
                if(msgList2.isEmpty()){
                    synchronized (msgList2){
                        msgList2.wait();
                    }
                }

                CompleteData data = msgList2.poll();
                if (data == null){
                    log.info("error, queue poll data null");
                    continue;
                }

                //log.info("complete mq2 : " + msgList2.size());

                completeOff(null, data.hash, data.sign);

            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    public static void init(){
        new Thread(MessageProcessServer::run).start();
        new Thread(MessageProcessServer::run2).start();
    }




    private static void complete(Jedis jedis, RunnerData runnerData) {

        try {

            MsgData msg = new MsgData(runnerData.json);

            JSONObject contentJson = msg.getContent();
            if (contentJson == null){
                log.info("[completeRedis] error : getContent");
                return;
            }
            CmdComplete cmd = new CmdComplete(contentJson);

            // data的格式分两种，一种是有hash的

            if (cmd.hash != null && cmd.sign != null){

                //log.info("1 hash : " +cmd.hash+ " sign :" + cmd.sign);
                if (completeRedis(jedis, cmd.hash, cmd.sign) == 1){
                    //log.info("2 hash : " +cmd.hash+ " sign :" + cmd.sign);
                    push(cmd.hash, cmd.sign);

                    // redis中没有数据，扔给线程2处理
                    //completeOff(cmd.hash, cmd.sign);
                }
            }

            if (cmd.receipts != null){
                for (Map.Entry<String,String> entry : cmd.receipts.entrySet()){
                    //log.info("1 hash : " +cmd.hash+ " sign :" + cmd.sign);
                    if (completeRedis(jedis, entry.getKey(), entry.getValue()) == 1){
                        push(entry.getKey(), entry.getValue());
                        //push(cmd.hash, cmd.sign);
                        //completeOff(entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
            log.info("json : " + runnerData.json);
        }
        return;
    }

    private static void push(String hash, String sign) {

        //log.info("hash : " +hash+ " sign :" + sign);

        CompleteData data = new CompleteData(hash, sign);

        msgList2.offer(data);
        synchronized (msgList2){
            msgList2.notify();
        }
    }

    private static int completeRedis(Jedis jedis, String hash, String sign) {


        if (stopFlag2) {
            return 1;
        }
        //log.info("hash : " +hash+ " sign :" + sign);

        try {

            // 从redis里取数据，如果出错，则交给上层处理
            MessageData messageData = db.message.readFromRedis(jedis, hash);
            if (messageData == null){
                // 正常情况，是有redis里面没有数据的情况
                // 这种情况下返回1即可
                //log.info("[completeMessage] error: messageData == null");
                return 1;
            }

            String from = messageData.fromID;
            String to = messageData.toID;
            if (from == null || to == null){
                //容错 数据有问题
                log.info("[completeRedis] error: from == null || to == null");
                return 0;
            }

            if (!stopFlag3) {
                // 验证使用的是 message 中的 to 来验证
                if (!ECUtils.osnVerify(to, hash.getBytes(), sign)) {
                    log.info("[completeMessage] error: verify failed hash: " + hash);
                    return 0;
                }
            }

            // 删除掉redis中的数据
            String msg = db.message.completeFromRedis(jedis, hash, to);
            if (msg == null) {
                return 0;
            }

            // 检查是否需要外发
            if (!isInThisNode(from)) {
                // from不是本节点成员，更新回执数据
                //log.info("[completeMessage] sendOsxMessageNoSave ");

                // 制作msg 发送回执JSONObject

                JSONObject data = new JSONObject();
                data.put("hash", hash);
                data.put("sign", sign);
                JSONObject json = wrapMessage("Complete", service.osnID, from, data, service.osnKey, null);

                sendOsxMessageNoSave(json);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            // redis 打开失败了，就扔给线程2处理
            return 1;
        }

        return 2;
    }


}
