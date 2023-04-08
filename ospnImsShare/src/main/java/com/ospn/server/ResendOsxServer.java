package com.ospn.server;

import com.alibaba.fastjson.JSONObject;

import com.ospn.data.MessageData;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


import static com.ospn.OsnIMServer.db;



import static com.ospn.service.MessageService.sendOsxMessageNoSave;

@Slf4j
public class ResendOsxServer {


    public static final Object sleepLock = new Object();

    public static void init() {
        new Thread(ResendOsxServer::run).start();
    }

    public static void run(){

        // 每5分钟执行一次重发
        while(true){

            try{

                synchronized (sleepLock){
                    sleepLock.wait(5*60*1000);
                }

                ReSendOsx();


            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    // 重发机制
    private static void ReSendOsx(){
        int count = 20;
        int offset = 0;
        // 分页获取数据，并进行转发
        // 获取数据列表
        while (true){

            //long current = System.currentTimeMillis();
            // 转发6小时以内的数据
            //long time = current - ms_minute * 60 * 6;

            List<MessageData> messages = db.receipt.listUnreadPage(offset, count);
            if (messages.size() > 0){
                // 转发
                //log.info("[ReSendOsx] offset : " + offset + " count : " + messages.size());
                offset = (int)reSendOsx(messages);
            }
            if (messages.size() < count) {
                break;
            }
        }
    }

    private static long reSendOsx(List<MessageData> messages){

        long timePre = System.currentTimeMillis() - 1000 * 60 * 60 * 6;

        long maxId = 0;
        for (MessageData msg : messages){

            if (msg.id > maxId){
                maxId = msg.id;
            }

            if (msg.timeStamp < timePre) {
                // 删除数据
                db.receipt.delete(msg.hash);
                continue;
            }

            if (msg.data != null){
                
                JSONObject data = JSONObject.parseObject(msg.data);
                if (data != null){
                    sendOsxMessageNoSave(data);
                } else {
                    log.info("[reSendOsx] error : data != null");
                }

            } else {
                log.info("[reSendOsx] error : msg.data != null");
            }
        }
        return maxId;
    }


}
