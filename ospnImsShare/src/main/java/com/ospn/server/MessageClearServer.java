package com.ospn.server;

import com.ospn.data.MessageData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Properties;

import static com.ospn.OsnIMServer.db;


@Slf4j
public class MessageClearServer {

    public static final Object sleepLock = new Object();

    public static int saveDay;
    public static long saveDayMS;

    //static Jedis jedisGlobal = null;


    public static void init(Properties prop) {
        saveDay = Integer.parseInt(prop.getProperty("saveDay", "30"));
        log.info("saveDay : " + saveDay);
        saveDayMS = 1000 * 60 * 60 * 24 * (long)saveDay;
        log.info("saveDayMS : " + saveDayMS);
        new Thread(MessageClearServer::run).start();
    }

    public static void run(){

        // 每1小时执行一次
        while(true){

            try{

                synchronized (sleepLock){
                    sleepLock.wait(60*1000 * 60);
                }

                log.info("get redis.");
                Jedis jedis = db.message.getJedis();
                if (jedis == null) {
                    log.info("get jedis error.");
                    return;
                }



                clearOutTimeDataMessage(jedis);


                log.info("close redis.");
                db.message.closeJedis(jedis);


            }catch (Exception e){
                log.error("", e);
            }
        }
    }


    public static void clearOutTimeDataMessage(Jedis jedis){

        try {

            int count = 1;
            int offset = 0;

            // 分页获取数据，并进行转发
            // 获取数据列表
            while (true){


                List<MessageData> messages = db.message.listUnreadPage(offset, count);
                if (messages.size() > 0){
                    // 转发
                    for (MessageData msg : messages) {
                        long timestamp = msg.createTime;
                        if (timestamp == 0) {
                            timestamp = msg.timeStamp;
                        }

                        // 如果create Time + save day < 当前时间，则需要清除数据
                        if ( timestamp + saveDayMS < System.currentTimeMillis() ) {

                            log.info("timestamp : " + timestamp);

                            log.info("now : " + System.currentTimeMillis() );


                            log.info("clear message : " + msg.hash);
                            // 删除数据库
                            db.message.delete(msg.hash);

                            // 删除redis里的hash
                            db.message.deleteRedisMessage(jedis, msg.hash, msg.toID);

                        } else {
                            // 时间没到，则退出
                            return;
                        }
                    }


                }
                if (messages.size() < count) {
                    break;
                }
            }



        } catch (Exception e) {

        }


    }
}
