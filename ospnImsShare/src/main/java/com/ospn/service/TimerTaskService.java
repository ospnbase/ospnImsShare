package com.ospn.service;


import com.ospn.core.IMData;
import com.ospn.server.MessageResendServer;
import com.ospn.server.MessageSaveServer;
import com.ospn.server.PushServer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ospn.OsnIMServer.db;

import static com.ospn.server.MessageResendServer.ResendRedisMessage;


@Slf4j
public class TimerTaskService {

    public void run(){
        ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(10); // 10 为线程数量
        // 执行任务
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            //log.info("[TaskService] run Schedule : "+ new Date());
            try {

                // 超时的数据转存到数据库中
                if (!MessageSaveServer.stopFlag) {
                    MessageSaveServer.MessageRedisToDB(db.message.MQOSX);
                    MessageSaveServer.MessageRedisToDB(db.message.MQCode);
                }



                TempAccountService.clean();

            } catch (Exception e) {
                log.info("Task 1 : " + e.getMessage());
            }


        }, 60, 60 * 2, TimeUnit.SECONDS); // 2分钟执行一次

        // 执行任务 2
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            //log.info("[TaskService] run Schedule2 : "+ new Date());
            try {

                PushServer.pushMessage();

                /*if (!PushServer.stopFlag){
                    PushServer.pushMessage();
                }*/

                // 重发
                // 这个是重发redis里的   // 目前是关闭状态
                /*if (!MessageResendServer.stopFlag){
                    ResendRedisMessage();
                }*/
                // 这个是重发数据库里的
                //MessageService.ReSendOsx();



            } catch (Exception e) {
                log.info("Task 2 : " + e.getMessage());
            }

        }, 60*5, 60*10, TimeUnit.SECONDS); // 10分钟执行一次



        // 执行任务 3
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            //log.info("[TaskService] run Schedule3 : "+ new Date());
            try {

                // 清理session
                IMData.cleanOutTimeSession();
                //db.keepAlive();




            } catch (Exception e) {
                log.info("Task 3 : " + e.getMessage());
            }



        }, 60, 10, TimeUnit.SECONDS); // 10秒执行一次

        /*// 执行任务 4
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            //log.info("[TaskService] run Schedule3 : "+ new Date());
            try {

                //ReplyMessage();

            } catch (Exception e) {
                log.info("Task 4 : " + e.getMessage());
            }

        }, 60, 60, TimeUnit.SECONDS); // 60秒执行一次*/

    }
}
