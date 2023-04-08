package com.ospn.server;


import com.ospn.data.RunnerData;
import com.ospn.service.SearchService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;




@Slf4j
public class ResultServer {

    static final ConcurrentLinkedQueue<RunnerData> msgList = new ConcurrentLinkedQueue<>();

    public static int getQueueSize(){
        return msgList.size();
    }

    public static void run(){
        while(true){
            try{
                if(msgList.isEmpty()){
                    synchronized (msgList){
                        msgList.wait();
                    }
                }
                RunnerData runnerData = msgList.poll();
                if (runnerData == null) {
                    continue;
                }

                SearchService.Result(runnerData);


            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    public static void push(RunnerData msg){
        msgList.offer(msg);
        synchronized (msgList){
            msgList.notify();
        }
    }


    public static boolean match(RunnerData runnerData) {

        try {

            String command = runnerData.json.getString("command");
            //String to = runnerData.json.getString("to");

            switch (command) {
                case "Result":
                    //log.info("Forward to result, command : " + command);
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

}
