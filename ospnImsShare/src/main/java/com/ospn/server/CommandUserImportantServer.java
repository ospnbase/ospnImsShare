package com.ospn.server;


import com.ospn.data.RunnerData;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



import static com.ospn.core.IMData.*;

@Slf4j
public class CommandUserImportantServer extends CommandServerBase {

    static ExecutorService executorServiceCommand;
    final static ConcurrentLinkedQueue<RunnerData> runnerDataList = new ConcurrentLinkedQueue<>();

    public static int getQueueSize(){
        return runnerDataList.size();
    }

    public static void init(Properties prop) {

        int userCommandThreadCount = Integer.parseInt(prop.getProperty("userCommandThreadCount", "40"));
        log.info("User Command thread count : " + userCommandThreadCount);
        executorServiceCommand = Executors.newFixedThreadPool(userCommandThreadCount);

        new Thread(CommandUserImportantServer::run).start();
    }

    public static void push(RunnerData runnerData) {

        if (runnerData == null) {
            log.info("error, runnerData null");
            return;
        }

        runnerDataList.offer(runnerData);
        synchronized (runnerDataList){
            runnerDataList.notify();
        }
        //log.info("user message MQ size : " + runnerDataList.size());
    }

    public static void run(){

        while(true){
            try{
                if(runnerDataList.isEmpty()){
                    synchronized (runnerDataList){
                        runnerDataList.wait();
                    }
                }
                if (runnerDataList.isEmpty()) {
                    //log.info("queue is empty");
                    continue;
                }
                RunnerData runnerData = runnerDataList.poll();
                if (runnerData == null) {
                    //log.info("error, runnerData null");
                    continue;
                }
                processCommand(runnerData, executorServiceCommand);

            }catch (Exception e){
                log.error("", e);
            }
        }

    }

    public static boolean match(RunnerData runnerData) {
        try {

            String command = runnerData.json.getString("command");
            String to = runnerData.json.getString("to");
            //log.info("json : " + runnerData.json);


            switch (command) {

                case "Message":
                    if (isUser(to)) {
                        String from = runnerData.json.getString("from");
                        //log.info("[user message] from : " + from);
                        push(runnerData);
                        return true;
                    }
                    break;

                case "Login":
                case "LoginByOsnID":
                case "Heart":
                    //log.info("[important command] command : " + command);
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
