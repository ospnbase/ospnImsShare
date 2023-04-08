package com.ospn.server;

import com.ospn.data.RunnerData;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



import static com.ospn.core.IMData.isUser;



@Slf4j
public class CommandLoginServer extends CommandServerBase {

    static ExecutorService executorServiceCommand;
    static ConcurrentLinkedQueue<RunnerData> runnerDataList = new ConcurrentLinkedQueue<>();

    public static void init(Properties prop) {

        int loginThreadCount = Integer.parseInt(prop.getProperty("loginThreadCount", "8"));
        log.info("login Command thread count : " + loginThreadCount);
        executorServiceCommand = Executors.newFixedThreadPool(loginThreadCount);

        new Thread(CommandLoginServer::run).start();
    }

    public static int getQueueSize(){
        return runnerDataList.size();
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

        //log.info("login MQ size : " + runnerDataList.size());

    }

    public static void run(){

        while(true){
            try{
                if(runnerDataList.isEmpty()){
                    synchronized (runnerDataList){
                        runnerDataList.wait();
                    }
                }
                RunnerData runnerData = runnerDataList.poll();
                if (runnerData == null) {

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
            //String to = runnerData.json.getString("to");
            //log.info("json : " + runnerData.json);


            switch (command) {

                case "Login":
                case "LoginV2":
                case "GetLoginInfo":
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
