package com.ospn.server;



import com.ospn.data.*;
import lombok.extern.slf4j.Slf4j;


import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;





@Slf4j
public class CommandWorkerServer extends CommandServerBase {

    static ExecutorService executorServiceCommand;
    final static ConcurrentLinkedQueue<RunnerData> runnerDataList = new ConcurrentLinkedQueue<>();

    public static void init(Properties prop) {
        int commandThreadCount = Integer.parseInt(prop.getProperty("commandThreadCount", "10"));
        log.info("Command thread count : " + commandThreadCount);
        executorServiceCommand = Executors.newFixedThreadPool(commandThreadCount);

        new Thread(CommandWorkerServer::run).start();
    }

    public static int getQueueSize(){
        return runnerDataList.size();
    }

    public static void push(RunnerData runnerData) {
        runnerDataList.offer(runnerData);
        synchronized (runnerDataList){
            runnerDataList.notify();
        }
        //log.info("worker MQ size : " + runnerDataList.size());
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

                
                // 开线程处理
                processCommand(runnerData, executorServiceCommand);

            }catch (Exception e){
                log.error("", e);
            }
        }

    }



}
