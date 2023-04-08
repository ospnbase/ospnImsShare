package com.ospn.server;


import com.ospn.data.RunnerData;
import lombok.extern.slf4j.Slf4j;


import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



import static com.ospn.core.IMData.isGroup;

@Slf4j
public class CommandGroupImportantServer extends CommandServerBase {

    static ExecutorService executorServiceCommand;
    final static ConcurrentLinkedQueue<RunnerData> runnerDataList = new ConcurrentLinkedQueue<>();

    public static int getQueueSize(){
        return runnerDataList.size();
    }

    public static void init(Properties prop) {

        int groupCommandThreadCount = Integer.parseInt(prop.getProperty("groupCommandThreadCount", "25"));
        log.info("Group Command thread count : " + groupCommandThreadCount);
        executorServiceCommand = Executors.newFixedThreadPool(groupCommandThreadCount);

        new Thread(CommandGroupImportantServer::run).start();
    }

    public static void push(RunnerData runnerData) {
        runnerDataList.offer(runnerData);
        synchronized (runnerDataList){
            runnerDataList.notify();
        }

        //log.info("group MQ size : " + runnerDataList.size());
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
            String to = runnerData.json.getString("to");
            String from = runnerData.json.getString("from");
            //log.info("[HandleMessage]command : " +command+ " from : " +from);
            //log.info("[HandleMessage] hash code : " + runnerData.sessionData.ctx.hashCode());

            switch (command) {
                case "Message":
                    if (isGroup(to)) {
                        //log.info("Forward to group important, command : " + command);
                        push(runnerData);
                        return true;
                    }
                    break;

                default:
                    break;
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }

        return false;
    }







}
