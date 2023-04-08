package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.data.CommandData;
import com.ospn.data.RunnerData;
import com.ospn.data.SessionData;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;



import static com.ospn.core.IMData.getCommand;

@Slf4j
public class CommandServerBase {

    protected static void processCommand(RunnerData runnerData, ExecutorService executorService){

        executorService.execute(()->{

            process(runnerData);


            //PerUtils.record(PerUtils.P_handleMessage, true);
            /*try {

                SessionData sessionData = runnerData.sessionData;
                if (!sessionData.remote)
                    sessionData.timeHeart = System.currentTimeMillis();

                JSONObject json = runnerData.json;
                String command = json.getString("command");

                CommandData commandData = getCommand(command);
                if (commandData == null) {
                    commandData = getCommand("MessageForward");
                }

                sessionData.command = commandData;
                commandData.run.apply(runnerData);

            } catch (Exception e) {
                log.error("", e);
            }*/
            //PerUtils.record(PerUtils.P_handleMessage, false);
        });
    }

    private static void process(RunnerData runnerData){
        //PerUtils.record(PerUtils.P_handleMessage, true);
        try {

            //log.info("[process] id : " + runnerData.json.getString("id"));

            if (runnerData == null) {
                log.info("error, runnerData null");
                return;
            }
            if (runnerData.sessionData == null) {
                log.info("error, sessionData null");
                return;
            }

            SessionData sessionData = runnerData.sessionData;
            if (!sessionData.remote)
                sessionData.timeHeart = System.currentTimeMillis();

            JSONObject json = runnerData.json;
            if (json == null) {
                log.info("error, json null");
                return;
            }
            String command = json.getString("command");
            if (command == null) {
                log.info("error, command null");
                return;
            }


            CommandData commandData = getCommand(command);
            if (commandData == null) {
                commandData = getCommand("MessageForward");
            }

            sessionData.command = commandData;

            try {
                commandData.run.apply(runnerData);
            } catch (Exception e) {
                log.info(e.getMessage());
            }

        } catch (Exception e) {
            log.info(e.getMessage());
            log.info("error json : " + runnerData.json);
        }


        //PerUtils.record(PerUtils.P_handleMessage, false);
    }
}
