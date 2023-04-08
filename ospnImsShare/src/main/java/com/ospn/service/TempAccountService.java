package com.ospn.service;

import com.ospn.command.CmdSetTemp;
import com.ospn.command.MsgData;
import com.ospn.data.RunnerData;
import com.ospn.utils.data.TempAccountData;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ospn.OsnIMServer.popOsnID;
import static com.ospn.OsnIMServer.pushOsnID;

import static com.ospn.core.IMData.service;
import static com.ospn.service.MessageService.sendReplyCode;

@Slf4j
public class TempAccountService {
    public static ConcurrentHashMap<String, TempAccountData> accMap = new ConcurrentHashMap<>();
    // key 为临时账号

    public static void clean(){
        List<String> cleanList = new ArrayList<>();
        for (String temp : accMap.keySet()){
            TempAccountData acc = accMap.get(temp);
            if (acc.staleTime < System.currentTimeMillis()){
                cleanList.add(temp);
            }
        }

        for (String temp : cleanList){
            accMap.remove(temp);
            // 从connector中移除temp
            popOsnID(temp);
        }
    }

    public static void setTempAccount(String user, String temp){

        accMap.remove(temp);
        TempAccountData tempAccountData = new TempAccountData(user);
        accMap.put(temp, tempAccountData);

        // 设置到connector中
        pushOsnID(temp);
    }

    public static String getAccount(String temp){
        TempAccountData acc = accMap.get(temp);
        if (acc == null){
            return null;
        }
        return acc.osnID;
    }

    public static Void SetTemp(RunnerData runnerData){

        try {

            MsgData msg = new MsgData(runnerData.json);
            CmdSetTemp cmd = new CmdSetTemp(msg.getContent(service.osnKey));
            log.info("[SetTemp] temp account : " + cmd.tempAcc);
            if (msg.from != null && cmd.tempAcc != null){
                setTempAccount(msg.from, cmd.tempAcc);
            }
        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[SetTemp] error json : "+runnerData.json);
        }
        sendReplyCode(runnerData, null, null);
        return null;
    }


}
