package com.ospn.service;

import com.alibaba.fastjson.JSONObject;
import com.ospn.command.MsgData;
import com.ospn.data.RunnerData;
import com.ospn.data.UserData;
import lombok.extern.slf4j.Slf4j;


import static com.ospn.core.IMData.getUserData;
import static com.ospn.service.MessageService.*;

@Slf4j
public class SearchService {

    public static Void FindObj(RunnerData runnerData){
        try {
            //log.info("[FindObj] begin.");

            MsgData msg = new MsgData(runnerData.json);

            if (getUserData(msg.from) == null){
                // 不是本节点的还要来搜索？
                return sendReplyCode(runnerData, null, null);
            }

            JSONObject content = msg.getContent();
            //log.info("[FindObj] content :" + content);

            if (content != null){

                String tempAcc = content.getString("from");

                if (tempAcc != null){
                    // 设置临时账户，有效期10分钟
                    TempAccountService.setTempAccount(msg.from, tempAcc);
                    //发送查询命令
                    sendOsxMessageNoSave(content);
                }

            }

        } catch (Exception e){
            log.error("[FindObj] error json :" + runnerData.json);
        }


        return sendReplyCode(runnerData, null, null);
    }

    public static Void Result(RunnerData runnerData) {

        try{
            //log.info("[Result] begin.");
            MsgData msg = new MsgData(runnerData.json);

            // 获取真实的id
            String realAcc = TempAccountService.getAccount(msg.to);
            if (realAcc == null){
                return null;
            }

            UserData udReal = getUserData(realAcc);
            //log.info("[Result] udReal : " + udReal.osnID);

            if (udReal != null){
                // 只发给在线的用户，不在线就扔掉
                sendClientMessageNoSave(udReal, runnerData.json);
            }

            // 该消息不需要回执

        } catch (Exception e){
            log.error("", e);
            log.error("[UserService::Result] error json : " + runnerData.json);
        }

        return null;
    }


}
