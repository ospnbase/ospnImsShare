package com.ospn.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.command.CmdComplete;
import com.ospn.command.MsgData;
import com.ospn.common.ECUtils;
import com.ospn.core.IMData;
import com.ospn.data.*;
import com.ospn.server.MessageSaveServer;
import com.ospn.server.OsnWSServer;
import com.ospn.utils.data.MessageSaveData;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.ospn.OsnIMServer.db;
import static com.ospn.OsnIMServer.isInThisNode;
import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.E_dataOverrun;
import static com.ospn.data.Constant.E_userError;
import static com.ospn.data.MessageData.toMessageData;
import static com.ospn.utils.CryptUtils.*;
import static com.ospn.utils.db.DBMessage.MQCode;
import static com.ospn.utils.db.DBMessage.MQOSX;

@Slf4j
public class MessageService {

    static String[] noReplyArray = {
            "GetUserInfo",
            "GetGroupSign",
            "GetOwnerSign",
            "GetMemberInfo",
            "GetMemberZone",
            "GetGroupInfo"
    };
    static List noReplyList = Arrays.asList(noReplyArray);

    static String[] noSaveArray = {
            "GetUserInfo",
            "GetFriendList",
            "GetFriendInfo",
            "GetGroupList",
            "GetGroupInfo",
            "GetGroupSign",
            "GetOwnerSign",
            "GetMemberInfo",
            "GetMemberZone",
            "GetServiceInfo",
            "AgreeFriend"
    };
    static List noSaveList = Arrays.asList(noSaveArray);


    // 外发, 不存数据
    public static void sendOsxMessageNoSave(JSONObject msg){
        if (IMData.standAlone)
            return;
        OsnIMServer.osxSender.send(msg);
    }
    // 外发, 存数据
    public static void sendOsxMessage(JSONObject msg){


        String command = msg.getString("command");
        if (!noSaveList.contains(command)){
            // 不在 不存列表中，需要存储
            MessageSaveServer.push(new MessageSaveData(msg, MQOSX));
            //db.message.insertInRedis(toMessageData(msg, 0), MQOSX);
        }
        sendOsxMessageNoSave(msg);
    }

    // 内发， 不存数据
    public static void sendClientMessageNoSave(UserData userData, JSONObject msg) {
        if (userData != null) {
            if (userData.session != null){
                //log.info("[sendClientMessageNoSave] sendClientMessageNoSave");
                sendClientMessageNoSave(userData.session, msg);
            }
        }
    }
    public static boolean sendClientMessageNoSave(SessionData sessionData, JSONObject msg){
        msg.remove("saved");
        msg.remove("aeskey");
        if (sessionData.webSock) {
            //log.info("[sendClientMessageNoSave] webSock");
            // 网页端
            if (msg.containsKey("ecckey") && sessionData.user != null){
                toAesMessage(msg, sessionData.user.osnKey);
            }
            //log.info("[sendClientMessageNoSave] OsnWSServer.sendClientJson");
            OsnWSServer.sendClientJson(sessionData, msg);
        } else {

            //sessionData.ctx.writeAndFlush(msg);
            if (sessionData.ctx.channel().isWritable()) {
                //log.info("msg : " + msg);
                //sessionData.ctx.writeAndFlush(msg);
                sessionData.ctx.channel().writeAndFlush(msg);
                return true;
            } else {
                log.info("channel write able is false");
            }

        }
        return false;
    }



    // 内发， 存数据
    public static void sendClientMessage(UserData userData, JSONObject msg){
        if (userData != null){
            MessageSaveServer.push(new MessageSaveData(msg, MQCode));
            //db.message.insertInRedis(toMessageData(msg, 0));
            sendClientMessageNoSave(userData, msg);
        }
    }


    // 从ims发出去的消息，都是不需要存的，只有少量需要存
    public static void sendMessageNoSave(String osnID, JSONObject msg){
        if (isInThisNode(osnID)){
            // 内发
            if (isGroup(osnID)){
                // 扔给GroupService处理
                // 不能给本节点群组发送
            } else if (isUser(osnID)){
                // 扔给UserService处理
                UserData ud = getUserData(osnID);
                sendClientMessageNoSave(ud, msg);
            }
            // 如果是小程序，扔给小程序处理

        } else {
            // 外发
            sendOsxMessageNoSave(msg);
        }

    }
    // 不知道内发还是外发， 存数据
    public static void sendMessage(String osnID, JSONObject msg){
        if (isInThisNode(osnID)){
            // 内发
            if (isGroup(osnID)){
                // 扔给GroupService处理
                // 不能给本节点群组发送
            } else if (isUser(osnID)){
                // 扔给UserService处理
                UserData ud = getUserData(osnID);
                sendClientMessage(ud, msg);
            }
            // 如果是小程序，扔给小程序处理

        } else {
            // 外发
            sendOsxMessage(msg);
        }

    }


    // 保存完成的消息，一般是用户发送群消息时使用
    public static boolean saveCompleteMessage(JSONObject msg){
        return db.message.insertCompleteInRedis(toMessageData(msg, 0));
    }

    // 处理消息回执
    public static void completeMessage(JSONObject msg) {
        try {
            String command = msg.getString("command");
            String osnID = msg.getString("to");        // message 中的 from
            String userID = msg.getString("from");     // message 中的 to

            //log.info("[completeMessage] begin");

            JSONObject data = takeMessage(msg);
            if (data == null){
                log.info("[completeMessage] error : takeMessage");
                return;
            }


            // data的格式分两种，一种是有hash的
            String hash = data.getString("hash");
            String sign = data.getString("sign");       //message hash
            JSONObject receiptList = data.getJSONObject("receiptList");

            if (hash != null && sign != null){
                completeMessage(hash, sign);
            }

            if (receiptList != null){

                Map<String, String> receipts = JSONObject.toJavaObject(receiptList, Map.class);
                if (receipts != null){

                    //log.info("[completeMessage] receipts : " + receipts.size());
                    for (Map.Entry<String,String> entry : receipts.entrySet()){

                        completeMessage(entry.getKey(), entry.getValue());

                    }
                }
            }


        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static Void complete(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);

            JSONObject contentJson = msg.getContent();
            if (contentJson == null){
                log.info("[complete] error : getContent");
                return null;
            }
            CmdComplete cmd = new CmdComplete(contentJson);

            // data的格式分两种，一种是有hash的

            if (cmd.hash != null && cmd.sign != null){
                if (completeMessage(cmd.hash, cmd.sign) == 1){
                    completeOff(null, cmd.hash, cmd.sign);
                }
            }

            if (cmd.receipts != null){
                for (Map.Entry<String,String> entry : cmd.receipts.entrySet()){
                    if (completeMessage(entry.getKey(), entry.getValue()) == 1){
                        completeOff(null, entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
            log.info("[complete] error json : " + runnerData.json);
        }
        return null;
    }

    public static JSONObject syncMessage(RunnerData runnerData) {
        SessionData sessionData = runnerData.sessionData;
        JSONObject json = runnerData.json;
        JSONObject data = runnerData.data;
        try {

            String userID = json.getString("from");
            //long timeStamp = data.getLong("timestamp");
            UserData userData = getUserData(userID);
            if(userData == null){
                //sendReplyCode(sessionData, E_userError, null);
                return null;
            }
            long timeStamp = userData.logoutTime;
            int count = data.containsKey("count") ? data.getInteger("count") : 20;
            if (count > 100) {
                //sendReplyCode(sessionData, E_dataOverrun, null);
                count = 100;
            }

            //List<MessageData> messageInfos = db.syncMessages(userID, timeStamp, count);
            List<MessageData> messageInfos = db.message.listFromRedis(userID, count);
            JSONArray array = new JSONArray();
            for (MessageData messageData : messageInfos){
                toWebMessage(sessionData, messageData);
                array.add(messageData.data);
            }
            data.clear();
            data.put("msgList", array);
            //sendReplyCode(sessionData, null, data);

            log.info("userID: " + userID + ", timestamp: " + timeStamp + ", count: " + count + ", size: " + array.size());
        } catch (Exception e) {
            log.error("", e);
        }
        return data;
    }

    public static JSONObject wrapMessageX(String command, CryptData cryptData, String to, JSONObject data, JSONObject original){
        if(imShare)
            return wrapMessage(command, service.osnID, to, data, service.osnKey, original);
        return wrapMessage(command, cryptData.osnID, to, data, cryptData.osnKey, original);
    }

    // 回复给用户的消息是不存的
    public static Void sendReplyCode(RunnerData runnerData, ErrorData error, JSONObject json) {
        if (runnerData == null){
            return null;
        }

        String from = runnerData.json.getString("from");
        if (!isInThisNode(from)) {

            JSONObject data = new JSONObject();
            data.put("id", runnerData.json.getString("id"));
            data.put("errCode", error == null ? "0:success" : error.toString());
            //data.put("crypto", "none");
            if(json != null) {
                data.put("content", json.toString());
            }
            data.put("command","Replay");
            String to = runnerData.json.getString("to");
            data.put("from", to);
            data.put("to", from);
            sendOsxMessageNoSave(data);
            return null;
        }


        SessionData sessionData = runnerData.sessionData;
        if (sessionData == null)
            return null;
        if (error != null)
            log.info(error.toString());
        if (sessionData.remote)
            return null;
        JSONObject data = new JSONObject();



        /*{
            String id = runnerData.json.getString("id");
            log.info("reply id : " + id);
        }*/



        data.put("id", runnerData.json.getString("id"));
        data.put("errCode", error == null ? "0:success" : error.toString());
        //data.put("crypto", "none");
        if(json != null) {
            data.put("content", json.toString());

        }

        sendClientMessageNoSave(sessionData, data);
        return null;
    }


    public static Void sendReplyCode(RunnerData runnerData, ErrorData error, JSONObject json, String command) {
        if (runnerData == null){
            return null;
        }

        if (needReply(command)){
            return sendReplyCode(runnerData, error, json);
        }

        return null;
    }

    public static Void forwardMessage(RunnerData runnerData){
        SessionData sessionData = runnerData.sessionData;
        JSONObject msg = runnerData.json;
        if (msg == null){
            return null;
        }

        //UserData userData = getUserData(to);
        String from = msg.getString("from");
        if (from == null){
            return null;
        }
        String to = msg.getString("to");
        if (to == null){
            return null;
        }

        UserData udFrom = getUserData(from);
        UserData udTo = getUserData(to);

        if (udFrom == null && udTo == null){
            return null;
        }


        if (udTo == null){
            // 外发数据
            // to user不在本节点
            if (isInThisNode(from)){
                //如果是本节点用户，就通知他消息已经收到
                sendReplyCode(runnerData, null, null);
            }
            sendOsxMessage(msg);
            return null;
        }

        // 是本节点的user
        if (isInThisNode(from)){
            //如果是本节点用户，就通知他消息已经收到
            sendReplyCode(runnerData, null, null);
        }
        //
        sendClientMessage(udTo, msg);
        return null;
    }

    // 获取离线数据
    public static Void getOff(RunnerData runnerData){
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = runnerData.data;
            String userID = json.getString("from");

            //long timeStamp = data.getLong("timestamp");

            UserData userData = getUserData(userID);
            if(userData == null){
                sendReplyCode(runnerData, E_userError, null);
                return null;
            }
            long timeStamp = userData.logoutTime;
            int count = data.containsKey("count") ? data.getInteger("count") : 20;
            if (count > 100) {
                sendReplyCode(runnerData, E_dataOverrun, null);
                return null;
            }

            List<MessageData> messageInfos = db.message.listUnread(userID, count);
            //List<MessageData> messageInfos = db.syncUnreadMessages(userID, count);
            JSONArray array = new JSONArray();
            for (MessageData messageData : messageInfos){
                toWebMessage(sessionData, messageData);
                array.add(messageData.data);
            }
            data.clear();
            data.put("msgList", array);
            sendReplyCode(runnerData, null, data);

            //log.info("userID: " + userID + ", timestamp: " + timeStamp + ", count: " + count + ", size: " + array.size());
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    //
    public static void completeOff(RunnerData runnerData){
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject msg = runnerData.json;
            String command = msg.getString("command");
            String osnID = msg.getString("to");        // message 中的 from
            String userID = msg.getString("from");     // message 中的 to

            //log.info("[completeMessage] begin");

            JSONObject data = takeMessage(msg);
            if (data == null){
                log.info("[completeOff] error : takeMessage");
                return;
            }


            // data的格式分两种，一种是有hash的
            String hash = data.getString("hash");
            String sign = data.getString("sign");       //message hash
            JSONObject receiptList = data.getJSONObject("receiptList");

            if (hash != null && sign != null){
                completeOff(null, hash, sign);
            }

            if (receiptList != null){

                Map<String, String> receipts = JSONObject.toJavaObject(receiptList, Map.class);
                if (receipts != null){

                    //log.info("[completeMessage] receipts : " + receipts.size());
                    for (Map.Entry<String,String> entry : receipts.entrySet()){

                        completeOff(null, entry.getKey(), entry.getValue());

                    }
                }
            }


        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static void sendComplete(String hash, CryptData cryptData, String to){
        CmdComplete cmd = new CmdComplete();
        cmd.hash = hash;
        cmd.sign = ECUtils.osnSign(cryptData.osnKey, hash.getBytes());

        JSONObject complete = wrapMessageX("Complete", cryptData, to, cmd.toJson(), null);
        sendMessageNoSave(to, complete);
    }





    private static void saveMessage(JSONObject msg){
        CommandData cmd = getCommand(msg.getString("command"));
        if (cmd == null){
            return;
        }
        if (cmd.needSave){
            MessageSaveServer.push(new MessageSaveData(msg, MQCode));
            //db.message.insertInRedis(toMessageData(msg, 0));
        }
        else if (cmd.needSaveOut){
            MessageSaveServer.push(new MessageSaveData(msg, MQOSX));
            //db.message.insertInRedis(toMessageData(msg, 0), MQOSX);
        }
    }

    private static int completeMessage(String hash, String sign){
        try {

            MessageData messageData = db.message.readFromRedis(hash);
            if (messageData == null){
                // 正常情况，是有redis里面没有数据的情况
                //log.info("[completeMessage] error: messageData == null");
                return 1;
            }
            String from = messageData.fromID;
            String to = messageData.toID;
            if (from == null || to == null){
                log.info("[completeMessage] error: from == null || to == null");
                return 0;
            }

            // 验证使用的是 message 中的 to 来验证
            if (!ECUtils.osnVerify(to, hash.getBytes(), sign)) {
                log.info("[completeMessage] error: verify failed hash: " + hash);
                return 0;
            }

            //
            String msg = db.message.completeFromRedis(hash, to);
            if (msg == null) {
                return 0;
            }

            // 检查是否需要外发
            if (!isInThisNode(from)) {
                // from不是本节点成员，更新回执数据
                //log.info("[completeMessage] sendOsxMessageNoSave ");

                // 制作msg 发送回执JSONObject

                JSONObject data = new JSONObject();
                data.put("hash", hash);
                data.put("sign", sign);
                JSONObject json = wrapMessage("Complete", service.osnID, from, data, service.osnKey, null);

                sendOsxMessageNoSave(json);
            }
        }catch (Exception e){
            log.error("", e);
        }

        return 2;
    }

    public static void completeOff(Jedis jedis, String hash, String sign){
        try {

            long time1 = System.currentTimeMillis();
            int osx = 0;
            MessageData messageData = null;
            messageData = db.message.read(hash);
            if (messageData == null){
                messageData = db.message.readOsx(hash);
                osx = 1;
            }
            long time2 = System.currentTimeMillis() - time1;

            //log.info("[select time] : " + time2);

            //MessageData messageData = db.message.readFromRedis(hash);
            if (messageData == null){
                // 正常情况，有可能没有数据
                //log.info("[completeMessage] error: messageData == null");
                return;
            }
            String from = messageData.fromID;
            String to = messageData.toID;
            if (from == null || to == null){
                log.info("[completeMessage] error: from == null || to == null");
                return;
            }

            // 验证使用的是 message 中的 to 来验证
            if (!ECUtils.osnVerify(to, hash.getBytes(), sign)) {
                log.info("[completeMessage] error: verify failed hash: " + hash);
                return;
            }


            long time5 = System.currentTimeMillis();
            // 更新失败好像也没关系
            if (osx == 0){
                db.message.delete(jedis, to, hash);
                //db.message.delete(messageData.id);
                //db.message.updateRead(hash);
            } else {
                //db.receipt.delete(messageData.id);
                db.receipt.delete(hash);
                //db.message.updateOsxRead(hash);
            }
            long time6 = System.currentTimeMillis() - time5;

            //log.info("id : " +messageData.id+ " [delete time] : " + time6);


            // 检查是否需要外发
            if (!isInThisNode(from)) {
                // from不是本节点成员，更新回执数据
                //log.info("[completeMessage] sendOsxMessageNoSave ");

                // 制作msg 发送回执JSONObject

                JSONObject data = new JSONObject();
                data.put("hash", hash);
                data.put("sign", sign);
                JSONObject json = wrapMessage("Complete", service.osnID, from, data, service.osnKey, null);

                sendOsxMessageNoSave(json);
            }
        }catch (Exception e){
            log.error("", e);
        }


    }

    private static void toWebMessage(SessionData sessionData, MessageData messageData) {
        try {
            if (sessionData.webSock && messageData.toID.equalsIgnoreCase(sessionData.fromUser.osnID)) {
                JSONObject data = JSON.parseObject(messageData.data);
                String crypto = data.getString("crypto");
                if (crypto == null)
                    return;
                else if (crypto.equalsIgnoreCase("ecc-aes"))
                    toAesMessage(data, sessionData.user.osnKey);
                else if (crypto.equalsIgnoreCase("aes")) {
                    UserData userData = getUserData(messageData.fromID);
                    if (userData == null) {
                        log.error("unknown src user: " + messageData.fromID);
                        return;
                    }
                    toLocalMessage(data, userData.osnKey, sessionData.user.osnKey);
                }
                messageData.data = data.toString();
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private static boolean needReply(String command){
        if (noReplyList.contains(command)){
            return false;
        }

        return true;
    }

    static long reSendOsx(List<MessageData> messages){

        long maxId = 0;
        for (MessageData msg : messages){
            if (msg.id > maxId){
                maxId = msg.id;
            }
            if (msg.data != null){
                JSONObject data = JSONObject.parseObject(msg.data);
                if (data != null){
                    sendOsxMessageNoSave(data);
                } else {
                    log.info("[reSendOsx] error : data != null");
                }

            } else {
                log.info("[reSendOsx] error : msg.data != null");
            }
        }
        return maxId;
    }
    // 重发机制
    public static void ReSendOsx(){
        int count = 20;
        int offset = 0;
        // 分页获取数据，并进行转发
        // 获取数据列表
        while (true){

            long current = System.currentTimeMillis();
            // 转发6小时以内的数据
            long time = current - ms_minute * 60 * 6;

            List<MessageData> messages = db.receipt.listUnreadPage(offset, count, time);
            if (messages.size() > 0){
                // 转发
                //log.info("[ReSendOsx] offset : " + offset + " count : " + messages.size());
                offset = (int)reSendOsx(messages);
            }
            if (messages.size() < count) {
                break;
            }
        }
    }


    public static JSONObject getMessage(MsgData msg) {
        try {

            String userID = msg.from;
            //long timeStamp = data.getLong("timestamp");
            UserData userData = getUserData(userID);
            if(userData == null){
                return null;
            }

            int count = 20;

            List<MessageData> messageInfos = db.message.listFromRedis(userID, count);

            log.info("[getMessage] count : "+messageInfos.size());

            if (messageInfos.size() == 0){
                return null;
            }

            JSONArray array = new JSONArray();
            for (MessageData messageData : messageInfos){

                array.add(messageData.data);
            }

            JSONObject result = new JSONObject();
            result.put("msgList", array);
            return result;

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static JSONObject history(MsgData msg) {
        try {

            String userID = msg.from;
            //long timeStamp = data.getLong("timestamp");
            UserData userData = getUserData(userID);
            if(userData == null){
                return null;
            }

            int count = 20;

            List<MessageData> messageInfos = db.message.listUnread(userID, count);
            //List<MessageData> messageInfos = db.message.listFromRedis(userID, count);

            log.info("[history] count : "+messageInfos.size());

            if (messageInfos.size() == 0){
                return null;
            }

            JSONArray array = new JSONArray();
            for (MessageData messageData : messageInfos){
                array.add(messageData.data);
            }

            JSONObject result = new JSONObject();
            result.put("msgList", array);
            return result;

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
