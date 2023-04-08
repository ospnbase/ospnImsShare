package com.ospn.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.command.CmdGetFriendInfo;
import com.ospn.command.CmdGroupUpdate;
import com.ospn.command.MsgData;
import com.ospn.data.*;
import com.ospn.server.PushServer;
import com.ospn.utils.data.PushData;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.ospn.OsnIMServer.*;
import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.*;
import static com.ospn.data.FriendData.*;
import static com.ospn.service.MessageService.*;
import static com.ospn.utils.CryptUtils.*;

@Slf4j
public class UserService {

    public static Void handleMessage(RunnerData runnerData){

        MsgData msg = new MsgData(runnerData.json);

        if (msg.to == null){
            log.info("[UserService::handleMessage] valid : " + runnerData.json);
            replayFormatError(runnerData);
            return null;
        }

        if (!isUser(msg.to)){
            log.info("[UserService::handleMessage] valid : " + runnerData.json);
            replayFormatError(runnerData);
            return null;
        }

        if (!isInThisNode(msg.from) && !isInThisNode(msg.to)){
            log.info("[UserService::handleMessage] error : no from , no to");
            return null;
        }


        UserData userData = getUserData(msg.to);
        if (userData == null){
            // 外发
            handleMessageNodeSelf(runnerData);
            sendReplyCode(runnerData, null, null, msg.command);
            sendOsxMessage(runnerData.json);
            return null;
        }

        switch(msg.command){
            case "Message":
                return Message(runnerData);

            case "EncData":
                return EncData(runnerData);

            case "AddFriend":
                return AddFriend(runnerData);

            case "AgreeFriend":
                return AgreeFriend(runnerData);

            case "RejectFriend":
                return RejectFriend(runnerData);

            case "GetUserInfo":
                return GetUserInfo(runnerData);



            //case "GetFriendInfo":
            //    return GetFriendInfo(runnerData);

            //case "SetFriendInfo":
            //    return SetFriendInfo(runnerData);

            case "Invitation":
                return Invitation(runnerData);

            case "GroupUpdate":
                return GroupUpdate(runnerData);

            /**
             * 以下几个为收到的信息，只需要转发，消息需要存
             * */
            case "JoinGroup":
                return forwardToUser(runnerData);

            /**
             * 以下几个为收到的信息，只需要转发，消息不存，该消息需要立即使用
             * */
            case "UserInfo":
            case "GroupInfo":
            case "MemberInfo":
            case "ServiceInfo":
            case "GetGroupSign":
            case "GetOwnerSign":
            case "Replay":
                return forwardToUserNoSave(runnerData);



            default:
                log.info("[UserService::HandleMessage] error command : " + msg.command);
                log.info("[UserService::HandleMessage] from : " + msg.from);
                log.info("[UserService::HandleMessage] to : " + msg.to);
                break;
        }

        return null;

    }

    public static Void handleMessageNodeSelf(RunnerData runnerData){
        MsgData msg = new MsgData(runnerData.json);
        if (msg.command == null){
            return null;
        }
        if (msg.from == null){
            return null;
        }

        switch(msg.command){

            case "AddFriend":
                return AddFriend(msg);

            case "AgreeFriend":
                return AgreeFriend(msg);
        }

        return null;

    }

    public static Void replayFormatError(RunnerData runnerData){
        if (!runnerData.sessionData.remote){
            return sendReplyCode(runnerData, E_formatError, null);
        }
        return null;
    }

    public static Void Message(RunnerData runnerData){

        try{
            JSONObject msg = runnerData.json;
            if (msg == null){
                log.info("[UserService::Message] error, msg == null");
                replayFormatError(runnerData);
                return null;
            }
            String to = msg.getString("to");
            if (to == null){
                log.info("[UserService::Message] error, to == null");
                replayFormatError(runnerData);
                return null;
            }

            // 只管转发，至于对方是否接受不重要
            String from = msg.getString("from");
            if (from == null){
                // 不需要reply
                log.info("[UserService::Message] error, from == null");
                replayFormatError(runnerData);
                return null;
            }

            UserData udFrom = getUserData(from);
            UserData udTo = getUserData(to);

            if (udFrom == null && udTo == null){
                log.info("[UserService::Message] error, udFrom == null && udTo == null");
                replayFormatError(runnerData);
                return null;
            }

            if (udTo == null){
                // 外发数据
                // to user不在本节点
                if (isInThisNode(from)){
                    //如果是本节点用户，就通知他消息已经收到
                    sendReplyCode(runnerData, null, null);
                }

                //log.info("[UserService::forwardMessage] sendOsxMessage : " + msg);
                sendOsxMessage(msg);

                return null;
            }



            log.info("udTo role: " + udTo.role);
            String role = udTo.getRole("AddFriend");
            if (role == null) {
                //默认是0
                role = "0";
            }
            if (!role.equalsIgnoreCase("0")) {
                log.info("role 0");
                // 检查双方是否是好友关系
                if (udFrom != null){
                    if (!udTo.isFriend(from)){
                        // 你还不是to的好友
                        sendReplyCode(runnerData, E_friendNoFind, null);
                        return null;
                    }
                }
            }



            if (udTo.isBlacked(from)){
                sendReplyCode(runnerData, E_friendNoFind, null);
                return null;
            }

            // 是本节点的user
            // 保存数据
            //log.info("[UserService::forwardMessage] msg : " + msg);
            if (udFrom != null){
                //如果是本节点用户，就通知他消息已经收到
                sendReplyCode(runnerData, null, null);
            }

            sendClientMessage(udTo, msg);

            /*if (udTo != null) {
                if (udTo.owner2 != null) {
                    if (!udTo.owner2.equalsIgnoreCase("")) {

                        PushData pushData = new PushData();
                        pushData.time = System.currentTimeMillis();
                        pushData.owner2 = udTo.owner2;
                        pushData.user = udTo.osnID;
                        pushData.type = "Message";
                        pushData.msgHash = msg.getString("hash");
                        log.info("[Push] PushServer user:"+udTo.osnID);
                        PushServer.push(pushData);
                    }
                    else {
                        log.info("[Push] owner2 is  .");
                    }
                } else {
                    log.info("[Push] owner2 is null.");
                }

            } else {
                log.info("[Push] udTo is null.");
            }*/

            return null;

        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[UserService::Message] error json : " + runnerData.json);
        }

        return null;
    }

    public static Void EncData(RunnerData runnerData){
        // log.info("EncData begin");

        try{
            MsgData msg = new MsgData(runnerData.json);

            UserData udFrom = getUserData(msg.from);
            UserData udTo = getUserData(msg.to);

            if (udFrom == null && udTo == null){
                log.info("[UserService::EncData] error, udFrom == null && udTo == null");
                replayFormatError(runnerData);
                return null;
            }

            if (udTo == null){
                // 外发数据
                sendReplyCode(runnerData, null, null);
                sendOsxMessage(runnerData.json);
                return null;
            }

            if (udFrom != null){
                //如果是本节点用户，就通知他消息已经收到
                sendReplyCode(runnerData, null, null);
            }

            //log.info("send data to To : " + msg.to);

            // 发送给user2
            sendClientMessage(udTo, runnerData.json);

            // 发送给push server，判断是否需要推送
            /*if (udTo != null) {
                if (udTo.owner2 != null) {
                    if (!udTo.owner2.equalsIgnoreCase("")) {

                        PushData pushData = new PushData();
                        pushData.time = System.currentTimeMillis();
                        pushData.owner2 = udTo.owner2;
                        pushData.user = udTo.osnID;
                        pushData.type = "Message";
                        pushData.msgHash = msg.hash;

                        PushServer.push(pushData);
                    }
                    else {
                        //log.info("[Push] owner2 is  .");
                    }
                } else {
                    //log.info("[Push] owner2 is null.");
                }

            } else {
                //log.info("[Push] udTo is null.");
            }*/

            return null;

        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[UserService::EncData] error json : " + runnerData.json);
        }

        return null;
    }


    public static Void AddFriend(RunnerData runnerData) {
        // 机制： 只要添加，就一定可以成功
        // 有远程
        //log.info("AddFriend begin.");

        try{
            if (runnerData.json == null){
                return null;
            }
            MsgData msg = new MsgData(runnerData.json);
            if (msg.from == null){
                return null;
            }
            if (msg.to == null){
                return null;
            }

            UserData udFrom = getUserData(msg.from);
            UserData udTo = getUserData(msg.to);




            // 检查双方是否是好友关系
            //外层处理了
            /*
            if (udFrom == null && udTo == null){
                // 两个都不是本节点的，数据丢弃
                return null;
            }*/

            // 机制，只要添加好友，就一定能添加成功
            if (udFrom != null){
                // 主动添加
                if (!udFrom.isFriend(msg.to)){
                    // 不是好友关系就添加
                    CreateFriend(msg.from, msg.to);
                } else {
                    log.info("already friend from(" + msg.from + ") to(" + msg.to + ")");
                }
                sendReplyCode(runnerData, null, null);
            }

            if (udTo != null){

                log.info("udTo role: " + udTo.role);

                String role = udTo.getRole("AddFriend");

                if (role == null) {
                    //默认是0
                    role = "0";
                }

                if (role.equalsIgnoreCase("0")) {

                    log.info("role 0");
                    // 允许所有人添加好友
                    if (!udTo.isFriend(msg.from)){
                        // 不是好友关系就添加
                        CreateFriend(msg.to, msg.from);
                    }



                    return null;

                } else if (role.equalsIgnoreCase("1")) {
                    // 需要验证

                    log.info("role 1");
                    // 收到消息，就转发给to
                    log.info("from : " + msg.from);
                    log.info("to : " + udTo.osnID);

                    if (!udTo.isFriend(msg.from)){
                        log.info("send to " + udTo.osnID);
                        sendClientMessage(udTo, runnerData.json);
                    } else {
                        log.info("Already friend from(" + msg.from + ") to(" + msg.to + ")");
                    }

                    /*if (udTo.owner2 != null) {
                        if (!udTo.owner2.equalsIgnoreCase("")) {

                            PushData pushData = new PushData();
                            pushData.time = System.currentTimeMillis();
                            pushData.owner2 = udTo.owner2;
                            pushData.user = udTo.osnID;
                            pushData.type = msg.command;
                            pushData.msgHash = msg.hash;
                            PushServer.push(pushData);
                        }
                    }*/


                } else if (role.equalsIgnoreCase("2")) {
                    log.info("role 2");
                    // 不允许添加好友
                    return null;
                }




            }




        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[AddFriend] error json : " + runnerData.json);
        }

        return null;
    }

    public static Void AgreeFriend(RunnerData runnerData) {
        // 没有远程
        try {

            JSONObject json = runnerData.json;
            String userID = json.getString("from");
            String friendID = json.getString("to");

            log.info("[AddFriend] " +userID+ " agree "+friendID);
            // 同意的步骤
            // 1. 添加对方为好友
            sendReplyCode(runnerData, null, null);
            if (isInThisNode(userID)){
                CreateFriend(userID, friendID);
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void RejectFriend(RunnerData runnerData) {
        // 拒绝好友添加， 机制message， 直接转发
        return forwardMessage(runnerData);
    }

    public static Void GetUserInfo(RunnerData runnerData) {
        // 有远程
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String from = json.getString("from");
            String to = json.getString("to");

            // 已经提到外层去了
            UserData udTo = getUserData(to);
            if (udTo == null){
                // 转发

                //log.info("[GetUserInfo] " + from + " => " + to);
                //sendOsxMessageNoSave(json);

                return null;
            }


            // 回信息
            JSONObject data ;
            if (from.equalsIgnoreCase(to)){
                data = udTo.toJson();
            } else {
                //String id = json.getString("id");
                data = udTo.getRemoteInfo();
                //log.info("[GetUserInfo] data : " + data);
            }

            data = wrapMessageX("UserInfo", udTo, from, data, json);

            if (!isInThisNode(from)){
                // 外发
                //log.info("[GetUserInfo] to (" + from + ") sendOsx : " + data);
                sendOsxMessageNoSave(data);
                return null;
            }

            /*if (!from.equalsIgnoreCase(to)){
                //log.info("Send user info : " + data);
                log.info("[GetUserInfo] id : " + json.getString("id"));
            }*/

            sendClientMessageNoSave(sessionData, data);
            //sendReplyCode(runnerData, null ,null);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }




    public static Void GetFriendList(RunnerData runnerData) {
        // 没有远程
        try {

            MsgData msg = new MsgData(runnerData.json);

            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                //from 不在本节点
                return null;
            }


            //log.info("[GetFriendList] begin.  id : " + runnerData.json.getString("id"));


            List<FriendData> friendDataList = OsnIMServer.db.friend.list(msg.from);

            JSONObject json = runnerData.json;


            JSONArray friendList = new JSONArray();
            for (FriendData f : friendDataList) {
                if (f.state != FriendStatus_Wait)
                    friendList.add(f.friendID);
            }

            //log.info("[GetFriendList] friend count : " + friendList.size());

            JSONObject data = new JSONObject();
            data.put("friendList", friendList);
            JSONObject result = makeMessage("FriendList", service.osnID, msg.from, data, service.osnKey, json);

            //log.info("[GetFriendList] id : " + result.getString("id"));

            sendClientMessageNoSave(udFrom, result);



        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetFriendInfo(RunnerData runnerData) {
        // 没有远程
        try {

            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                log.info("[GetFriendInfo] no from. ");
                return null;
            }
            CmdGetFriendInfo cmd = new CmdGetFriendInfo(msg.getContent(service.osnKey, udFrom.aesKey));
            if (cmd.friendID == null){
                log.info("[GetFriendInfo] no friendID. ");
                return null;
            }
            //log.info("[GetFriendInfo] friendID : " + cmd.friendID);

            FriendData friendData = OsnIMServer.db.friend.read(msg.from, cmd.friendID);
            if (friendData == null) {
                log.info("[GetFriendInfo] no find friend: " + cmd.friendID);
                sendReplyCode(runnerData, E_friendNoFind, null);
                return null;
            }

            JSONObject result = makeMessage("FriendInfo", service.osnID, msg.from, friendData.toJson(), service.osnKey, runnerData.json);
            sendClientMessageNoSave(udFrom, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void SetFriendInfo(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject data = runnerData.data;
            log.info(data.toString());

            FriendData friendData = new FriendData();
            friendData.userID = sessionData.user.osnID;
            friendData.friendID = data.getString("friendID");

            List<String> keys = new ArrayList<>();
            if (data.containsKey("remarks")) {
                keys.add("remarks");
                friendData.remarks = data.getString("remarks");
            }
            if (data.containsKey("state")) {
                keys.add("state");
                friendData.state = data.getIntValue("state");
            }
            if (!db.friend.update(friendData, keys)) {
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            sendReplyCode(runnerData, null, null);

            if (friendData.state == FriendStatus_Blacked)
                sessionData.fromUser.addBlack(friendData.friendID);
            else if (friendData.state == FriendStatus_Normal)
                sessionData.fromUser.delBlack(friendData.friendID);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }



    public static Void Invitation(RunnerData runnerData){
        return Message(runnerData);
    }





    public static Void forwardToUser(RunnerData runnerData) {
        // 该消息不怎么重要，失败了就重新获取一次
        try {
            if (runnerData.json == null){
                return null;
            }
            MsgData msg = new MsgData(runnerData.json);

            //判断to是给群的还是给个人的
            if (!isUser(msg.to)){
                return null;
            }

            // 给用户的，直接发给用户，该消息必须在线
            sendMessage(msg.to, runnerData.json);

        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }

    public static Void forwardToUserNoSave(RunnerData runnerData) {
        // 该消息不怎么重要，失败了就重新获取一次
        try {

            if (runnerData.json == null){
                return null;
            }
            String command = runnerData.json.getString("command");

            MsgData msg = new MsgData(runnerData.json);

            //判断to是给群的还是给个人的
            if (!isUser(msg.to)){
                return null;
            }

            // 给用户的，直接发给用户，该消息必须在线
            /*log.info("command : " + command);
            log.info("json : " + runnerData.json);
            log.info("send to : " + msg.to);*/
            sendMessageNoSave(msg.to, runnerData.json);

        } catch (Exception e){
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }


    private static void CreateFriend(String userID, String friendID) {

        FriendData friend = OsnIMServer.db.friend.read(userID, friendID);
        if (friend != null) {
            // 说明已经是好友了
            friend.state = FriendStatus_Normal;
            // 更新好友信息
            if (!db.friend.update(friend, Collections.singletonList("state"))) {
                log.info("[CreateFriend] error : update");
                return;
            }
            // 退出
            return;
        }
        // 创建好友
        friend = new FriendData(userID, friendID, FriendStatus_Normal);
        if (!db.friend.insert(friend)) {
            log.info("[CreateFriend] error : insert");
            return;
        }

        // friend 表中更新数据
        UserData userData = getUserData(userID);
        userData.addFriend(friendID);

        // 给用户发送数据
        JSONObject data = new JSONObject();
        data.put("userID", userID);
        data.put("friendID", friendID);
        data.put("state", FriendStatus_Normal);
        JSONObject json = makeMessage("FriendUpdate", service.osnID, userID, data, service.osnKey, null);
        sendMessageNoSave(userID, json);
    }

    private static Void GroupUpdate(RunnerData runnerData) {

        try {

            forwardToUser(runnerData);

            //log.info("[GroupUpdate] begin.");
            MsgData msg = new MsgData(runnerData.json);
            UserData userData = getUserData(msg.to);

            CmdGroupUpdate cmd = new CmdGroupUpdate(msg.getContent(userData.osnKey));

            if (cmd.notice.state.equalsIgnoreCase("DelMember")){
                // 删除group member 表中的记录
                // user list中是否有 to
                if (cmd.notice.userList.contains(msg.to)) {
                    db.groupMember.delete(msg.to, msg.from);
                    userData.delGroup(msg.from);
                }
            } else if (cmd.notice.state.equalsIgnoreCase("DelGroup")) {
                // 删除group member 表中的记录
                db.groupMember.delete(msg.to, cmd.notice.groupID);
                userData.delGroup(msg.from);
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static Void AddFriend(MsgData msg) {
        // 处理 本节点加好友问题
        // 有远程


        try{
            if (msg.to == null){
                return null;
            }

            UserData udFrom = getUserData(msg.from);
            if (udFrom != null){
                if (!udFrom.isFriend(msg.to)){
                    // 不是好友关系就添加
                    CreateFriend(msg.from, msg.to);
                }
            }

        } catch (Exception e){
            log.info(e.getMessage());
        }

        return null;
    }

    private static Void AgreeFriend(MsgData msg) {
        // 没有远程
        try {
            if (msg.to == null){
                return null;
            }

            CreateFriend(msg.from, msg.to);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }



}
