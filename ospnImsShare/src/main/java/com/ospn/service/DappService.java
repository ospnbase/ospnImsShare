package com.ospn.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.command.*;
import com.ospn.command2.*;
import com.ospn.data.*;
import com.ospn.server.MessageSyncServer;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;


import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import static com.ospn.OsnIMServer.*;
import static com.ospn.api.GroupV2.handleAddAttribute;
import static com.ospn.api.GroupV2.handleCreateGroup;
import static com.ospn.common.OsnUtils.*;
import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.*;
import static com.ospn.data.FriendData.*;
import static com.ospn.service.MessageService.*;
import static com.ospn.utils.CryptUtils.makeMessage;
import static com.ospn.utils.CryptUtils.wrapMessage;

/**
 * DappService 只处理 user 和service之间通信的命令，不外发
 * to为OSNS，并且为本机ID
 * */
@Slf4j
public class DappService {

    public static Void handleMessage(RunnerData runnerData){

        MsgData msg = new MsgData(runnerData.json);


        //log.info("[DappService] id : " + runnerData.json.getString("id"));

        if (msg.to == null){
            log.info("[DappService::handleMessage] valid : " + runnerData.json);
            return null;
        }

        if (!isService(msg.to)){
            log.info("[DappService::handleMessage] valid : " + runnerData.json);
            return null;
        }



        if (!msg.to.equalsIgnoreCase(service.osnID)){

            if (!isInThisNode(msg.from)){
                log.info("[DappService::handleMessage] error : no from , no to");
                return null;
            }

            // 外发
            //log.info("[DappService::handleMessage] not my service");
            //log.info("[DappService::handleMessage] to :" + msg.to);
            //log.info("[DappService::handleMessage] service id :" + service.osnID);

            sendReplyCode(runnerData, null, null, msg.command);
            sendOsxMessageNoSave(runnerData.json);
            return null;
        }

        //log.info("[DappService] command : " + msg.command);

        switch(msg.command){

            /**
             * 对自己的 信息 的操作
             * */
            case "GetUserInfo":
                return GetUserInfo(runnerData);
            case "SetUserInfo":
                return SetUserInfo(runnerData);
            case "UpRole":
                return UpRole(runnerData);
            case "UpDescribes":
                return UpDescribes(runnerData);
            case "RemoveDescribes":
                return RemoveDescribes(runnerData);




            /**
             *  对好友的操作
             * */
            case "GetFriendList":
                return UserService.GetFriendList(runnerData);
            case "GetFriendInfo":
                return UserService.GetFriendInfo(runnerData);
            case "SetFriendInfo":
                return SetFriendInfo(runnerData);
            case "DelFriend":
                return DelFriend(runnerData);

            /**
             *  对群的操作
             * */
            case "GetGroupList":
                return GroupService.GetGroupList(runnerData);
            //case "GenGrp":
            //    return GenGrp(runnerData);
            case "CreateGroup":
                return GroupService.CreateGroup(runnerData);
            case "SaveGroup":
                /**保存群，group service里也有一个**/
                return SetMemberInfo(runnerData);


            /**
             *  对临时账号的操作
             * */
            case "SetTemp":
                return TempAccountService.SetTemp(runnerData);


            /**
             *  查找操作
             * */
            case "FindObj":
                return SearchService.FindObj(runnerData);


            case "GetServiceInfo":
                return GetServiceInfo(runnerData);

            case "GetLoginInfo":
                return GetLoginInfo(runnerData);

            case "LoginByOsnID":
                return LoginByOsnID(runnerData);

            case "BCRegister":
                return BCRegister(runnerData);

            // 扔队列里处理
            case "MessageSync":
                return MessageSync(runnerData);

            case "EncData":
                return EncData(runnerData);


            /**
             *  管理操作
             * */
            default:
                return ManagerService.handleMessage(runnerData);

        }

    }



    private static Void EncData(RunnerData runnerData) {

        try {
            // json转message
            log.info("1");
            MsgData msg = new MsgData(runnerData.json);
            //log.info("2");
            JSONObject msgContent = msg.getContent(service.osnKey);
            //log.info("msgContent : " + msgContent);
            String command = msgContent.getString("command");
            log.info("command : " + command);
            if (command.equalsIgnoreCase("CreateGroup")) {
                // TODO
                handleCreateGroup(runnerData, msgContent, runnerData.json, msg);
            }

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }

    public static Void UpRole(RunnerData runnerData) {

        try {

            //log.info("UpRole begin");


            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null) {
                log.info("no user");
                return null;
            }

            JSONObject contentJson = msg.getContent(service.osnKey, udFrom.aesKey);
            String role = contentJson.getString("role");

            //
            JSONObject roleJson = JSONObject.parseObject(role);

            JSONObject udRoleJson;
            if (udFrom.role != null) {
                udRoleJson = JSONObject.parseObject(udFrom.role);
            } else {
                udRoleJson = new JSONObject();
            }



            Iterator<String> it = roleJson.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                String value = roleJson.getString(key);
                udRoleJson.remove(key);
                udRoleJson.put(key, value);
            }




            udFrom.role = udRoleJson.toString();

            //log.info("role : " + udFrom.role);


            List<String> keys = new ArrayList<>();
            keys.add("role");
            db.user.update(udFrom, keys);

            sendReplyCode(runnerData, null, null);







            /*CmdSetUserInfo cmd = new CmdSetUserInfo();

            UserData udUp = new UserData(cmd);
            udUp.osnID = msg.from;

            log.info("[SetUserInfo] data : " + udUp.getRemoteInfo());
            OsnIMServer.db.user.update(udUp);
            udFrom.update(cmd);

            sendReplyCode(runnerData, null, null);*/

            /*if (cmd.aesKey == null) {
                // 包含aeskey的不通知
                CmdUserUpdate reCmd = new CmdUserUpdate(cmd);
                JSONObject result = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);
                sendClientMessageNoSave(runnerData.sessionData, result);

                List<FriendData> friends = db.friend.list(msg.from);
                for (FriendData f : friends) {
                    JSONObject json = reCmd.makeMessage(service.osnID, f.friendID, service.osnKey, null);
                    sendMessage(f.friendID, json);
                }
            }*/

        } catch (Exception e) {
            log.error("", e);
            sendReplyCode(runnerData, E_formatError, null);
        }
        return null;
    }


    public static Void UpDescribes(RunnerData runnerData) {

        try {
            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null) {
                return null;
            }

            JSONObject contentJson = msg.getContent(service.osnKey, udFrom.aesKey);
            String describes = contentJson.getString("data");

            //
            JSONObject describesJson = JSONObject.parseObject(describes);

            JSONObject udDesJson;
            if (udFrom.describes != null) {
                udDesJson = JSONObject.parseObject(udFrom.describes);
            } else {
                udDesJson = new JSONObject();
            }



            Iterator<String> it = describesJson.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                String value = describesJson.getString(key);
                udDesJson.remove(key);
                udDesJson.put(key, value);
            }



            udFrom.describes = udDesJson.toString();
            List<String> keys = new ArrayList<>();
            keys.add("describes");
            db.user.update(udFrom, keys);

            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
            sendReplyCode(runnerData, E_formatError, null);
        }
        return null;
    }

    public static Void RemoveDescribes(RunnerData runnerData) {

        try {
            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null) {
                return null;
            }

            JSONObject contentJson = msg.getContent(service.osnKey, udFrom.aesKey);
            String describes = contentJson.getString("data");

            //
            JSONObject describesJson = JSONObject.parseObject(describes);

            JSONObject udDesJson;
            if (udFrom.describes != null) {
                udDesJson = JSONObject.parseObject(udFrom.describes);
            } else {
                udDesJson = new JSONObject();
            }

            String removeStr = describesJson.getString("command");
            if (removeStr == null) {
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }
            udDesJson.remove(removeStr);

            udFrom.describes = udDesJson.toString();
            List<String> keys = new ArrayList<>();
            keys.add("describes");
            db.user.update(udFrom, keys);

            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
            sendReplyCode(runnerData, E_formatError, null);
        }
        return null;
    }




    public static Void GetUserInfo(RunnerData runnerData) {

        try {

            MsgData msg = new MsgData(runnerData.json);
            SessionData sessionData = runnerData.sessionData;
            String from = msg.from;

            UserData udFrom = getUserData(from);
            if (udFrom == null){
                //from 不在本节点
                return null;
            }

            CmdUserInfo reCmd = new CmdUserInfo(udFrom.toJson());
            JSONObject data = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);

            sendClientMessageNoSave(sessionData, data);
            //sendReplyCode(runnerData, null ,null);

        } catch (Exception e) {
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }

    private static Void SetUserInfo(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                return null;
            }
            CmdSetUserInfo cmd = new CmdSetUserInfo(msg.getContent(service.osnKey, udFrom.aesKey));

            UserData udUp = new UserData(cmd);
            udUp.osnID = msg.from;

            log.info("[SetUserInfo] data : " + udUp.getRemoteInfo());
            OsnIMServer.db.user.update(udUp);
            udFrom.update(cmd);

            sendReplyCode(runnerData, null, null);

            /*if (cmd.aesKey == null) {
                // 包含aeskey的不通知
                CmdUserUpdate reCmd = new CmdUserUpdate(cmd);
                JSONObject result = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);
                sendClientMessageNoSave(runnerData.sessionData, result);

                List<FriendData> friends = db.friend.list(msg.from);
                for (FriendData f : friends) {
                    JSONObject json = reCmd.makeMessage(service.osnID, f.friendID, service.osnKey, null);
                    sendMessage(f.friendID, json);
                }
            }*/

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetFriendZone(RunnerData runnerData) {

        try {
            MsgData msg = new MsgData(runnerData.json);

            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                //from 不在本节点
                return null;
            }

            CmdGetFriendZone cmd = new CmdGetFriendZone(msg.getContent(service.osnKey, udFrom.aesKey));

            //udFrom.friends

            List<FriendData> friendDataList = OsnIMServer.db.friend.list(msg.from);

            JSONObject json = runnerData.json;

            //log.info("[GetFriendList] friend count : " + friendDataList.size());
            JSONArray friendList = new JSONArray();
            for (FriendData f : friendDataList) {
                if (f.state != FriendStatus_Wait)
                    friendList.add(f.friendID);
            }

            JSONObject data = new JSONObject();
            data.put("friendList", friendList);
            JSONObject result = makeMessage("FriendList", service.osnID, msg.from, data, service.osnKey, json);
            sendClientMessageNoSave(udFrom, result);


        } catch (Exception e) {
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }

    public static Void GetFriendInfo(RunnerData runnerData) {

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
            log.info("[GetFriendInfo] friendID : " + cmd.friendID);

            FriendData friendData = OsnIMServer.db.friend.read(msg.from, cmd.friendID);
            if (friendData == null) {
                log.info("[GetFriendInfo] no find friend: " + cmd.friendID);
                sendReplyCode(runnerData, E_friendNoFind, null);
                return null;
            }

            CmdFriendInfo reCmd = new CmdFriendInfo(friendData);
            JSONObject result = reCmd.makeMessage(
                    service.osnID,
                    msg.from,
                    service.osnKey,
                    runnerData.json
                    );

            sendClientMessageNoSave(runnerData.sessionData, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void SetFriendInfo(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                return null;
            }

            CmdSetFriendInfo cmd = new CmdSetFriendInfo(msg.getContent(service.osnKey, udFrom.aesKey));

            FriendData friendData = new FriendData();
            friendData.userID = msg.from;
            friendData.friendID = cmd.friendID;
            friendData.remarks = cmd.remarks;
            friendData.state = cmd.state;

            if (!db.friend.update(friendData)) {
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }

            sendReplyCode(runnerData, null, null);

            if (friendData.state == FriendStatus_Blacked)
                udFrom.addBlack(friendData.friendID);
            else if (friendData.state == FriendStatus_Normal)
                udFrom.delBlack(friendData.friendID);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetGroupList(RunnerData runnerData) {

        try {
            MsgData msg = new MsgData(runnerData.json);
            SessionData sessionData = runnerData.sessionData;

            List<String> groupList = OsnIMServer.db.group.list(msg.from, false);


            log.info("[GetGroupList] group count: " + groupList.size());


            CmdGroupList reCmd = new CmdGroupList(groupList);
            JSONObject result = reCmd.makeMessage(
                    service.osnID,
                    msg.from,
                    service.osnKey,
                    runnerData.json);

            sendClientMessageNoSave(sessionData, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void DelFriend(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                return null;
            }
            CmdDelFriend cmd = new CmdDelFriend(msg.getContent(service.osnKey, udFrom.aesKey));

            OsnIMServer.db.friend.delete(msg.from, cmd.friendID);
            udFrom.delFriend(cmd.friendID);

            sendReplyCode(runnerData, null, null);

            // 删除以后不需要通知对方
            CmdFriendUpdate reCmd = new CmdFriendUpdate();
            reCmd.state = FriendStatus_Delete;
            reCmd.friendID = cmd.friendID;
            reCmd.userID = msg.from;


            JSONObject result = reCmd.makeMessage(
                    service.osnID,
                    msg.from,
                    service.osnKey,
                    runnerData.json);
            sendClientMessageNoSave(runnerData.sessionData, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GenGrp(RunnerData runnerData) {
        return GroupService.GenGrp(runnerData);
    }

    /**
     * 该指令是保存群
     * */
    public static Void SetMemberInfo(RunnerData runnerData) {
        try {
            log.info("[SetMemberInfo] begin.");
            MsgData msg = new MsgData(runnerData.json);
            CmdSetMemberInfo cmd = new CmdSetMemberInfo(msg.getContent(service.osnKey));
            if (cmd.groupID == null){
                return null;
            }

            log.info("[SetMemberInfo] groupID : " + cmd.groupID);
            log.info("[SetMemberInfo] status : " + cmd.status);

            // 判断群是否在本地
            if (OsnIMServer.isInThisNode(cmd.groupID)) {
                // 在本地

                // 判断是否是群成员
                GroupData groupData = getGroupData(cmd.groupID);
                if (groupData.hasMember(msg.from)) {

                    MemberData memberData = groupData.getMember(msg.from);
                    memberData.status = cmd.status;

                    if (!OsnIMServer.db.groupMember.setMemberStatus(memberData)) {

                        log.info("error : db set member status.");
                        sendReplyCode(runnerData, E_dataBase, null);
                        return null;
                    }

                    log.info("success.");
                    sendReplyCode(runnerData, null, null);
                    return null;


                } else {
                    log.info("not member.");
                    sendReplyCode(runnerData, E_notMember, null);
                    return null;
                }



            } else {
                // 不在本地
                MemberData memberData = OsnIMServer.db.groupMember.read(cmd.groupID, msg.from);
                if (memberData == null) {
                    memberData = new MemberData(msg.from, cmd.groupID, 1);
                    memberData.status = cmd.status;
                    memberData.inviter = "";
                    if (!OsnIMServer.db.groupMember.insert(memberData)) {
                        log.info("error : db set member status.");
                        sendReplyCode(runnerData, E_dataBase, null);
                        return null;
                    }
                    log.info("success.");
                    sendReplyCode(runnerData, null, null);
                    return null;
                } else {
                    memberData.status = cmd.status;
                    if (!OsnIMServer.db.groupMember.setMemberStatus(memberData)) {
                        log.info("error : db set member status.");
                        sendReplyCode(runnerData, E_dataBase, null);
                        return null;
                    }
                    sendReplyCode(runnerData, null, null);
                    return null;
                }

            }





            /*MemberData memberData = OsnIMServer.db.groupMember.read(cmd.groupID, msg.from);
            if (memberData == null){
                GroupData groupData = getGroupData(cmd.groupID);
                if (groupData == null){
                    // 群不在本节点，需要记录一下
                    memberData = new MemberData(msg.from, cmd.groupID, 1);
                    memberData.status = cmd.status;
                    memberData.inviter = "";
                    OsnIMServer.db.groupMember.insert(memberData);
                    sendReplyCode(runnerData, null, null);
                    return null;
                }
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }

            OsnIMServer.db.groupMember.setMemberStatus(memberData);
            sendReplyCode(runnerData, null, null);
            return null;*/








        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetServiceInfo(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);

            String userID = msg.from;   //json.getString("from");
            String serviceID = msg.to;  //json.getString("to");

            if (serviceID == null)
                return null;

            // log.info("serviceID: " + serviceID + ", from: " + userID + ", remote: " + sessionData.remote);

            JSONObject result;
            if (serviceID.equalsIgnoreCase(service.osnID)) {
                JSONObject info = new JSONObject();
                info.put("type", "IMS");
                info.put("urlSpace", urlSpace);
                result = wrapMessage("ServiceInfo", serviceID, userID, info, service.osnKey, runnerData.json);
                sendMessage(msg.from, result);
            }


        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetLoginInfo(RunnerData runnerData){
        try {
            log.info("GetLoginInfo : " + runnerData.json);
            MsgData msg = new MsgData(runnerData.json);
            UserData userData = getUserData(msg.from);
            if(userData == null){
                log.info("[GetLoginInfo] error. userData == null");
                // delSessionData(runnerData.sessionData);
                return null;
            }

            SessionData sessionData = runnerData.sessionData;
            if (sessionData.challenge == 0) {
                sessionData.challenge = System.currentTimeMillis();
            }


            JSONObject replyData = new JSONObject();
            replyData.put("challenge", sessionData.challenge);
            replyData.put("serviceID", service.osnID);

            JSONObject result =makeMessage("LoginInfo",
                    service.osnID,
                    msg.from,
                    replyData,
                    service.osnKey,
                    runnerData.json);

            //CmdLoginInfo reCmd = new CmdLoginInfo(sessionData.challenge);
            //JSONObject result = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);




            return sendReplyCode(runnerData, null, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void LoginByOsnID(RunnerData runnerData){
        try {

            log.info("LoginByOsnID begin.");
            MsgData msg = new MsgData(runnerData.json);

            SessionData sessionData = runnerData.sessionData;

            // 检查用户是否存在
            UserData userData = getUserData(msg.from);
            if(userData == null){
                log.info("[LoginByOsnID] error. userData == null");
                // delSessionData(runnerData.sessionData);
                return null;
            }

            // 验证登录权限，一条链接只能登录一次
            CmdLoginByOsnID cmd = new CmdLoginByOsnID(msg.getContent(service.osnKey, userData.aesKey));
            if (!cmd.checkRight(sessionData.challenge, msg.from)){
                log.info("[LoginByOsnID] error. check right failed.");
                // delSessionData(runnerData.sessionData);
                return null;
            }


            // 已经是登录状态了
            if (sessionData.state == 1) {
                // 返回登录信息

                JSONObject result = genLoginData(runnerData);
                sendReplyCode(runnerData, null, result);

                return null;
            }



            //log.info("[LoginByOsnID] challenge : " + sessionData.challenge);



            if (userData.session != null && userData.session != sessionData) {
                log.info("device : " + userData.session.deviceID);
                log.info("login device : " + sessionData.deviceID);

                if (userData.session.deviceID != null
                        && sessionData.deviceID != null
                        && !sessionData.deviceID.equalsIgnoreCase(userData.session.deviceID)) {

                    JSONObject data = makeMessage("KickOff", service.osnID, msg.from, "{}", userData.osnKey, null);
                    sendClientMessageNoSave(userData.session, data);

                    log.info("[LoginByOsnID] KickOff userID: " + msg.from + ", deviceID: " + userData.session.deviceID);
                }
                delSessionData(userData.session);
            }

            // 加入session map
            sessionData.state = 1;
            if (!userMap.containsKey(userData.osnID)) {
                userMap.put(userData.osnID, userData);
            }


            synchronized (userData) {
                userData.session = sessionData;
            }
            sessionData.user = userData;
            sessionData.timeHeart = System.currentTimeMillis();
            userData.loginTime = sessionData.timeHeart;
            //db.user.update(userData, Collections.singletonList("loginTime"));

            userData.token = cmd.token;
            Jedis jedis = db.message.getJedis();
            db.conversation.setLoginToken(jedis, userData.osnID, cmd.token);
            db.message.closeJedis(jedis);

            log.info("[LoginByOsnID] user: " + userData.osnID + ", name: " + userData.name);



            JSONObject result = genLoginData(runnerData);
            sendReplyCode(runnerData, null, result);

            pushOsnID(userData);

            MessageSyncServer.push(userData.osnID);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static JSONObject genLoginData(RunnerData runnerData) {
        UserData userData = runnerData.sessionData.user;
        SessionData sessionData = runnerData.sessionData;
        JSONObject data = new JSONObject();
        data.put("aesKey", userData.aesKey);
        data.put("msgKey", userData.msgKey);
        data.put("serviceID", service.osnID);
        data.put("challenge", sessionData.challenge);

        JSONObject result = makeMessage("LoginInfo",
                service.osnID,
                userData.osnID,
                data,
                service.osnKey,
                runnerData.json
                );
        return result;
    }

    public static Void BCRegister(RunnerData runnerData){

        try {

            MsgData msg = new MsgData(runnerData.json);

            CmdBCRegitser cmd = new CmdBCRegitser(msg.getContent(service.osnKey, null));

            UserData userData = getUserData(msg.from);
            if (userData != null) {
                log.info("[LoginRegister] user existed.");
                sendReplyCode(runnerData, E_userExist, null);
                return null;
            }

            if (msg.from.equalsIgnoreCase(cmd.osnID)) {
                log.info("[LoginRegister] format error.");
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }

            if (!register(cmd.osnID, cmd.displayName, cmd.owner)) {
                log.info("[LoginRegister] register error.");
                sendReplyCode(runnerData, E_registFailed, null);
                return null;
            }

            sendReplyCode(runnerData, null, null);
            pushOsnID(msg.from);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void MessageSync(RunnerData runnerData){
        try {
            MsgData msg = new MsgData(runnerData.json);

            UserData userData = getUserData(msg.from);
            if(userData == null){
                log.info("[MessageSync] error. userData == null");
                //return sendReplyCode(runnerData, E_noRight, null);
                delSessionData(runnerData.sessionData);
                return null;
            }

            sendReplyCode(runnerData, null, null);

            MessageSyncServer.push(msg.from);

            //MessageSyncThreadServer thread = new MessageSyncThreadServer(msg.from);
            //thread.start();

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }




    private static boolean register(String osnID, String displayName, String owner) {
        /**
         * osnID :
         * displayName :
         * owner :
         * **/

        try {


            UserData userData = new UserData();

            userData.osnID = osnID;
            // userData.osnKey = newOsnID[1];
            // userData.name = userName;
            userData.owner2 = owner;
            if (userData.owner2 == null){
                userData.owner2 = globalOwnerID;
            }
            //userData.password = Base64.getEncoder().encodeToString(sha256(password.getBytes()));
            userData.aesKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
            userData.msgKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
            userData.displayName = displayName;
            userData.createTime = System.currentTimeMillis();
            //userData.urlSpace = urlSpace;

            if (!db.user.insert(userData)) {
                log.info("db insert error.");
                return false;
            }

            return true;


        } catch (Exception e) {
            log.error(e.getMessage());
        }



        return false;
    }


}
