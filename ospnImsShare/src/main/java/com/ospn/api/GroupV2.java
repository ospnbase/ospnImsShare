package com.ospn.api;

import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.command.CmdCreateGroup;
import com.ospn.command.MsgData;
import com.ospn.common.ECUtils;
import com.ospn.common.OsnUtils;
import com.ospn.data.GroupData;
import com.ospn.data.MemberData;
import com.ospn.data.RunnerData;
import com.ospn.data.UserData;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.ospn.OsnIMServer.*;


import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.E_dataBase;
import static com.ospn.data.Constant.E_noSupport;
import static com.ospn.data.MemberData.*;
import static com.ospn.service.MessageService.*;
import static com.ospn.utils.CryptUtils.*;

@Slf4j
public class GroupV2 {

    public static void handleCreateGroup(RunnerData runnerData, JSONObject data, JSONObject json, MsgData msg) {

        //log.info("begin.");

        if (groupServer.length() < 32) {
            log.info("begin. createGroup");
            createGroup(runnerData, data, json, msg);

            return;
        }



        //
        if (isUser(msg.from)) {
            /** 消息是用户发来的，将消息打包以后，发给group server **/
            log.info(" send to Group server ");
            SendEncDataToGroup(data, json, msg);
        } else if (isService(msg.from)) {
            /** 消息是服务器发来的，判断是否是ims，是则创建群 **/
            handleCreateGroupFromServer(data, json, msg);
        }

    }

    public static void handleAddAttribute(JSONObject data, JSONObject json, MsgData msg) {

        //
        if (isUser(msg.from)) {
            /** 消息是用户发来的，将消息打包以后，发给group server **/
            SendEncDataToGroup(data, json, msg);
        }

    }



    private static void SendEncDataToGroup(JSONObject data, JSONObject json, MsgData msg) {

        //log.info("begin.");
        try {

            // 添加群主
            if (!data.containsKey("owner")){
                data.put("owner", msg.from);
            }
            // 添加群所有者
            if (!data.containsKey("owner2")){
                data.put("owner2", globalOwnerID);
            }

            /*log.info("data : " + data);
            log.info("service.osnID : " + service.osnID);
            log.info("groupServer : " + groupServer);
            log.info("service.osnKey : " + service.osnKey);
            log.info("json : " + json);*/

            JSONObject result = makeMessage(
                    "EncData",
                    service.osnID,
                    groupServer,
                    data,
                    service.osnKey, json);

            log.info("from : " + service.osnID);
            log.info("to : " + groupServer);

            sendOsxMessageNoSave(result);



        } catch (Exception e) {
            log.error("", e);
        }

    }

    private static void createGroup(RunnerData runnerData, JSONObject cmdContent, JSONObject json, MsgData msg) {

        try {

            CmdCreateGroup cmd = new CmdCreateGroup(cmdContent);
            log.info("members : " + cmd.userList);

            // 3. 生成群
            GroupData groupData = new GroupData(cmd, cmd.owner);

            groupData.maxMember = 500;

            // 4. 创建member列表
            List<String> needShare;
            if (cmd.userList != null) {
                needShare = cmd.userList; //createMemberList(groupData, cmd.userList);
                needShare.remove(cmd.owner);
            } else {
                needShare = new ArrayList<>();
            }


            MemberData ownerData = groupData.getMember(groupData.owner);
            if (ownerData == null){
                ownerData = new MemberData(
                        groupData.owner,
                        groupData.osnID,
                        MemberType_Owner,
                        "",
                        groupData.aesKey);
                groupData.addMember(ownerData);
            }
            ownerData.setStatus(MemberStatus_Save);
            ownerData.inviter = "";

            // 5. 存储到数据库中
            if (!db.group.insert(groupData)){
                log.info("error. insert group error");

                sendReplyCode(runnerData, E_dataBase, null);
                return;
            }
            if (!db.groupMember.insert(ownerData)){
                log.info("error. insert groupMember error");
                sendReplyCode(runnerData, E_dataBase, null);
                return;
            }


            // add member to group , new add by 20220921
            needShare = addMember(groupData, needShare);



            //log.info("[CreateGroup] add to map");
            // 6. 添加到缓存中
            groupMap.put(groupData.osnID, groupData);
            // 7. 推送到connector
            OsnIMServer.pushOsnID(groupData);

            // 8. 发送replay 给群主
            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            if (needShare.size() > 0){
                data.put("userList", needShare);
            }
            JSONObject result = makeMessage("Replay", groupData.osnID, cmd.owner, data, groupData.osnKey, json);
            UserData ownerUd = getUserData(cmd.owner);
            if (ownerUd != null)
            {
                sendClientMessageNoSave(ownerUd, result);
            } else {
                //log.info("send replay : " + result);
                sendOsxMessageNoSave(result);
            }

            // 给成员发送newly
            for (MemberData memberData : groupData.members.values()){
                newlyGroup(groupData, memberData.osnID);
            }

            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
            log.info("error input : " + cmdContent);
        }

        return;
    }

    private static void handleCreateGroupFromServer(JSONObject cmdContent, JSONObject json, MsgData msg) {

        try {

            log.info("from : " + msg.from);
            // 1. 判断from是否来自ims
            if (!imsList.contains(msg.from)) {
                log.info("error : " + msg.from);
                return;
            }

            // 2. 将json转化成command CreateGroup
            //log.info("[CreateGroup] from : " + msg.from);
            CmdCreateGroup cmd = new CmdCreateGroup(cmdContent);
            log.info("members : " + cmd.userList);

            // 3. 生成群
            GroupData groupData = new GroupData(cmd, cmd.owner);
            groupData.maxMember = 500;

            //log.info("[CreateGroup] owner : " +groupData.owner);
            //log.info("[CreateGroup] owner2 : " +groupData.owner2);
            //log.info("[CreateGroup] group id : " +groupData.osnID);
            //log.info("[CreateGroup] group key : " +groupData.osnKey);


            // 4. 创建member列表
            List<String> needShare;
            if (cmd.userList != null) {
                needShare = cmd.userList; //createMemberList(groupData, cmd.userList);
                needShare.remove(cmd.owner);
            } else {
                needShare = new ArrayList<>();
            }


            MemberData ownerData = groupData.getMember(groupData.owner);
            if (ownerData == null){
                ownerData = new MemberData(
                        groupData.owner,
                        groupData.osnID,
                        MemberType_Owner,
                        "",
                        groupData.aesKey);
                groupData.addMember(ownerData);
            }
            ownerData.setStatus(MemberStatus_Save);
            ownerData.inviter = "";

            // 5. 存储到数据库中
            if (!db.group.insert(groupData)){
                log.info("error. insert group error");
                // 不需要发送，超时就行了
                return;
            }
            if (!db.groupMember.insert(ownerData)){
                log.info("error. insert groupMember error");
                // 不需要发送，超时就行了
                return;
            }


            // add member to group , new add by 20220921
            needShare = addMember(groupData, needShare);






            //log.info("[CreateGroup] add to map");
            // 6. 添加到缓存中
            groupMap.put(groupData.osnID, groupData);
            // 7. 推送到connector
            OsnIMServer.pushOsnID(groupData);

            // 8. 发送replay 给群主
            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            if (needShare.size() > 0){
                data.put("userList", needShare);
            }
            JSONObject result = makeMessage("Replay", groupData.osnID, cmd.owner, data, groupData.osnKey, json);
            UserData ownerUd = getUserData(cmd.owner);
            if (ownerUd != null)
            {
                sendClientMessageNoSave(ownerUd, result);
            } else {
                //log.info("send replay : " + result);
                sendOsxMessageNoSave(result);
            }


            // 给成员发送newly
            for (MemberData memberData : groupData.members.values()){
                newlyGroup(groupData, memberData.osnID);
            }

        } catch (Exception e) {
            log.error("", e);
            log.info("error input : " + cmdContent);
        }

        return;
    }


    public static String createGroupFromHttp(JSONObject cmdContent) {

        try {

            // 2. 将json转化成command CreateGroup
            //log.info("[CreateGroup] from : " + msg.from);

            CmdCreateGroup cmd = new CmdCreateGroup(cmdContent);
            log.info("members : " + cmd.userList);

            // 3. 生成群
            GroupData groupData = new GroupData(cmd, cmd.owner);
            groupData.maxMember = 500;

            //log.info("[CreateGroup] owner : " +groupData.owner);
            //log.info("[CreateGroup] owner2 : " +groupData.owner2);
            //log.info("[CreateGroup] group id : " +groupData.osnID);
            //log.info("[CreateGroup] group key : " +groupData.osnKey);


            // 4. 创建member列表
            List<String> needShare;
            if (cmd.userList != null) {
                needShare = cmd.userList; //createMemberList(groupData, cmd.userList);
                needShare.remove(cmd.owner);
            } else {
                needShare = new ArrayList<>();
            }


            MemberData ownerData = groupData.getMember(groupData.owner);
            if (ownerData == null){
                ownerData = new MemberData(
                        groupData.owner,
                        groupData.osnID,
                        MemberType_Owner,
                        "",
                        groupData.aesKey);
                groupData.addMember(ownerData);
            }
            ownerData.setStatus(MemberStatus_Save);
            ownerData.inviter = "";

            // 5. 存储到数据库中
            if (!db.group.insert(groupData)){
                log.info("error. insert group error");
                // 不需要发送，超时就行了
                return null;
            }
            if (!db.groupMember.insert(ownerData)){
                log.info("error. insert groupMember error");
                // 不需要发送，超时就行了
                return null;
            }


            // add member to group , new add by 20220921
            needShare = addMember(groupData, needShare);






            //log.info("[CreateGroup] add to map");
            // 6. 添加到缓存中
            groupMap.put(groupData.osnID, groupData);
            // 7. 推送到connector
            OsnIMServer.pushOsnID(groupData);

            // 8. 发送replay 给群主
            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            if (needShare.size() > 0){
                data.put("userList", needShare);
            }
            JSONObject result = makeMessage("Replay", groupData.osnID, cmd.owner, data, groupData.osnKey, null);
            UserData ownerUd = getUserData(cmd.owner);
            if (ownerUd != null)
            {
                sendClientMessageNoSave(ownerUd, result);
            } else {
                //log.info("send replay : " + result);
                sendOsxMessageNoSave(result);
            }


            // 给成员发送newly
            for (MemberData memberData : groupData.members.values()){
                newlyGroup(groupData, memberData.osnID);
            }

            return groupData.osnID;

        } catch (Exception e) {
            log.error("", e);
            log.info("error input : " + cmdContent);
        }

        return null;
    }

    private static void newlyGroup(GroupData groupData, String userID) {
        JSONObject data = groupData.toPublicJson();
        data.put("state", "NewlyGroup");
        JSONObject json = makeMessage("GroupUpdate", groupData.osnID, userID, data, groupData.osnKey, null);
        log.info("userID: " + userID + ", groupID: " + groupData.osnID);
        sendMessage(userID, json);
    }

    private static List<String> addMember(GroupData groupData, List<String> members) {

        log.info("members : " + members);
        List<String> needShare = new ArrayList<>();

        needShare.addAll(members);

        for (String member : members) {

            MemberData memberData = new MemberData(
                    member,
                    groupData.osnID,
                    MemberType_Normal,
                    "",
                    groupData.aesKey);
            memberData.setStatus(0);


            log.info("insert : " + memberData.osnID);

            if (!db.groupMember.insert(memberData)){
                log.info("error. insert groupMember error");
                // 不需要发送，超时就行了
                return needShare;
            }

            groupData.addMember(memberData);

            needShare.remove(member);

        }

        log.info("need share size : " + needShare.size());

        return needShare;

    }



    public static void handleRecall(JSONObject data, GroupData groupData, MsgData msg, JSONObject json){

        /**
         * data 的格式
         * command : Recall
         * from :
         * to :
         * messageHash :
         * sign :
         * **/

        try {
            // 1. 验证签名
            // 2. 分发
            String command = data.getString("command");
            String from = data.getString("from");
            String to = data.getString("to");
            // 如何向服务器证明这条消息是我曾经发的？
            String messageHash = data.getString("messageHash");
            String sign = data.getString("sign");

            if (!groupData.osnID.equalsIgnoreCase(to)) {
                log.info("error : group(" +groupData.osnID+ ") to(" +to+ ")");
                return;
            }


            String calc = command + from + to + messageHash;
            String hash = ECUtils.osnHash(calc.getBytes());

            if (!ECUtils.osnVerify(from, hash.getBytes(), sign)) {
                log.info("verify error");
                log.info(data.toString());
                return;
            }

            log.info("forwardMessage");
            forwardMessage(data, groupData, json);


        } catch (Exception e) {
            log.info("error data : " + data);
        }


    }

    public static void handleDeleteMessage(JSONObject data, GroupData groupData, MsgData msg, JSONObject json){

        /**
         * data 的格式
         * command : DeleteMessage
         * from :
         * to :
         * messageHash :
         * sign :
         * **/

        try {
            // 判断from是否是管理员
            if (!groupData.isAdmin(msg.from)) {
                return ;
            }
            // 1. 验证签名
            // 2. 分发
            String command = data.getString("command");
            String from = data.getString("from");
            String to = data.getString("to");
            // 如何向服务器证明这条消息是我曾经发的？
            String messageHash = data.getString("messageHash");
            String sign = data.getString("sign");

            if (!groupData.osnID.equalsIgnoreCase(to)) {
                log.info("error : group(" +groupData.osnID+ ") to(" +to+ ")");
                return;
            }

            String calc = command + from + to + messageHash;
            String hash = ECUtils.osnHash(calc.getBytes());

            if (!ECUtils.osnVerify(from, hash.getBytes(), sign)) {
                log.info("verify error");
                log.info(data.toString());
                return;
            }

            forwardMessage(data, groupData, json);


        } catch (Exception e) {
            log.info("error data : " + data);
        }

    }

    public static void handleUpdateAttribute(JSONObject data, GroupData groupData, MsgData msg, JSONObject json){

        /**
         * data 的格式
         * command : UpdateAttribute
         * from :
         * to :
         * attribute : {dapp:xxxxxxxxxxxxxx}
         * sign :
         * **/

        try {
            // 判断from是否是管理员
            if (!groupData.isAdmin(msg.from)) {
                return ;
            }
            // 1. 验证签名
            String command = data.getString("command");
            String from = data.getString("from");
            String to = data.getString("to");

            String attribute = data.getString("attribute");

            String calc = command + from + to + attribute;
            String hash = ECUtils.osnHash(calc.getBytes());

            if (!ECUtils.osnVerify(msg.to, hash.getBytes(), groupData.osnKey)) {
                return;
            }

            // 验证是否是管理员
            if (!groupData.isAdmin(msg.from)) {
                return;
            }



            // 获取group的attribute
            JSONObject jsonGroupAttribute;
            if (groupData.attribute == null ){
                jsonGroupAttribute = new JSONObject();
            } else {
                jsonGroupAttribute = JSONObject.parseObject(groupData.attribute);
            }

            JSONObject jsonAddAttribute = JSONObject.parseObject(attribute);
            jsonGroupAttribute.putAll(jsonAddAttribute);

            db.group.setAttribute(groupData, jsonGroupAttribute.toString());




            forwardMessage(data, groupData, json);


        } catch (Exception e) {
            log.info("error data : " + data);
        }

    }


    private static void forwardMessage(JSONObject data, GroupData groupData, JSONObject json) {

        try {

            // 用group 的aeskey 进行加密
            byte[] aesKey = Base64.getDecoder().decode(groupData.aesKey);
            String content = OsnUtils.aesEncrypt(data.toString().getBytes(), aesKey);

            // 将content直接传入进行数据组合
            for (MemberData md  : groupData.members.values()) {
                JSONObject pack = packGroupMessage(json, groupData.osnID, md, groupData.osnKey, content);
                // 发送
                sendMessage(md.osnID, pack);
            }


        } catch (Exception e) {
            log.info("forwardMessage Exception");
        }

    }




}
