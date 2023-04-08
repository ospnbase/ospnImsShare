package com.ospn.service;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ospn.OsnIMServer;
import com.ospn.command.*;
import com.ospn.command2.*;
import com.ospn.common.ECUtils;
import com.ospn.common.OsnUtils;
import com.ospn.core.IMData;
import com.ospn.data.*;
import com.ospn.server.GroupNoticeServer;
import com.ospn.server.MessageSaveServer;
import com.ospn.server.PushServer;
import com.ospn.utils.data.GroupHistoryMessageData;
import com.ospn.utils.data.MessageSaveData;
import com.ospn.utils.data.PushData;
import com.ospn.utils.http.Http;
import lombok.extern.slf4j.Slf4j;


import java.util.*;

import static com.ospn.OsnIMServer.*;
import static com.ospn.api.GroupV2.*;
import static com.ospn.command.CmdMute.mode_mute_all;
import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.*;
import static com.ospn.data.GroupData.*;
import static com.ospn.data.MemberData.*;
import static com.ospn.service.MessageService.*;
import static com.ospn.utils.CryptUtils.*;
import static com.ospn.utils.db.DBMessage.MQCode;
import static com.ospn.utils.db.DBMessage.MQOSX;

@Slf4j
public class GroupService {

    public static Void handleMessage(RunnerData runnerData){

        MsgData msg = new MsgData(runnerData.json);
        if (msg.to == null){
            log.info("[GroupService::handleMessage] valid info : " + runnerData.json);
            return null;
        }

        if (!isGroup(msg.to)){
            log.info("[GroupService::handleMessage] valid info : " + runnerData.json);
            return null;
        }

        if (!isInThisNode(msg.from) && !isInThisNode(msg.to)){
            log.info("[GroupService::handleMessage] no from, no to");
            return null;
        }

        String group = msg.to;
        GroupData groupData = getGroupData(group);
        if (groupData == null){
            // 外发
            handleMessageNodeSelf(runnerData);
            sendReplyCode(runnerData, null, null, msg.command);
            if (!sendByHttp2(runnerData.json)) {
                sendOsxMessage(runnerData.json);
            }
            return null;
        }

        String command = msg.command;
        if (command == null){
            log.info("[GroupService::handleMessage] no command");
            return null;
        }
        Void errCode = null;

        if (groupData.type == 99){
            if (!command.equalsIgnoreCase("ReGroupChange")){
                return null;
            }
        }

        switch(command){
            case "Message":
                // 用户给群发的消息，需要群进行转发
                // 扔队列里处理
                errCode = Message(runnerData);
                break;

            case "EncData":
                errCode = EncData(runnerData);
                break;

            case "JoinGroup":
                // 加入群
                errCode = JoinGroup(runnerData);
                break;

            case "AgreeMember":
                errCode = AgreeMember(runnerData);
                break;

            case "RejectMember":
                //errCode = RejectMember(runnerData);
                sendReplyCode(runnerData, null, null);
                break;


            case "GetGroupSign":
                errCode = GetGroupSign(runnerData);
                break;
            case "GetOwnerSign":
                errCode = GetOwnerSign(runnerData);
                break;

            case "GetGroupInfo":
                // 获取群信息 可远程获取
                errCode = GetGroupInfo(runnerData);
                break;




            case "Mute":
                errCode = Mute(runnerData);
                break;

            case "Billboard":
                errCode = Billboard(runnerData);
                break;













            case "SystemNotify":
                // 发送系统消息， 不需要返回
                // notice 不需要实时性
                // 扔队列里慢慢处理
                errCode = SystemNotify(runnerData);
                break;


            case "Invitation":
                // 生成邀请函
                errCode = Invitation(runnerData);
                break;


            case "JoinGrp":
                // 加入群 使用邀请函加群
                errCode = JoinGrp(runnerData);
                break;






            // 新机制，群不会邀请人，用户不用拒绝，也不用同意
            // case "RejectJoin":
            //    errCode = RejectJoin(runnerData);
            //    break;
            //case "AgreeJoin":
            //    errCode = AgreeJoin(runnerData);
            //    break;


            case "SetMaxMember":
                errCode = SetMaxMember(runnerData);
                break;

            case "SetAdmin":
                errCode = SetAdmin(runnerData);
                break;

            case "NewOwner":
                errCode = NewOwner(runnerData);
                break;

            case "AddAdmin":
                errCode = AddAdmin(runnerData);
                break;

            case "DelAdmin":
                errCode = DelAdmin(runnerData);
                break;

            case "AddMember":
                errCode = AddMember(runnerData);
                break;

            case "DelMember":
                errCode = DelMember(runnerData);
                break;

            case "SetMemberInfo":
                errCode = SetMemberInfo(runnerData);
                break;

            case "SetGroupInfo":
                errCode = SetGroupInfo(runnerData);
                break;

            case "UpAttribute":
                errCode = UpAttribute(runnerData);
                break;


            case "RemoveAttribute":
                errCode = RemoveAttribute(runnerData);
                break;


            case "UpDescribe":
                errCode = UpDescribe(runnerData);
                break;

            case "RemoveDescribe":
                errCode = RemoveDescribe(runnerData);
                break;


            case "UpPrivateInfo":
                errCode = UpPrivateInfo(runnerData);
                break;



            case "QuitGroup":
                errCode = QuitGroup(runnerData);
                break;

            case "DelGroup":
                errCode = DelGroup(runnerData);
                break;

            case "GetMemberInfo":
                errCode = GetMemberInfo(runnerData);
                break;

            case "GetMemberZone":
                errCode = GetMemberZone(runnerData);
                break;

            case "GetService":
                // 获取群所在节点的id
                errCode = GetService(runnerData);
                break;

            case "ReGroupChange":
                errCode = ReGroupChange(runnerData);
                break;


            default:
                log.info("[GroupService::HandleMessage] error command : " + command);
                break;
        }

        if (!isInThisNode(msg.from)){
            sendComplete(msg.hash, groupData, msg.from);
        }
        return errCode;
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

            case "QuitGroup":
                return QuitGroup(runnerData, msg);

            case "DelGroup":
                return DelGroup(runnerData, msg);
        }

        return null;

    }

    public static Void CreateGroup(RunnerData runnerData) {
        try {

            if (runnerData != null) {
                sendReplyCode(runnerData, E_stateError, null);
                return null;
            }


            if (imType.equalsIgnoreCase("group")) {
                // group 类型的ims 不支持这种格式的创建群
                log.info("[CreateGroup] imType is group.");
                return null;
            }

            MsgData msg = new MsgData(runnerData.json);
            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                log.info("[CreateGroup] error. udFrom == null");
                return null;
            }

            // 没有私钥不能创建群
            if (udFrom.osnKey == null) {
                log.info("[CreateGroup] error. osn user can not create group");
                return null;
            }
            if (udFrom.osnKey.equalsIgnoreCase("")){
                log.info("[CreateGroup] error. osn user can not create group");
                return null;
            }

            //log.info("[CreateGroup] from : " + msg.from);
            CmdCreateGroup cmd = new CmdCreateGroup(msg.getContent(service.osnKey, udFrom.aesKey));

            GroupData groupData = new GroupData(cmd, msg.from);

            //log.info("[CreateGroup] owner : " +groupData.owner);
            //log.info("[CreateGroup] owner2 : " +groupData.owner2);
            //log.info("[CreateGroup] group id : " +groupData.osnID);
            //log.info("[CreateGroup] group key : " +groupData.osnKey);


            // 创建member列表
            log.info("[CreateGroup] createMemberList");
            List<String> needShare = createMemberList(groupData, cmd.userList);
            if (groupData.members.size() > groupData.maxMember){
                log.info("[CreateGroup] error. member is more than max.");
                sendReplyCode(runnerData, E_maxLimits, null);
                return null;
            }

            //log.info("[CreateGroup] save");
            // 存储到数据库中
            if (!db.group.insert(groupData)){
                log.info("[CreateGroup] error. insert group error");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            if (!db.groupMember.insertMembers(groupData.members.values())){
                log.info("[CreateGroup] error. insert groupMember error");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }






            //log.info("[CreateGroup] add to map");
            // 添加到缓存中
            groupMap.put(groupData.osnID, groupData);
            OsnIMServer.pushOsnID(groupData);




            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            if (needShare.size() > 0){
                data.put("userList", needShare);
            }
            JSONObject result = makeMessage("Replay", service.osnID, msg.from, data, service.osnKey, runnerData.json);
            sendClientMessageNoSave(runnerData.sessionData, result);


            // 发送消息
            /*
            CmdReplay reCmd = new CmdReplay(groupData.osnID);
            if (needShare.size() > 0)
                reCmd.userList = needShare;
            JSONObject result = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);
            sendClientMessageNoSave(runnerData.sessionData, result);*/



            for (MemberData memberData : groupData.members.values()){
                newlyGroup(groupData, memberData.osnID);
            }





        } catch (Exception e) {
            log.error("", e);
            log.info("[CreateGroup] error json : " + runnerData.json);
        }
        return null;
    }

    public static Void GetService(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);

            GroupData groupData = getGroupData(msg.to);

            if (groupData.owner.equalsIgnoreCase(msg.from)
                    ||groupData.owner2.equalsIgnoreCase(msg.from)){

                CmdGetService cmd = new CmdGetService(msg.getContent(groupData.osnKey));

                JSONObject data = new JSONObject();
                data.put("time", cmd.time);
                data.put("service", service.osnID);
                JSONObject result = makeMessage(
                        "ServiceInfo",
                        service.osnID,
                        msg.from,
                        data,
                        service.osnKey,
                        runnerData.json);
                sendMessageNoSave(msg.from, result);

            }

        } catch (Exception e) {
            log.error("", e);
            log.info("[CreateGroup] error json : " + runnerData.json);
        }
        return null;
    }

    public static Void GenGrp(RunnerData runnerData) {
        // 生成group 仅和owner通信
        try {
            SessionData sessionData = runnerData.sessionData;
            MsgData msg = new MsgData(runnerData.json);

            UserData udFrom = getUserData(msg.from);
            if (udFrom == null){
                return null;
            }

            JSONObject contentJson = msg.getContent(service.osnKey, udFrom.aesKey);
            if (contentJson == null) {
                return null;
            }

            CmdGenGrp cmd = new CmdGenGrp(contentJson);
            // 以上代码为复用代码



            String owner = msg.from;

            String[] osnData = ECUtils.createOsnID("group");
            if (osnData == null){
                log.info("[GenGrp] error E_genAccount.");
                sendReplyCode(runnerData, E_genAcount, null);
                return null;
            }


            GroupData groupData = new GroupData();
            groupData.osnID = osnData[0];
            groupData.name = cmd.name;
            groupData.osnKey = osnData[1];
            groupData.owner = owner;
            groupData.owner2 = globalOwnerID;
            groupData.portrait = cmd.portrait;
            groupData.type = cmd.type;
            groupData.joinType = cmd.joinType;
            groupData.passType = cmd.passType;

            byte[] aesKey = OsnUtils.getAesKey();       //  生成随机AESKEY
            groupData.aesKey = Base64.getEncoder().encodeToString(aesKey);      // 生成AESKEY，这里只能用base64
            groupData.validType();










            MemberData memberData = new MemberData(owner, groupData.osnID, MemberType_Owner, "", groupData.aesKey);
            memberData.setStatus(MemberStatus_Save);
            groupData.addMember(memberData);

            if (!db.group.insert(groupData)){
                log.info("[GenGrp] error insertGroup.");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }

            if (!db.groupMember.insert(memberData)){
                log.info("[GenGrp] error insertMembers.");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }

            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            JSONObject result = makeMessage("Replay", service.osnID, owner, data, service.osnKey, runnerData.json);
            sendClientMessageNoSave(sessionData, result);
            newlyGroup(groupData, owner);
            OsnIMServer.pushOsnID(groupData);

            // 如果附带了邀请函，则发送邀请函
            if (cmd.invitations != null){
                createInvitations(groupData, cmd.invitations);
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void Invitation(RunnerData runnerData) {
        // 仅和成员通信
        try {

            MsgData msg = new MsgData(runnerData.json);
            JSONObject contentJson = msg.getContent();


            CmdInvitation cmd = new CmdInvitation(contentJson);
            // 以上代码为复用代码


            String group = msg.to;
            GroupData groupData = getGroupData(group);



            // 验证权限
            if (!groupData.hasMember(msg.from)){
                // 都不是成员还来申请邀请函？
                log.info("[Invitation] not member");
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }
            if (groupData.joinType == GroupJoinType_Admin){
                if (!groupData.isAdmin(msg.from)){
                    sendReplyCode(runnerData, E_notAdmin, null);
                    return null;
                }
            }

            if (cmd.invitations == null){
                log.info("[Invitation] error , invitations null.");
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }

            //log.info("[Invitation] invitations count :" + cmd.invitations.size());
            List<String> invitationLetters = createInvitations(groupData, cmd.invitations);
            if (invitationLetters == null){
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }

            JSONObject content = new JSONObject();
            content.put("invitations", invitationLetters);


            // 以下代码为复用代码
            if (isInThisNode(msg.from)){
                sendReplyCode(runnerData, null, content);
            } else {

                JSONObject reMessage = makeMessage(
                        "Invitation",
                        groupData.osnID,
                        msg.from,
                        content,
                        groupData.osnKey,
                        null);
                sendOsxMessageNoSave(reMessage);

                /*// 如果不是本节点的，我就给他发notice过去
                GroupNoticeData notice = new GroupNoticeData();
                notice.groupID = group;
                notice.state = "invitation";
                notice.invitations = invitationLetters;
                notifyOne(groupData, msg.from, notice.toJson());*/
            }

        } catch (Exception e) {
            log.error("", e);
        }
        //sendReplyCode(runnerData, null, null);
        return null;
    }

    public static Void JoinGrp(RunnerData runnerData) {
        // join group 分两种
        // 1. 带了邀请函的
        // 2. 不带邀请函的
        ErrorData errorData = null;
        try {
            MsgData msg = new MsgData(runnerData.json);
            String group = msg.to;
            GroupData groupData = getGroupData(group);

            if (groupData.full()){
                sendReplyCode(runnerData, E_groupFull, null);
                return null;
            }




            JSONObject contentJson = msg.getContent(groupData.osnKey);
            CmdJoinGrp cmd = new CmdJoinGrp(contentJson);



            // 判断是否带 邀请函
            if (cmd.letter == null){
                //log.info("[JoinGrp] joinGroupNoInvitation.");
                errorData = E_formatError;    //joinGroupNoInvitation(cmd, groupData, msg.from);
            } else {
                //log.info("[JoinGrp] joinGroupWithInvitation.");
                errorData = joinGroupWithInvitation(cmd, groupData);
            }


        } catch (Exception e) {
            log.error("", e);
        }
        sendReplyCode(runnerData, errorData, null);
        return null;
    }

    public static Void AgreeJoin(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            JSONObject contentJson = msg.getContent();
            if (contentJson == null){
                return null;
            }
            CmdAgreeJoin cmd = new CmdAgreeJoin(contentJson);

            String adminID = msg.from;
            String groupID = msg.to;


            GroupData groupData = getGroupData(groupID);


            //isAdmin
            if (!groupData.isAdmin(adminID)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            // 添加邀请人
            MemberData newMember = new MemberData(
                    cmd.userID,
                    groupData.osnID,
                    MemberType_Normal,
                    null,
                    groupData.aesKey);
            if (!db.groupMember.insert(newMember)){
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            groupData.addMember(newMember);

            // 通知到群
            // 通知
            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = groupData.osnID;
            notice.state = "AddMember";
            notice.approver = msg.from;
            notice.addUser(cmd.userID);
            //systemNotifyGroup(groupData, notice.toJson());
            GroupNoticeServer.push(notice);
            sendReplyCode(runnerData, null, null);



            //ErrorData errorData = newlyMember(groupData, cmd.userID);
            //sendReplyCode(runnerData, errorData, null);


        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void RejectJoin(RunnerData runnerData) {
        // 机制 message 直接转发,不需要经过群
        return forwardMessage(runnerData);
    }

    public static Void Message(RunnerData runnerData){

        try {
            JSONObject msg = runnerData.json;
            if (msg == null) {
                return null;
            }
            String groupID = msg.getString("to");
            if (groupID == null) {
                return null;
            }

            // 消息分发过来以后
            String userID = msg.getString("from");

            GroupData groupData = getGroupData(groupID);

            //处理本节点的分发


            // 判断是否是本群成员
            MemberData memberData = groupData.getMember(userID);
            if (memberData == null){
                log.info("[GroupService::Message] memberData == null");
                if (hasUser(userID)){
                    //如果是本节点用户，就通知他消息已经收到
                    sendReplyCode(runnerData, E_notMember, null);
                }
                return null;
            }
            // 判断该成员是否被禁言
            if (memberData.mute == 1){
                log.info("[GroupService::Message] memberData.mute == 1");
                if (hasUser(userID)){
                    //如果是本节点用户，就通知他消息已经收到
                    sendReplyCode(runnerData, E_mute, null);
                }
                return null;
            }
            if (groupData.mute == 1){
                if (!groupData.isAdmin(userID)){
                    log.info("[GroupService::Message] group all mute");
                    if (hasUser(userID)){
                        //如果是本节点用户，就通知他消息已经收到
                        sendReplyCode(runnerData, E_mute, null);
                    }
                    return null;
                }
            }


            // 1. 首先需要对content进行解密
            // 2. 用groupdata里的aeskey来对content进行加密
            String content;
            try
            {
                content = reEncryptContent(msg, groupData.aesKey, groupData.osnKey);
            }catch (Exception e){
                log.info("[GroupService::Message] error:" + e);
                if (hasUser(userID)){
                    //如果是本节点用户，就通知他消息已经收到
                    sendReplyCode(runnerData, E_formatError, null);
                }
                return null;
            }

            if (content == null){
                log.info("[GroupService::Message] error: reEncryptContent failed.");
                if (hasUser(userID)){
                    //如果是本节点用户，就通知他消息已经收到
                    sendReplyCode(runnerData, E_formatError, null);
                }
                return null;
            }



            /**
             * 存储群消息
             * **/
            //log.info("recv group message.");
            //saveGroupMessageToHistory(groupData, msg, content);

            //UserData userData = IMData.getUserData(userID);
            if (hasUser(userID)) {
                sendReplyCode(runnerData, null, null);
            }
            /*if (isInThisNode(userID)){
                //如果是本节点用户，就通知他消息已经收到
                sendReplyCode(runnerData, null, null);
            }*/

            //

            // 验证成功，保存msg
            //log.info("[GroupService::forwardMessage] saveCompleteMessage:" + msg);
            //log.info("save Complete Message");
            // saveCompleteMessage(msg);
            // 保存失败也没关系
            log.info("create group messages begin.");

            // 以下是分发给各个成员
            // 以下耗时操作扔队列里处理

            List<JSONObject> msgs = new ArrayList<>();
            List<JSONObject> msgs2 = new ArrayList<>();

            // 3. 将content直接传入进行数据组合
            for (MemberData md  : groupData.members.values()) {
                // 不需要给发送者发送
                if (md.osnID.equalsIgnoreCase(userID))
                    continue;
                JSONObject pack;
                if (md.receiverKey == null){
                    pack = packMessage(msg, groupData.osnID, md.osnID, groupData.osnKey);
                }
                else if (md.receiverKey.equalsIgnoreCase("")){
                    pack = packMessage(msg, groupData.osnID, md.osnID, groupData.osnKey);
                }
                else {
                    pack = packGroupMessage(msg, groupData.osnID, md, groupData.osnKey, content);
                }

                //log.info("BEGIN.");
                if (hasUser(md.osnID)) {
                //if (isInThisNode(md.osnID)){
                    msgs.add(pack);
                }else {
                    msgs2.add(pack);
                }
                //log.info("END.");
            }

            //log.info("[GroupService::forwardMessage] msgs1 count:"+msgs.size());
            //log.info("[GroupService::forwardMessage] msgs2 count:"+msgs2.size());
            //log.info("create group messages begin.");
            log.info("create group messages end. write message to redis. begin");

            // 将消息写入redis
            if (msgs.size() > 0){

                MessageSaveServer.push(new MessageSaveData(msgs, MQCode));
                /*if (!db.message.insertMessagesInRedis(msgs, db.message.MQCode)){
                    log.info("[GroupService::Message] error : insertMessagesInRedis");
                }*/

                // 需要先存，再发
                for (JSONObject message : msgs){
                    String to = message.getString("to");
                    // 判断成员是节点内，还是节点外
                    //UserData userData = IMData.getUserData(to);
                    UserData userData = db.user.readUserByID(to);
                    if (userData != null){
                        //log.info("[GroupService::Message] sendClientMessageNoSave :" + to);
                        sendClientMessageNoSave(userData, message);

                        // 发送消息通知
                        /*if (userData.owner2 != null) {
                            if (!userData.owner2.equalsIgnoreCase("")) {

                                PushData pushData = new PushData();
                                pushData.time = System.currentTimeMillis();
                                pushData.owner2 = userData.owner2;
                                pushData.user = userData.osnID;
                                pushData.type = "Message";
                                pushData.msgHash = message.getString("hash");
                                PushServer.push(pushData);
                            }
                        }*/

                    }
                }
            }

            // 以下是外发数据
            log.info("send message osx.");
            if (msgs2.size() > 0){
                MessageSaveServer.push(new MessageSaveData(msgs2, MQOSX));

                // 需要先存，再发
                for (JSONObject message : msgs2){
                    // 这里判断，加直发

                    if (!sendByHttp(message)){
                        sendOsxMessageNoSave(message);
                    }
                }
            }

            log.info("Group Message end.");

        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[GroupService::Message] error json : " + runnerData.json);
        }



        return null;
    }

    private static boolean sendByHttp(JSONObject message) {
        String to = message.getString("to");
        log.info("get tips begin.");
        List<String> tips = db.dbOsnid.getTip(to);
        log.info("tips : " + tips);
        if (tips.contains("111.111.111.111")) {
            tips.remove("111.111.111.111");
            // http send

            if (!Http.post("http://111.111.111.111:8300/message", message)) {
                log.info("http post error.");
                //sendOsxMessageNoSave(message);
                return false;
            } else {
                // true
                log.info("http post success. tips count:" + tips.size());
                if (tips.size() > 0) {
                    log.info("tips multi.");
                    //sendOsxMessageNoSave(message);
                    return false;
                }
                return true;
            }
        }

        return false;
    }

    private static boolean sendByHttp2(JSONObject message) {
        String to = message.getString("to");
        log.info("get tips begin.");
        List<String> tips = db.dbOsnid.getTip(to);
        log.info("tips : " + tips);
        if (tips.contains("111.111.111.111")) {
            tips.remove("111.111.111.111");
            // http send

            if (!Http.post("http://111.111.111.111:8300/message", message)) {
                log.info("http post error.");
                //sendOsxMessageNoSave(message);
                return false;
            } else {
                // true
                log.info("http post success. tips count:" + tips.size());
                if (tips.size() > 0) {
                    log.info("tips multi.");
                    //sendOsxMessageNoSave(message);
                    return false;
                }

                String command = message.getString("command");
                if (!noSaveList.contains(command)){
                    // 不在 不存列表中，需要存储
                    MessageSaveServer.push(new MessageSaveData(message, MQOSX));
                    //db.message.insertInRedis(toMessageData(msg, 0), MQOSX);
                }

                return true;
            }
        }

        return false;
    }

    private static void saveGroupMessageToHistory(GroupData groupData, JSONObject msg, String content) {
        /**
         * 存储群消息
         * msg需要存
         * content需要存
         * groupID需要存
         * timestamp需要存
         * hash需要存
         * Attribute的key是GetHistory
         *
         * **/

        try {

            String attrGetHistory = groupData.getAttribute("GetHistory");
            if (attrGetHistory != null) {
                if (attrGetHistory.equalsIgnoreCase("yes")) {
                    // 存数据
                    GroupHistoryMessageData historyMsg = new GroupHistoryMessageData();
                    historyMsg.group = groupData.osnID;
                    historyMsg.hash = msg.getString("hash");
                    historyMsg.msg = msg.toString();
                    historyMsg.content = content;
                    db.groupHistory.insert(historyMsg);
                }
            }

        } catch (Exception e) {
            log.info("save group message history error : " + e.getMessage());
        }

    }

    public static Void GetGroupList(RunnerData runnerData) {
        // 没有远程
        try {
            MsgData msg = new MsgData(runnerData.json);
            SessionData sessionData = runnerData.sessionData;

            List<String> groupList = db.group.list(msg.from, false);


            //log.info("[GetGroupList] group count: " + groupList.size());

            JSONObject data = new JSONObject();
            data.put("groupList", groupList);

            JSONObject result = makeMessage("GroupList", service.osnID, msg.from, data, service.osnKey, runnerData.json);
            sendClientMessageNoSave(sessionData, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void JoinGroup(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                sendReplyCode(runnerData, E_groupNoFind, null);
                return null;
            }

            if (groupData.getJoinType() != null) {
                return joinGroupByJoinType(runnerData, groupData, msg);
            }


            if (groupData.passType == GroupJoinType_None) {
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            if (groupData.hasMember(msg.from)) {
                // 已经是群成员了
                sendReplyCode(runnerData, null, null);
                return null;
            }

            if (groupData.full()){
                sendReplyCode(runnerData, E_groupFull, null);
                return null;
            }

            if (groupData.passType == GroupPassType_Free){
                return joinGroupMyself(runnerData, groupData, msg.from);
            }

            if (groupData.passType == GropuPassType_Admin){

                CmdJoinGroup cmd = new CmdJoinGroup(msg.getContent(groupData.osnKey));
                cmd.userID = msg.from;
                log.info("[JoinGroup] cmd : " + cmd.toJson());

                return forwardToAdmin(runnerData, cmd, groupData);
            }





            sendReplyCode(runnerData, E_formatError, null);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void AgreeMember(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            if (!groupData.isAdmin(msg.from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            CmdAgreeMember cmd = new CmdAgreeMember(msg.getContent(groupData.osnKey));
            if (cmd.userID == null){
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }
            agreeMemberJoin(groupData, cmd.userID, msg.from);
            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void RejectMember(RunnerData runnerData) {
        try {
            JSONObject json = runnerData.json;
            String adminID = json.getString("from");
            String groupID = json.getString("to");

            log.info("[RejectMember] adminID: " + adminID + ", groupID: " + groupID);

            GroupData groupData = getGroupData(groupID);


            JSONObject data = takeMessage(json);
            if (data == null) {
                sendReplyCode(runnerData, E_cryptError, null);
                return null;
            }

            sendReplyCode(runnerData, null, null);

            String userID = data.getString("userID");
            String reason = data.getString("reason");

            JSONObject toUserJson = new JSONObject();
            toUserJson.put("groupID", groupData.osnID);
            toUserJson.put("userID", userID);
            toUserJson.put("reason", reason);

            log.info("[RejectMember] send message to joiner: " + toUserJson);
            JSONObject pack = makeMessage("RejectMember",
                    groupID,
                    userID,
                    toUserJson,
                    groupData.osnKey,
                    null);
            sendMessage(userID, pack);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void GetGroupSign(RunnerData runnerData) {
        // 该消息不怎么重要，失败了就重新获取一次
        try {
            if (runnerData.json == null){
                log.info("[GroupService::GetGroupSign] json == null");
                return null;
            }
            MsgData msg = new MsgData(runnerData.json);


            String groupID = msg.to;
            String from = msg.from;
            if (groupID == null || from == null ){
                log.info("[GroupService::GetGroupSign] from to == null");
                return null;
            }

            // 不是群的不处理
            if (!isGroup(groupID)){
                log.info("[GroupService::GetGroupSign] not group");
                return null;
            }

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                log.info("[GroupService::GetGroupSign] not group");
                return null;
            }

            if(!groupData.hasMember(from)){
                sendReplyCode(runnerData, E_memberNoFind, null);
                return null;
            }


            JSONObject contentJson = msg.getContent();
            if (contentJson == null){
                log.info("[GroupService::GetGroupSign] contentJson null");
                return null;
            }
            //log.info("[GroupService::GetGroupSign] contentJson");

            CmdGetGroupSign cmd = new CmdGetGroupSign(contentJson);
            //log.info("[GroupService::GetGroupSign] CmdGetGroupSign : " + cmd.toJson());

            if (!cmd.create(groupData)){
                sendReplyCode(runnerData, E_errorCmd, null);
                return null;
            }

            JSONObject result = cmd.toJson();
            //log.info("[GroupService::GetGroupSign] toJson : " + result);
            JSONObject jsonBack = wrapMessage("GetGroupSign", groupID, from, result, groupData.osnKey, runnerData.json);
            // 回发消息不需要存，没有获得就再提交一次
            //log.info("[GroupService::GetGroupSign] sendMessage : " + jsonBack);
            sendMessageNoSave(from, jsonBack);

        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }

    public static Void Mute(RunnerData runnerData) {
        try {
            log.info("Mute begin.");
            MsgData msg = new MsgData(runnerData.json);
            JSONObject contentJson = msg.getContent();
            if (contentJson == null) {
                log.info("content is null.");
                return null;
            }
            CmdMute cmd = new CmdMute(contentJson);
            // 以上代码为复用代码

            //log.info("[Mute] members : " + cmd.members);
            //log.info("[Mute] mode : " + cmd.mode);
            //log.info("[Mute] mute : " + cmd.mute);


            String from = msg.from;
            String groupID = msg.to;

            //log.info("[Mute] userID: " + from + ", groupID: " + groupID);

            GroupData groupData = getGroupData(groupID);


            // 判断是否是管理员
            if (!groupData.isAdmin(from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                log.info("not admin.");
                return null;
            }

            if (cmd.mode == mode_mute_all){
                log.info("mute all.");
                // 全员禁言
                groupData.mute = cmd.mute;
                List<String> keys = new ArrayList<>();
                keys.add("mute");
                if (!db.group.update(groupData, keys)){
                    log.info("[Mute] error. update group");
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }

                // 发送通知
                GroupNoticeData notice = new GroupNoticeData();
                notice.groupID = groupID;
                notice.muteRange = "all";
                notice.mute = cmd.mute;
                if (cmd.mute == 0){
                    notice.state = "Allow";
                    notice.text = "Resumes talk.";
                } else {
                    notice.state = "Mute";
                    notice.text = "All member mute.";
                }

                //systemNotifyGroup(groupData, notice.toJson());
                GroupNoticeServer.push(notice);

                sendReplyCode(runnerData, null, null);


                return null;
            }

            List<String> userList = cmd.members;
            if (userList == null) {
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }
            if (userList.size() == 0){
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }


            int mute = cmd.mute;

            for (String member : userList){

                setMemberMute(groupData, member, mute);

                // 定向发送禁言
                GroupNoticeData notice = new GroupNoticeData();
                notice.groupID = groupID;
                notice.muteRange = "you";
                notice.mute = cmd.mute;
                if (cmd.mute == 0){
                    notice.state = "Allow";
                    notice.text = "The administrator resumes you.";
                } else {
                    notice.state = "Mute";
                    notice.text = "The administrator put a gag on you.";
                }
                notifyOne(groupData, member, notice.toJson());
            }


        } catch (Exception e) {
            log.error("", e);
        }
        sendReplyCode(runnerData, null, null);
        return null;
    }

    public static Void Billboard(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            JSONObject contentJson = msg.getContent();
            if (contentJson == null) {
                return null;
            }
            CmdGrpInf cmd = new CmdGrpInf(contentJson);
            // 以上代码为复用代码


            String from = msg.from;
            String groupID = msg.to;

            //log.info("[billboard] userID: " + from + ", groupID: " + groupID);

            GroupData groupData = getGroupData(groupID);


            if (!groupData.isAdmin(from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            //String billBoard = cmd.text;
            groupData.billboard = cmd.text;
            List<String> keys = new ArrayList<>();
            keys.add("billboard");
            if (!db.group.update(groupData, keys)){
                log.info("[billboard] error . update ");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }

            // 全员发送通知
            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = groupID;
            notice.state = "UpdateGroup";
            notice.addInfo("billboard");
            notice.billboard = groupData.billboard;
            //systemNotifyGroup(groupData, notice.toJson());
            GroupNoticeServer.push(notice);

        } catch (Exception e) {
            log.error("", e);
        }
        sendReplyCode(runnerData, null, null);
        return null;
    }

    public static Void GetOwnerSign(RunnerData runnerData) {
        // 该消息不怎么重要，失败了就重新获取一次
        try {
            if (runnerData.json == null){
                log.info("[GroupService::GetOwnerSign] json == null");
                return null;
            }
            MsgData msg = new MsgData(runnerData.json);

            String groupID = msg.to;
            String from = msg.from;
            if (groupID == null || from == null ){
                log.info("[GroupService::GetOwnerSign] from to == null");
                return null;
            }

            // 不是群的不处理
            if (!isGroup(groupID)){
                log.info("[GroupService::GetOwnerSign] not group");
                return null;
            }

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                //外发
                sendOsxMessageNoSave(runnerData.json);
                return null;
            }


            JSONObject contentJson = msg.getContent();
            if (contentJson == null){
                log.info("[GroupService::GetOwnerSign] contentJson null");
                return null;
            }


            CmdGetOwnerSign cmd = new CmdGetOwnerSign(contentJson);
            if(!groupData.owner.equalsIgnoreCase(cmd.owner)){
                // 跟cmd里的owner对不上，不处理
                log.info("[GroupService::GetOwnerSign] error. owner !=");
                return null;
            }

            if (!cmd.create(groupData)){
                sendReplyCode(runnerData, E_errorCmd, null);
                return null;
            }

            JSONObject result = cmd.toJson();
            //log.info("[GroupService::GetOwnerSign] toJson : " + result);
            JSONObject jsonBack = wrapMessage("GetOwnerSign", groupID, from, result, groupData.osnKey, runnerData.json);
            // 回发消息不需要存，没有获得就再提交一次
            //log.info("[GroupService::GetOwnerSign] toJson" + result);
            sendMessageNoSave(from, jsonBack);

        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }

    public static Void SetMemberInfo(RunnerData runnerData) {
        try {
            //log.info("[SetMemberInfo] to group");
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            CmdSetMemberInfo cmd = new CmdSetMemberInfo(msg.getContent(groupData.osnKey));

            if (cmd.nickName == null){
                return null;
            }

            MemberData memberData = groupData.getMember(msg.from);
            memberData.nickName = cmd.nickName;
            db.groupMember.setMemberNickName(memberData);
            sendReplyCode(runnerData, null, null);

            //发送通知
            /*JSONObject data = new JSONObject();
            data.put("groupID", msg.to);
            data.put("state", "UpdateMember");
            data.put("userList", Collections.singletonList(memberData.getMemberInfo()));
            List<String> keys = new ArrayList<>();
            keys.add("nickname");
            data.put("infoList", keys);
            log.info("[SetMemberInfo] notice : " + data);
            systemNotifyGroup(groupData, data);*/

            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = msg.to;
            notice.state = "UpdateMember";
            notice.addInfo("nickname");
            notice.memberInfo = memberData.getMemberInfo();
            GroupNoticeServer.push(notice);


            return null;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void SetGroupInfo(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            if (!groupData.hasMember(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            CmdSetGroupInfo cmd = new CmdSetGroupInfo(msg.getContent(groupData.osnKey));

            if (cmd.type != -1 ||
                    cmd.joinType != -1 ||
                    cmd.passType != -1 ||
                    cmd.attribute != null){
                /**owner 才能改**/
                if (!groupData.owner.equalsIgnoreCase(msg.from)){
                    sendReplyCode(runnerData, E_noRight, null);
                    return null;
                }
            }
            boolean needNotice = false;
            if (cmd.name != null || cmd.portrait != null){
                /**
                 * GroupType_Free 所有人都可以改
                 * GroupType_Restrict 管理员才能改
                 * **/
                needNotice = true;
                if (groupData.type == GroupType_Restrict){
                    // 管理员才能改
                    if (!groupData.isAdmin(msg.from)){
                        sendReplyCode(runnerData, E_noRight, null);
                        return null;
                    }
                }
            }

            List<String> infoList = new ArrayList<>();
            // 更新数据库
            GroupNoticeData notice = new GroupNoticeData();
            JSONObject noticeData = new JSONObject();
            if (cmd.type != -1){
                infoList.add("type");
                groupData.type = cmd.type;
                if (!db.group.setType(groupData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            if (cmd.joinType != -1){
                infoList.add("joinType");
                groupData.joinType = cmd.joinType;
                if (!db.group.setJoinType(groupData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            if (cmd.passType != -1){
                infoList.add("passType");
                groupData.passType = cmd.passType;
                if (!db.group.setPassType(groupData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            if (cmd.name != null){
                infoList.add("name");
                notice.name = cmd.name;
                noticeData.put("name", cmd.name);
                groupData.name = cmd.name;
                if (!db.group.setName(groupData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            if (cmd.portrait != null){
                infoList.add("portrait");
                notice.portrait = cmd.portrait;
                noticeData.put("portrait", cmd.portrait);
                groupData.portrait = cmd.portrait;
                if (!db.group.setPortrait(groupData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            if (cmd.attribute != null){
                infoList.add("attribute");
                needNotice = true;
                notice.attribute = cmd.attribute;
                noticeData.put("attribute", cmd.attribute);
                //groupData.attribute = cmd.attribute;
                if (!db.group.setAttribute(groupData, cmd.attribute)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
            }
            sendReplyCode(runnerData, null, null);

            if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                GroupNoticeServer.push(notice);
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void UpAttribute(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);


            boolean authority = false;

            if (groupData.isAdmin(msg.from)) {
                authority = true;
            } else if (groupData.isOwner2(msg.from)) {
                authority = true;
            }

            if (!authority) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not admin, from : " + msg.from);
                return null;
            }




            /*if (!groupData.isAdmin(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not admin");
                return null;
            }*/



            CmdUpAttribute cmd = new CmdUpAttribute(msg.getContent(groupData.osnKey));
            log.info("COMMAND : " + cmd.key + "  " + cmd.value);



            if (groupData.attribute != null) {
                JSONObject attrJson = JSONObject.parseObject(groupData.attribute);
                attrJson.remove(cmd.key);
                attrJson.put(cmd.key, cmd.value);
                groupData.attribute = attrJson.toString();
            } else {
                JSONObject attrJson = new JSONObject();
                attrJson.put(cmd.key, cmd.value);
                groupData.attribute = attrJson.toString();
            }



            //log.info("2");

            boolean needNotice = true;


            List<String> infoList = new ArrayList<>();
            // 更新数据库
            GroupNoticeData notice = new GroupNoticeData();
            JSONObject noticeData = new JSONObject();



            if (groupData.isOwner2(msg.from)) {

                log.info("notice owner2.");

                JSONObject data = new JSONObject();
                data.put("groupID", msg.to);
                data.put("id", cmd.id);
                data.put("result", "success");

                JSONObject msgJson = makeMessage("Message", groupData.osnID, msg.from, data, groupData.osnKey, null);
                sendMessageNoSave(msg.from, msgJson);
            }

            if (groupData.attribute != null){
                infoList.add("attribute");
                needNotice = true;
                notice.attribute = groupData.attribute;
                noticeData.put("attribute", groupData.attribute);
                //groupData.attribute = cmd.attribute;

                log.info("up db group attribute : " + groupData.attribute);
                if (!db.group.setAttribute(groupData, groupData.attribute)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    log.info("update db error.");
                    return null;
                }
            }

            log.info("group attribute : " + groupData.attribute);

            //sendReplyCode(runnerData, null, null);

            if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                //log.info("notice : " + noticeData.toString());
                GroupNoticeServer.push(notice);
            }

            sendReplyCode(runnerData, null, null);
            return null;

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
            sendReplyCode(runnerData, E_exception, null);
        }
        return null;
    }

    public static Void RemoveAttribute(RunnerData runnerData) {
        try {
            //log.info("begin.");
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            //log.info("user : " + msg.from);
            //log.info("group : " + msg.to);
            if (!groupData.isAdmin(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not admin");
                return null;
            }

            //log.info("1");

            CmdRemoveAttribute cmd = new CmdRemoveAttribute(msg.getContent(groupData.osnKey));

            //log.info("1-1");

            if (groupData.attribute == null) {
                sendReplyCode(runnerData, null, null);
                return null;
            }

            JSONObject attrJson = JSONObject.parseObject(groupData.attribute);
            for (String key : cmd.keys) {
                attrJson.remove(key);
            }
            String newAttr = attrJson.toString();

            if (groupData.attribute.equalsIgnoreCase(newAttr)){
                sendReplyCode(runnerData, null, null);
                return null;
            }

            groupData.attribute = newAttr;



            //log.info("2");

            boolean needNotice = true;


            List<String> infoList = new ArrayList<>();
            // 更新数据库
            GroupNoticeData notice = new GroupNoticeData();
            JSONObject noticeData = new JSONObject();

            //log.info("3");

            if (groupData.attribute != null){
                infoList.add("attribute");
                needNotice = true;
                notice.attribute = groupData.attribute;
                noticeData.put("attribute", groupData.attribute);
                //groupData.attribute = cmd.attribute;
                if (!db.group.setAttribute(groupData, groupData.attribute)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    log.info("update db error.");
                    return null;
                }
            }

            //log.info("group attribute : " + groupData.attribute);

            sendReplyCode(runnerData, null, null);

            if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                //log.info("notice : " + noticeData.toString());
                GroupNoticeServer.push(notice);
            }

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }

    public static Void UpDescribe(RunnerData runnerData) {
        try {
            //log.info("begin.");
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            log.info("user : " + msg.from);
            log.info("group : " + msg.to);
            if (!groupData.isOwner(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not owner");
                return null;
            }

            //log.info("1");

            CmdUpAttribute cmd = new CmdUpAttribute(msg.getContent(groupData.osnKey));

            //log.info("1-1");

            if (groupData.describe != null) {
                JSONObject attrJson = JSONObject.parseObject(groupData.describe);
                attrJson.remove(cmd.key);
                attrJson.put(cmd.key, cmd.value);
                groupData.describe = attrJson.toString();
            } else {
                JSONObject attrJson = new JSONObject();
                attrJson.put(cmd.key, cmd.value);
                groupData.describe = attrJson.toString();
            }



            //log.info("2");

            boolean needNotice = true;


            List<String> infoList = new ArrayList<>();
            // 更新数据库
            //GroupNoticeData notice = new GroupNoticeData();
            //JSONObject noticeData = new JSONObject();

            //log.info("3");

            if (groupData.describe != null){
                infoList.add("describe");
                needNotice = true;
                //notice.attribute = groupData.attribute;
                //noticeData.put("attribute", groupData.attribute);
                //groupData.attribute = cmd.attribute;
                if (!db.group.setDescribe(groupData, groupData.describe)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    log.info("update db error.");
                    return null;
                }
            }

            //log.info("group attribute : " + groupData.attribute);

            sendReplyCode(runnerData, null, null);

            /*if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                //log.info("notice : " + noticeData.toString());
                GroupNoticeServer.push(notice);
            }*/

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }

    public static Void RemoveDescribe(RunnerData runnerData) {
        try {
            //log.info("begin.");
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            //log.info("user : " + msg.from);
            //log.info("group : " + msg.to);
            if (!groupData.isAdmin(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not admin");
                return null;
            }

            //log.info("1");

            CmdRemoveAttribute cmd = new CmdRemoveAttribute(msg.getContent(groupData.osnKey));

            //log.info("1-1");

            if (groupData.describe == null) {
                sendReplyCode(runnerData, null, null);
                return null;
            }

            JSONObject attrJson = JSONObject.parseObject(groupData.describe);
            for (String key : cmd.keys) {
                attrJson.remove(key);
            }
            String newAttr = attrJson.toString();

            if (groupData.describe.equalsIgnoreCase(newAttr)){
                sendReplyCode(runnerData, null, null);
                return null;
            }

            groupData.describe = newAttr;



            //log.info("2");

            boolean needNotice = true;


            List<String> infoList = new ArrayList<>();
            // 更新数据库
            //GroupNoticeData notice = new GroupNoticeData();
            //JSONObject noticeData = new JSONObject();

            //log.info("3");

            if (groupData.describe != null){
                infoList.add("describe");
                //needNotice = true;
                //notice.describe = groupData.describe;
                //noticeData.put("describe", groupData.describe);
                //groupData.attribute = cmd.attribute;
                if (!db.group.setDescribe(groupData, groupData.describe)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    log.info("update db error.");
                    return null;
                }
            }

            //log.info("group attribute : " + groupData.attribute);

            sendReplyCode(runnerData, null, null);

            /*if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                //log.info("notice : " + noticeData.toString());
                GroupNoticeServer.push(notice);
            }*/

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }

    public static Void UpPrivateInfo(RunnerData runnerData) {
        try {
            //log.info("begin.");
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            log.info("user : " + msg.from);
            log.info("group : " + msg.to);
            if (!groupData.isOwner(msg.from)) {
                sendReplyCode(runnerData, E_noRight, null);
                log.info("not owner");
                return null;
            }

            //log.info("1");

            CmdUpAttribute cmd = new CmdUpAttribute(msg.getContent(groupData.osnKey));

            //log.info("1-1");

            if (groupData.privateInfo != null) {
                JSONObject attrJson = JSONObject.parseObject(groupData.privateInfo);
                attrJson.remove(cmd.key);
                attrJson.put(cmd.key, cmd.value);
                groupData.privateInfo = attrJson.toString();
            } else {
                JSONObject attrJson = new JSONObject();
                attrJson.put(cmd.key, cmd.value);
                groupData.privateInfo = attrJson.toString();
            }



            //log.info("2");

            boolean needNotice = true;


            List<String> infoList = new ArrayList<>();
            // 更新数据库
            //GroupNoticeData notice = new GroupNoticeData();
            //JSONObject noticeData = new JSONObject();

            //log.info("3");

            if (groupData.privateInfo != null){
                infoList.add("privateInfo");
                needNotice = true;
                //notice.attribute = groupData.attribute;
                //noticeData.put("attribute", groupData.attribute);
                //groupData.attribute = cmd.attribute;
                if (!db.group.setPrivateInfo(groupData, groupData.privateInfo)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    log.info("update db error.");
                    return null;
                }
            }

            //log.info("group attribute : " + groupData.attribute);

            sendReplyCode(runnerData, null, null);

            /*if (needNotice){
                notice.groupID = msg.to;
                notice.state = "UpdateGroup";
                notice.infoList = infoList;
                noticeData.put("groupID", msg.to);
                noticeData.put("state", "UpdateGroup");
                noticeData.put("infoList", infoList);
                //systemNotifyGroup(groupData, noticeData);
                //log.info("notice : " + noticeData.toString());
                GroupNoticeServer.push(notice);
            }*/

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }











    /**
     *  以下是未测试的命令
     * */
    public static Void GetGroupInfo(RunnerData runnerData) {
        try {
            JSONObject json = runnerData.json;
            String userID = json.getString("from");
            String groupID = json.getString("to");

            GroupData groupData = getGroupData(groupID);

            // 是否需要权限
            JSONObject data;
            if (groupData.hasMember(userID)){

                data = groupData.toPrivateJson(userID);
                //log.info("[GetGroupInfo] member : " + data);

            } else {
                data = groupData.toPublicJson();
                //log.info("[GetGroupInfo] not member : " + data);
            }


            JSONObject result = wrapMessage("GroupInfo", groupID, userID, data, groupData.osnKey, json);
            log.info("Group info to : " + userID);

            UserData ud = getUserData(userID);
            if (ud == null){
                sendOsxMessageNoSave(result);
                return null;
            }
            sendMessageNoSave(userID, result);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public static Void SystemNotify(RunnerData runnerData){
        return SystemNotifyGroupByOwner2(runnerData);
    }

    public static Void SetAdmin(RunnerData runnerData){

        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                sendReplyCode(runnerData, E_groupNoFind, null);
                return null;
            }

            // 判断是否是群主
            if (!groupData.isOwner(msg.from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            JSONObject content = msg.getContent(groupData.osnKey);

            CmdSetAdmin cmd = new CmdSetAdmin(content);
            // 判断是否是成员
            MemberData memberData = groupData.getMember(cmd.osnID);
            if (memberData == null){
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }
            if (memberData.type == 0){
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }

            if (cmd.type == memberData.type){
                sendReplyCode(runnerData, null, null);
                return null;
            }

            if (cmd.type == MemberType_Normal || cmd.type == MemberType_Admin){
                //  更新
                if (!db.groupMember.setMemberType(memberData)){
                    sendReplyCode(runnerData, E_dataBase, null);
                    return null;
                }
                memberData.type = cmd.type;

                // 发送通知
                GroupNoticeData notice = new GroupNoticeData();
                notice.groupID = groupData.osnID;
                //notice.muteRange = "all";
                //notice.mute = cmd.mute;
                if (cmd.type == MemberType_Admin){
                    notice.state = "AddAdmin";
                }else {
                    notice.state = "DelAdmin";
                }
                notice.addUser(cmd.osnID);
                //notice.text = cmd.osnID;

                //systemNotifyGroup(groupData, notice.toJson());
                GroupNoticeServer.push(notice);


            }


        } catch (Exception e){
            log.error("", e);
            log.error("error json: " + runnerData.json);
        }

        return null;
    }

    public static Void AddAdmin(RunnerData runnerData){

        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                sendReplyCode(runnerData, E_groupNoFind, null);
                return null;
            }

            // 判断是否是群主
            if (!groupData.isOwner(msg.from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            CmdAddAdmin cmd = new CmdAddAdmin(msg.getContent(groupData.osnKey));

            if (cmd.adminList == null){
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }

            for (String member : cmd.adminList){
                addAdmin(groupData, member);
            }

            sendReplyCode(runnerData, null, null);


        } catch (Exception e){
            log.error("", e);
            log.error("error json: " + runnerData.json);
        }

        return null;
    }

    public static Void DelAdmin(RunnerData runnerData){

        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                sendReplyCode(runnerData, E_groupNoFind, null);
                return null;
            }

            // 判断是否是群主
            if (!groupData.isOwner(msg.from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            CmdDelAdmin cmd = new CmdDelAdmin(msg.getContent(groupData.osnKey));

            if (cmd.adminList == null){
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }

            for (String member : cmd.adminList){
                delAdmin(groupData, member);
            }

            sendReplyCode(runnerData, null, null);


        } catch (Exception e){
            log.error("", e);
            log.error("error json: " + runnerData.json);
        }

        return null;
    }


    public static Void SetMaxMember(RunnerData runnerData){
        try {

            //log.info("begin.");
            MsgData msg = new MsgData(runnerData.json);

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                log.info("no group");
                return null;
            }

            // 检查是否是owner2
            if (!groupData.owner2.equalsIgnoreCase(msg.from)) {
                log.info("not owner");
                return null;
            }

            JSONObject content = msg.getContent(groupData.osnKey);

            /**
             * max:
             * */
            int max = content.getIntValue("max");

            groupData.maxMember = max;
            List<String> keys = new ArrayList<>();
            keys.add("maxMember");

            db.group.update(groupData, keys);

        } catch (Exception e){
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }


    public static Void NewOwner(RunnerData runnerData){
        try {
            MsgData msg = new MsgData(runnerData.json);

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                return null;
            }

            // 判断是否是群主
            if (!groupData.isOwner(msg.from)){
                sendReplyCode(runnerData, E_notAdmin, null);
                return null;
            }

            CmdNewOwner cmd = new CmdNewOwner(msg.getContent(groupData.osnKey));

            if (cmd.osnID == null){
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }
            // 是否是成员
            MemberData memberData = groupData.getMember(cmd.osnID);
            if (memberData == null) {
                sendReplyCode(runnerData, E_notMember, null);
                return null;
            }

            groupData.owner = memberData.osnID;
            memberData.type = MemberType_Owner;

            if (!db.group.newOwner(groupData)){
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            if (!db.groupMember.setMemberType(memberData)){
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }


        } catch (Exception e){
            log.error("", e);
            log.error(runnerData.json.toString());
        }

        sendReplyCode(runnerData, null, null);
        return null;
    }

    public static Void AddMember(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                // 这里是不应该出现这个错误的
                sendReplyCode(runnerData, E_groupNoFind, null);
                return null;
            }

            if (groupData.isOwner2(msg.from)) {
                return AddMemberByOwner2(runnerData, msg, groupData);
            }

            // 验证权限
            if (!groupData.checkAddMemberRight(msg.from)){
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            CmdAddMember cmd = new CmdAddMember(msg.getContent(groupData.osnKey));

            // 加人进群
            if (cmd.memberList == null){
                sendReplyCode(runnerData, E_errorCmd, null);
                return null;
            }

            List<String> needShare = new ArrayList<>();

            for (String invitee : cmd.memberList){
                String needShareUser = addMemberOne(groupData, msg.from, invitee);
                if (needShareUser != null){
                    needShare.add(needShareUser);
                }
            }

            if (needShare.size() > 0){
                GroupNoticeData notice = new GroupNoticeData();
                notice.groupID = groupData.osnID;
                notice.state = "NeedShare";
                notice.userList = needShare;
                notifyOne(groupData, msg.from, notice.toJson());

                JSONObject result = new JSONObject();
                result.put("userList", needShare);
                sendReplyCode(runnerData, null, result);
                return null;
            }

            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
            log.error("error json : " + runnerData.json);
        }
        return null;
    }

    public static Void AddMemberByOwner2(RunnerData runnerData, MsgData msg, GroupData groupData) {

        try {

            CmdAddMember cmd = new CmdAddMember(msg.getContent(groupData.osnKey));

            if (cmd.memberList == null){
                //sendReplyCode(runnerData, E_errorCmd, null);
                return null;
            }

            for (String invitee : cmd.memberList){
                addMemberOne(groupData, msg.from, invitee);
            }

        } catch (Exception e) {

        }

        return null;
    }

    public static Void DelMember(RunnerData runnerData) {
        try {

            MsgData msg = new MsgData(runnerData.json);
            GroupData groupData = getGroupData(msg.to);

            if (!groupData.isAdmin(msg.from)){
                log.info("[DelMember] error : E_noRight");
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            CmdDelMember cmd = new CmdDelMember(msg.getContent(groupData.osnKey));
            log.info("[DelMember] members : " + cmd.memberList);
            // 过滤列表
            List<String> delList = new ArrayList<>();
            for (String user : cmd.memberList){
                if (groupData.hasMember(user)){
                    delList.add(user);
                }
            }

            if (delList.size() == 0) {
                log.info("[DelMember] error :  delList.size() == 0");
                sendReplyCode(runnerData, null, null);
                return null;
            }

            if (!db.groupMember.deleteMembers(groupData.osnID, delList)) {
                log.info("[DelMember] error :  E_db error");
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            groupData.delMembers(delList);

            sendReplyCode(runnerData, null, null);

            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = groupData.osnID;
            notice.state = "DelMember";
            notice.approver = msg.from;
            notice.userList = delList;
            //systemNotifyGroup(groupData, notice.toJson());
            GroupNoticeServer.push(notice);

            // 这里要全部通知了，才更新缓存
            //groupData.delMembers(delList);

            for (String delMember : delList) {
                notifyOne(groupData, delMember, notice.toJson());
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static Void EncData(RunnerData runnerData) {

        try {
            // json转message
            MsgData msg = new MsgData(runnerData.json);

            GroupData groupData = getGroupData(msg.to);

            // 判断是否是本群的成员或者是owner
            if (!(msg.from.equalsIgnoreCase(groupData.owner2)
                    || msg.from.equalsIgnoreCase(groupData.owner)
                    || groupData.hasMember(msg.from))) {
                return null;
            }

            JSONObject msgContent = msg.getContent(groupData.osnKey, groupData.aesKey);

            String command = msgContent.getString("command");

            if (command.equalsIgnoreCase("Recall")) {
                // TODO
                handleRecall(msgContent, groupData, msg, runnerData.json);
            } else if (command.equalsIgnoreCase("DeleteMessage")) {
                // TODO
                handleDeleteMessage(msgContent, groupData, msg, runnerData.json);
            } else if (command.equalsIgnoreCase("UpdateAttribute")) {
                // TODO
                handleUpdateAttribute(msgContent, groupData, msg, runnerData.json);
            }

        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }





    /**
     *  以下是未添加的命令
     * */






    private static void agreeMemberJoin(GroupData groupData, String user, String admin){
        // 检查invitee 是否在群中
        if (groupData.hasMember(user)){
            return;
        }

        // 检查群是否已经满员
        if (groupData.full()){
            return;
        }

        MemberData newMember = new MemberData(user, groupData.osnID, MemberType_Normal, null, groupData.aesKey);
        if (!db.groupMember.insert(newMember)){
            log.info("[agreeMemberJoin] insert member error.");
            return;
        }
        groupData.addMember(newMember);
        newlyGroup(groupData, user);



        // 通知到群
        // 通知
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddMember";
        notice.approver = admin;
        notice.addUser(user);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);

    }


    private static List<String> createMemberList(GroupData groupData, List<String> userList){

        List<String> needShare = new ArrayList<>();

        for (String member : userList) {

            UserData udMember = getUserData(member);
            if (udMember == null){
                needShare.add(member);
                continue;
            }
            if (!udMember.isFriend(groupData.owner)){
                needShare.add(member);
                continue;
            }

            MemberData memberData = new MemberData(
                    member,
                    groupData.osnID,
                    MemberType_Normal,
                    groupData.owner,
                    groupData.aesKey);
            if (groupData.owner.equalsIgnoreCase(memberData.osnID)) {
                memberData.type = MemberType_Owner;
            }
            groupData.addMember(memberData);
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
        return needShare;

    }

    private static Void forwardToAdmin(RunnerData runnerData,CmdJoinGroup cmd, GroupData groupData){

        // 列出admin
        List<String> admins = groupData.listAdmin();
        for (String admin : admins){
            JSONObject result = cmd.makeMessage(groupData.osnID, admin, groupData.osnKey, null);
            if (result == null){
                log.info("[forwardToAdmin] makeMessage error; ");
            }
            sendMessage(admin, result);

            UserData userData = getUserData(admin);
            if (userData != null) {
                if (userData.owner2 != null) {
                    if (!userData.owner2.equalsIgnoreCase("")) {

                        /*PushData pushData = new PushData();
                        pushData.time = System.currentTimeMillis();
                        pushData.owner2 = userData.owner2;
                        pushData.user = userData.osnID;
                        pushData.type = "JoinGroup";
                        pushData.msgHash = result.getString("hash");

                        PushServer.push(pushData);*/
                    }
                }

            }

        }
        sendReplyCode(runnerData, null, null);
        return null;
    }

    private static Void joinGroupByJoinType(RunnerData runnerData, GroupData groupData, MsgData msg) {
        String joinType = groupData.getJoinType();
        if (joinType.equalsIgnoreCase("password")) {
            return joinGroupByPassword(runnerData, groupData, msg);
        } else if (joinType.equalsIgnoreCase("licence")){

        }
        return null;
    }

    private static Void joinGroupByPassword(RunnerData runnerData, GroupData groupData, MsgData msg) {
        String joinPwd = groupData.getPrivateInfo("joinPwd");
        if (joinPwd == null) {
            sendReplyCode(runnerData, E_dataBase, null);
            return null;
        }

        try {
            CmdJoinGroup cmd = new CmdJoinGroup(msg.getContent(groupData.osnKey));
            if (cmd.reason.equalsIgnoreCase(joinPwd)) {
                return joinGroupMyself(runnerData, groupData, msg.from);
            }

        } catch (Exception e) {
            sendReplyCode(runnerData, E_dataBase, null);
            log.info("");
        }
        return null;
    }

    private static Void joinGroupMyself(RunnerData runnerData, GroupData groupData, String user){


        //
        MemberData newMember = new MemberData(user, groupData.osnID, MemberType_Normal, null, groupData.aesKey);
        if (!db.groupMember.insert(newMember)){
            sendReplyCode(runnerData, E_dataBase, null);
            return null;
        }
        groupData.addMember(newMember);
        newlyGroup(groupData, user);

        // 通知到群
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddMember";
        notice.addUser(user);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);
        sendReplyCode(runnerData, null, null);
        return null;
    }

    private static void addAdmin(GroupData groupData, String user){

        // 检查是否已经是管理员
        if (groupData.isAdmin(user)){
            return;
        }

        // 检查是否是member
        MemberData memberData = groupData.getMember(user);
        if (memberData == null){
            return;
        }

        if (memberData.type != MemberType_Normal){
            return;
        }


        groupData.changeMemberType(memberData, MemberType_Admin);
        //memberData.type = MemberType_Admin;

        if (!db.groupMember.setMemberType(memberData)){
            log.info("[addAdmin] db error");
            return;
        }

        // 通知
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddAdmin";
        notice.addUser(user);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);

    }

    private static void delAdmin(GroupData groupData, String user){

        // 不是管理员直接返回
        if (!groupData.isAdmin(user)){
            return;
        }

        // 检查是否是member
        MemberData memberData = groupData.getMember(user);
        if (memberData == null){
            return;
        }

        if (memberData.type != MemberType_Admin){
            return;
        }

        groupData.changeMemberType(memberData, MemberType_Normal);
        //memberData.type = MemberType_Normal;

        if (!db.groupMember.setMemberType(memberData)){
            log.info("[addAdmin] db error");
            return;
        }

        // 通知
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "DelAdmin";
        notice.addUser(user);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);

    }

    private static String addMemberOne(GroupData groupData, String from, String invitee){
        // 检查invitee 是否在群众
        if (groupData.hasMember(invitee)){
            return null;
        }
        // 检查群是否已经满员
        if (groupData.full()){
            return null;
        }

        // 检查invitee 的好友里是否有from
        /*UserData udInvitee = getUserData(invitee);
        if (udInvitee == null){
            return invitee;
        }*/

        /*if (!udInvitee.isFriend(from)){
            return invitee;
        }*/

        // 是好友关系

        // 直接添加
        MemberData newMember = new MemberData(invitee, groupData.osnID, MemberType_Normal, from, groupData.aesKey);
        if (!db.groupMember.insert(newMember)){
            log.info("[addMemberOne] error : insert member error.");
            return null;
        }
        groupData.addMember(newMember);
        newlyGroup(groupData, invitee);

        // 通知到群
        // 通知
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddMember";
        notice.invitor = from;
        notice.addUser(invitee);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);

        return null;
    }

    private static ErrorData joinGroupNoInvitation(CmdJoinGrp cmd, GroupData groupData, String userID){
        try {

            if (groupData.joinType == GroupJoinType_Free) {
                // GroupJoinType_Free 自由加群
                return newlyMember(groupData, userID);
            } else {
                // 转发给管理员
                List<MemberData> adminList = db.groupMember.listAdmin(groupData.osnID);
                for (MemberData md : adminList){

                    // 以通知的形式发送给管理员
                    /*GroupNoticeData notice = new GroupNoticeData();
                    notice.groupID = groupData.osnID;
                    notice.state = "JoinGroup";
                    notice.addUser(userID);
                    notice.text = cmd.text;
                    notifyOne(groupData, md.osnID, notice.toJson());*/

                    JSONObject data = new JSONObject();
                    data.put("originalUser", null);
                    data.put("reason", cmd.reason);
                    data.put("userID", userID);
                    JSONObject msg = makeMessage(
                            "JoinGroup",
                            groupData.osnID,
                            md.osnID,
                            data,
                            groupData.osnKey,
                            null);
                    sendMessage(md.osnID, msg);

                }
                return null;
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static ErrorData joinGroupWithInvitation(CmdJoinGrp cmd, GroupData groupData){
        // 验证 邀请函
        if (!cmd.letter.verify()){
            return E_InvalidInvitation;
        }

        if (!groupData.hasMember(cmd.letter.from)){
            // 用户已经存在了
            return E_InvalidInvitation;
        }

        // 判断邀请人的权限
        if (groupData.joinType == GroupJoinType_Admin){
            if (!groupData.isAdmin(cmd.letter.from)){
                return E_InvalidInvitation;
            }
        }

        // 添加邀请人
        MemberData newMember = new MemberData(
                cmd.letter.to,
                groupData.osnID,
                MemberType_Normal,
                cmd.letter.from,
                groupData.aesKey);
        if (!db.groupMember.insert(newMember)){
            log.info("[joinGroupWithInvitation] insert member error.");
            return E_dataBase;
        }
        groupData.addMember(newMember);
        newlyGroup(groupData, cmd.letter.to);

        // 通知到群
        // 通知
        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddMember";
        notice.invitor = cmd.letter.from;
        notice.addUser(cmd.letter.to);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);


        return null;
    }

    private static String createInvitation(GroupData groupData, InvitationLetter letter){

        //InvitationLetter letter = new InvitationLetter();
        if (!letter.create(groupData)){
            return null;
        }

        return letter.toString();
    }

    private static List<String> createInvitations(GroupData groupData, List<InvitationLetter> letters){
        List<String> invitationLetters = new ArrayList<>();
        // 生成邀请函
        for (InvitationLetter letter : letters){
            String invitationLetter = createInvitation(groupData, letter);
            if (invitationLetter != null){

                invitationLetters.add(invitationLetter);

                log.info("[createInvitations] invite : "+letter.to);
                // 转发给受邀者
                JSONObject content = new JSONObject();
                content.put("invitation", invitationLetter);
                JSONObject reMessage = makeMessage(
                        "Invitation",
                        groupData.osnID,
                        letter.to,      // 受邀者
                        content,
                        groupData.osnKey,
                        null);
                sendMessage(letter.to, reMessage);

            }
        }
        if (invitationLetters.size() == 0){
            return null;
        }
        return invitationLetters;
    }

    private static void setMemberMute(GroupData groupData, String member, int mute){
        MemberData md = groupData.members.get(member);
        if (md == null){
            log.info("[setMemberMute] no member");
            return;
        }
        if (mute != 0) {
            if (groupData.isAdmin(member)){
                // 不能禁言管理员
                return;
            }
        }

        md.mute = mute;
        // 存入数据库
        List<String> keys = new ArrayList<>();
        keys.add("mute");
        if (!db.groupMember.update(md, keys)){
            log.info("[setMemberMute] error. update member");
        }
    }

    public static void systemNotifyGroup(GroupData groupData, JSONObject data){
        try {
            List<JSONObject> noticesIn = new ArrayList<>();
            List<JSONObject> noticesOut = new ArrayList<>();

            for (MemberData md : groupData.members.values()){
                JSONObject json;
                if (md.receiverKey == null){
                    json = makeMessage("GroupUpdate", groupData.osnID, md.osnID, data, groupData.osnKey, null);
                }
                else if (md.receiverKey.equalsIgnoreCase("")){
                    json = makeMessage("GroupUpdate", groupData.osnID, md.osnID, data, groupData.osnKey, null);
                }
                else {
                    json = packGroupNotice(data,"GroupUpdate", groupData, md,null);
                }
                // 系统通知都是需要需要存的
                // 统一存储
                if (isInThisNode(md.osnID)){
                    noticesIn.add(json);
                } else {
                    noticesOut.add(json);
                }
                //sendMessage(md.osnID, json);
            }

            // 集体存，减轻redis的压力
            if (noticesIn.size() > 0){
                MessageSaveServer.push(new MessageSaveData(noticesIn, MQCode));
                /*if (!db.message.insertMessagesInRedis(noticesIn, MQCode)){
                    log.info("[GroupService::forwardMessage] error : insertMessagesInRedis");
                }*/
                // 需要先存，再发
                for (JSONObject message : noticesIn){

                    String to = message.getString("to");
                    UserData userData = IMData.getUserData(to);
                    if (userData != null){
                        sendClientMessageNoSave(userData, message);
                    }
                }
            }

            // 以下是外发数据
            if (noticesOut.size() > 0){
                MessageSaveServer.push(new MessageSaveData(noticesOut, MQOSX));
                /*if (!db.message.insertMessagesInRedis(noticesOut, MQOSX)){
                    log.info("[GroupService::forwardMessage] error : insertMessagesInRedis");
                }*/
                // 需要先存，再发
                for (JSONObject message : noticesOut){
                    sendOsxMessageNoSave(message);
                }
            }




        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static void notifyOne(GroupData groupData, String member, JSONObject data){
        try {
            //log.info("[notifyOne] member : " + member);
            MemberData md = groupData.getMember(member);

            JSONObject message;
            if (md == null) {
                //log.info("[notifyOne] info : " + data);
                message = makeMessage("GroupUpdate", groupData.osnID, member, data, groupData.osnKey, null);
                //log.info("[notifyOne] message : " + message);
            } else if (md.receiverKey == null){
                message = makeMessage("GroupUpdate", groupData.osnID, md.osnID, data, groupData.osnKey, null);
            }
            else if (md.receiverKey.equalsIgnoreCase("")){
                message = makeMessage("GroupUpdate", groupData.osnID, md.osnID, data, groupData.osnKey, null);
            }
            else {
                message = packGroupNotice(data,"GroupUpdate", groupData, md,null);
            }

            UserData userData = getUserData(member);
            if (userData != null){
                sendClientMessage(userData, message);
            } else {
                // 外发
                sendOsxMessage(message);
            }




        } catch (Exception e) {
            log.error("", e);
        }
    }



    private static ErrorData newlyMember(GroupData groupData, String userID) {

        if (groupData.hasMember(userID)){
            // 用户已经存在了
            return E_userExist;
        }

        MemberData memberData = getMemberData(groupData.osnID, userID);
        if (memberData == null) {
            // 判断群是否已经满了
            if (groupData.members.size() >= groupData.maxMember){
                // 群满了
                return E_groupFull;
            }

            memberData = new MemberData(userID, groupData.osnID, MemberType_Normal,"",groupData.aesKey);
            if (!db.groupMember.insert(memberData)){
                log.info("[newlyMember] error insert");
                return E_dataBase;
            }
        } else {
            // 更新
            memberData.type = MemberType_Normal;
            if (!db.groupMember.update(memberData, Collections.singletonList("type"))){
                log.info("[newlyMember] error updateMember error");
                return E_dataBase;
            }
        }

        groupData.addMember(memberData);
        newlyGroup(groupData, userID);

        UserData userData = getUserData(userID);
        if (userData != null)
            userData.addGroup(groupData.osnID);

        GroupNoticeData notice = new GroupNoticeData();
        notice.groupID = groupData.osnID;
        notice.state = "AddMember";
        notice.addUser(userID);
        //systemNotifyGroup(groupData, notice.toJson());
        GroupNoticeServer.push(notice);

        return null;
    }

    public static void newlyGroup(GroupData groupData, String userID) {
        JSONObject data = groupData.toPublicJson();
        data.put("state", "NewlyGroup");
        JSONObject json = makeMessage("GroupUpdate", groupData.osnID, userID, data, groupData.osnKey, null);
        log.info("userID: " + userID + ", groupID: " + groupData.osnID);
        sendMessage(userID, json);

        /**
         * 根据GetHistory字段来确定是否拉消息给用户
         * 1. 查询history的最后30条数据
         * 2. pack message
         * 3. 发送给用户
         * **/
        String attrGetHistory = groupData.getAttribute("GetHistory");
        if (attrGetHistory != null) {
            if (attrGetHistory.equalsIgnoreCase("yes")) {
                List<GroupHistoryMessageData> msgList = db.groupHistory.listDesc(groupData.osnID);
                for (GroupHistoryMessageData msg : msgList) {

                    MemberData md = groupData.getMember(userID);

                    JSONObject msgJson = JSONObject.parseObject(msg.msg);
                    JSONObject pack = null;
                    if (md.receiverKey == null){
                        pack = packMessage(msgJson, groupData.osnID, md.osnID, groupData.osnKey);
                    }
                    else if (md.receiverKey.equalsIgnoreCase("")){
                        pack = packMessage(msgJson, groupData.osnID, md.osnID, groupData.osnKey);
                    }
                    else {
                        pack = packGroupMessage(msgJson, groupData.osnID, md, groupData.osnKey, msg.content);
                    }
                    if (pack != null) {
                        sendMessage(userID, pack);
                    }
                }
            }
        }
    }

    private static Void SystemNotifyGroupByOwner2(RunnerData runnerData){
        try{
            log.info("[SystemNotifyGroupByOwner2]");
            MsgData msg = new MsgData(runnerData.json);

            if (msg.to == null){
                log.info("[SystemNotifyGroupByOwner2] error: to is null. ");
                return null;
            }

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null){
                log.info("[SystemNotifyGroupByOwner2] error: no group " + msg.to);
                return null;
            }

            if (!msg.from.equalsIgnoreCase(groupData.owner2)){
                // 不是我的所有者，不能发送通知
                log.info("[SystemNotifyGroupByOwner2] error: not owner2");
                log.info("[SystemNotifyGroupByOwner2] error: group owner2 : " +groupData.owner2);
                log.info("[SystemNotifyGroupByOwner2] error: from : " +msg.from);
                return null;
            }

            JSONObject contentJson = msg.getContent();
            GroupNoticeData notice = new GroupNoticeData(contentJson);

            log.info("[SystemNotifyGroupByOwner2] notify...");
            log.info(contentJson.toString());
            log.info(notice.toJson().toString());
            GroupNoticeServer.push(notice);
            //systemNotifyGroup(groupData, contentJson);
        } catch (Exception e){
            log.info(e.getMessage());
            log.info("[SystemNotifyGroupByOwner2] error json : " + runnerData.json);
        }


        return null;
    }


    private static Void QuitGroup(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);

            String userID = msg.from;//json.getString("from");
            String groupID = msg.to;//json.getString("to");


            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                return null;
            }

            if (!groupData.hasMember(userID)) {
                log.info("[QuitGroup] not member: " + userID);
                sendReplyCode(runnerData, null, null);
                return null;
            }
            if (!db.groupMember.delete(userID, groupID)) {
                sendReplyCode(runnerData, null, null);
                return null;
            }
            sendReplyCode(runnerData, null, null);

            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = groupData.osnID;
            notice.state = "QuitGroup";
            notice.addUser(userID);
            GroupNoticeServer.push(notice);
            //systemNotifyGroup(groupData, notice.toJson());

            /*JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            data.put("state", "QuitGroup");
            data.put("userList", Collections.singletonList(userID));
            systemNotifyGroup(groupData, data);*/

            groupData.delMember(userID);




        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private static Void DelGroup(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);


            String userID = msg.from;//json.getString("from");
            String groupID = msg.to;//json.getString("to");

            //log.info("userID: " + userID + ", groupID: " + groupID);

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                return null;
            }

            if (!userID.equalsIgnoreCase(groupData.owner)) {
                log.info("[DelGroup] not owner");
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            if (!db.group.delete(groupData) || !db.groupMember.deleteMembers(groupID, new ArrayList<>(groupData.members.keySet()))) {
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }

            sendReplyCode(runnerData, null, null);

            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = groupData.osnID;
            notice.state = "DelGroup";
            GroupNoticeServer.push(notice);
            //systemNotifyGroup(groupData, notice.toJson());

            /*JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            data.put("state", "DelGroup");
            systemNotifyGroup(groupData, data);*/

            groupMap.remove(groupID);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static Void GetMemberInfo(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);

            String userID = msg.from;
            String groupID = msg.to;

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                //
                return null;
            }

            if (!groupData.hasMember(userID)) {
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            List<MemberData> memberList = db.groupMember.list(groupID);
            List<MemberInfo> members = new ArrayList<>();
            for (MemberData md : memberList) {
                members.add(md.getMemberInfo());
            }

            log.info("[GetMemberInfo] member count : " + groupData.members.size());

            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            data.put("memberCount", groupData.members.size());
            data.put("name", groupData.name);
            data.put("userList", members);


            JSONObject result = makeMessage("MemberInfo", groupID, userID, data, groupData.osnKey, runnerData.json);
            sendMessageNoSave(userID, result);



        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private static Void GetMemberZone(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);

            String userID = msg.from;
            String groupID = msg.to;

            GroupData groupData = getGroupData(msg.to);
            if (groupData == null) {
                //
                log.info("[GetMemberZone] error, groupData == null");
                return null;
            }

            if (!groupData.hasMember(userID)) {
                log.info("[GetMemberZone] error, not member");
                sendReplyCode(runnerData, E_noRight, null);
                return null;
            }

            // log.info("1");

            CmdGetMemberZone cmd = new CmdGetMemberZone(msg.getContent(groupData.osnKey));

            //log.info("begin : " + cmd.begin);
            //log.info("size : " + cmd.size);
            log.info("[GetMemberZone] group:" +groupData.name+
                    "  user:"+msg.from);
            log.info("[GetMemberZone] begin(" + cmd.begin +
                    ") size(" + cmd.size +
                    ")");

            if (cmd.begin != 0) {
                if (cmd.begin < 25) {
                    log.info("[GetMemberZone] input format error.");
                    return null;
                }
            }


            List<MemberInfo> members = groupData.getMemberZone(cmd.begin, cmd.size);


            log.info("[GetMemberZone] member count : " + members.size());

            JSONObject data = new JSONObject();
            data.put("groupID", groupData.osnID);
            data.put("memberCount", groupData.members.size());
            data.put("size", members.size());
            data.put("name", groupData.name);
            data.put("userList", members);


            JSONObject result = makeMessage("MemberInfo", groupData.osnID, msg.from, data, groupData.osnKey, runnerData.json);
            sendMessageNoSave(userID, result);

            log.info("[GetMemberZone] end.");


        } catch (Exception e) {
            log.error("", e);
            log.info("error json : " + runnerData.json);
        }
        return null;
    }

    private static Void ReGroupChange(RunnerData runnerData) {
        try {
            MsgData msg = new MsgData(runnerData.json);
            GroupData group = getGroupData(msg.to);
            if (group.type != 99) {
                return null;
            }
            if (!group.hasMember(msg.from)){
                return null;
            }
            db.groupMember.delete(msg.from, msg.to);
            group.delMember(msg.from);

            if (group.members.size() == 0){
                db.group.delete(group);
            }

        } catch (Exception e) {
            log.error("", e);
            log.info("[ReGroupChange] error json : " + runnerData.json);
        }
        return null;
    }



    private static Void QuitGroup(RunnerData runnerData, MsgData msg) {
        try {
            UserData userData = getUserData(msg.from);
            db.groupMember.delete(msg.from, msg.to);
            userData.delGroup(msg.to);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private static Void DelGroup(RunnerData runnerData, MsgData msg) {
        try {
            UserData userData = getUserData(msg.from);
            db.groupMember.delete(msg.from, msg.to);
            userData.delGroup(msg.to);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


}
