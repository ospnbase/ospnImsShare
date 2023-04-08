package com.ospn.service;

import com.alibaba.fastjson.JSONObject;
import com.ospn.command.*;
import com.ospn.command2.*;
import com.ospn.data.*;
import com.ospn.server.GroupNoticeServer;
import com.ospn.utils.CryptUtils;
import lombok.extern.slf4j.Slf4j;


import java.util.ArrayList;
import java.util.List;

import static com.ospn.OsnIMServer.*;
import static com.ospn.core.IMData.*;
import static com.ospn.core.IMData.getUserData;
import static com.ospn.data.Constant.*;
import static com.ospn.data.GroupData.GroupType_CrossGroup;
import static com.ospn.data.MemberData.MemberType_Owner;
import static com.ospn.service.GroupService.*;
import static com.ospn.service.MessageService.*;

@Slf4j
public class ManagerService {

    public static Void handleMessage(RunnerData runnerData){

        //log.info("[ManagerService] json : " + runnerData.json);

        MsgData msg = new MsgData(runnerData.json);
        if (msg.from == null){
            return null;
        }

        /*if (!msg.from.equalsIgnoreCase(manageID)){
            return null;
        }*/

        // 消息来自管理账户

        switch(msg.command){

            case "Message":
                return Message(runnerData);
            case "UserReg":
                return UserReg(runnerData);
            case "InitMember":
                return InitMember(runnerData);

            default:
                log.info("[ManagerService::HandleMessage] error command : " + msg.command);
                break;
        }

        return null;
    }


    public static Void Message(RunnerData runnerData) {
        /**
         * Message 中包含了其他指令，以防止在公网上泄露操作
         * **/
        try {

            MsgData msg = new MsgData(runnerData.json);

            JSONObject content = msg.getContent(service.osnKey);

            String command = content.getString("command");

            switch (command){
                case "Approver":
                    Approver(msg, runnerData.json, content);
                    break;
                case "GroupReg":
                    GroupReg(msg, runnerData.json, content);
                    break;
                case "GroupChange":
                    GroupChange(msg, runnerData.json, content);
                    break;
                case "UserUnreg":
                    UserUnreg(msg, runnerData.json, content);
                    break;
                case "GroupUnreg":
                    GroupUnreg(msg, runnerData.json, content);
                    break;

                default:
                    log.info("[Message] unknown command : " + command);
                    break;

            }

        } catch (Exception e) {
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }


    public static Void UserReg(RunnerData runnerData) {
        /**
         * 该命令来自于用户自己，或者owner2
         * **/
        try {

            MsgData msg = new MsgData(runnerData.json);
            CmdUserReg cmd = new CmdUserReg(msg.getContent(service.osnKey));


            // 检查 注册人数是否已经满了
            int count = db.user.getOsnUserCount();
            log.info("[UserReg] osn user count : " + count);
            if (count >= 2000){
                if (runnerData.sessionData.remote){
                    // 远程要怎么回？
                    CmdReUserReg reCmd = new CmdReUserReg();
                    reCmd.result = E_userRegisterFul.toString();
                    JSONObject ret = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);
                    sendOsxMessageNoSave(ret);
                } else
                    return sendReplyCode(runnerData, E_userRegisterFul, null);
            }

            if (isInThisNode(cmd.user)){
                log.info("[UserReg] error. already register user.");
                if (runnerData.sessionData.remote){
                    // 远程要怎么回？
                    CmdReUserReg reCmd = new CmdReUserReg();
                    reCmd.result = E_userExist.toString();
                    JSONObject ret = reCmd.makeMessage(service.osnID, msg.from, service.osnKey, runnerData.json);
                    sendOsxMessageNoSave(ret);
                } else
                    return sendReplyCode(runnerData, E_userExist, null);
            }

            if (!ipIMServer.equalsIgnoreCase(cmd.ip)){
                // ip 不对
                log.info("[UserReg] error. ip error.");
                return null;
            }

            if (!cmd.checkSign()){
                // 签名不对
                log.info("[UserReg] error. checkSign error.");
                return null;
            }

            // 存入数据库
            if (!db.osnUser.save(cmd)){
                // 不要重复发送
                log.info("[UserReg] error. db error.");
                return null;
            }

            // 转发给manage
            JSONObject sendData = cmd.genMessage(service.osnID, manageID, service.osnKey, null);
            sendMessageNoSave(manageID, sendData);

            if (runnerData.sessionData.remote){
                // 远程要怎么回？
            } else
                return sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }

    private static Void InitMember(RunnerData runnerData) {
        /**
         * 该命令来自于用户owner
         * 注册群成功以后，可以添加群成员
         * **/
        try {

            MsgData msg = new MsgData(runnerData.json);

            CmdInitMember cmd = new CmdInitMember(msg.getContent(service.osnKey));

            GroupData group = getGroupData(cmd.group);
            if (group.members.size() > 1){
                // 只有首次可以，添加成员以后则不行
                return null;
            }

            if (!group.isOwner(msg.from) && !group.isOwner2(msg.from)) {
                // 不是群所有者不能执行此命令
                return null;
            }

            addInitMembers(group, cmd.members);

            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = group.osnID;
            notice.state = "InitMember";
            notifyOne(group, msg.from, notice.toJson());


        } catch (Exception e) {
            log.error("", e);
            log.error(runnerData.json.toString());
        }
        return null;
    }






    private static Void Approver(MsgData msg, JSONObject json, JSONObject content) {
        /**
         * 该命令来自于manageID
         *
         * **/
        try {
            //log.info("[Approver] begin.");

            if (!msg.from.equalsIgnoreCase(manageID)){
                log.info("[Approver] not manage");
                return null;
            }
            
            CmdApprover cmd = new CmdApprover(content);

            if (!isUser(cmd.user)){
                // 不是user账号
                log.info("[Approver] error. not user");
                return null;
            }
            if (isInThisNode(cmd.user)){
                // 已经是节点了
                // 更新
                log.info("[Approver] error. already registered");
                return null;
            }


            // 获取osnUser中的数据 state = 1 判断一下时间
            if (!db.osnUser.isUndone(cmd.user, cmd.approverTime)){
                // 或许是已经处理过了，或许是db error
                log.info("[Approver] isUndone");
                return null;
            }

            // 获取数据
            CmdUserReg cmdUserReg = db.osnUser.readBySign(cmd.sign);
            if (cmdUserReg == null){
                // 没有数据
                log.info("[Approver] error. no user reg data");
                return null;
            }

            // 更新osn user 数据
            db.osnUser.updateState(cmdUserReg);

            // 创建user信息
            UserData userData = new UserData(cmdUserReg);
            db.user.insert(userData);
            // 添加到缓存中
            userMap.put(userData.osnID, userData);

            // pop 到osn网络中
            popOsnID(cmd.user);

        } catch (Exception e) {
            log.error("", e);
            log.error("error content : " + content);
        }
        return null;
    }

    private static Void GroupReg(MsgData msg, JSONObject json, JSONObject content) {

        /**
         * 该命令来自于manageID
         * 节点需要创建该群
         * **/
        try {

            //log.info("[GroupReg] begin. content: " + content);

            if (!msg.from.equalsIgnoreCase(manageID)){
                log.info("[GroupReg] not manage");
                return null;
            }
            CmdGroupReg cmd = new CmdGroupReg(content);
            //log.info("[GroupReg] cmd : " + cmd.toJson());
            if (!cmd.checkSign()){
                // 签名不对
                log.info("[GroupReg] error, sign error.");
                return null;
            }

            if (isInThisNode(cmd.group)){
                // 群已经存在了
                log.info("[GroupReg] error, group existed.");
                return null;
            }

            // 创建群
            createGroup(cmd);

        } catch (Exception e) {
            log.error("", e);
            log.error("error content : " +content);
        }
        return null;
    }

    private static Void GroupChange(MsgData msg, JSONObject json, JSONObject content) {
        /**
         * 该命令来自于manage
         * 由owner2签名， 发送给manage， manage转发给node
         * **/
        try {

            if (!msg.from.equalsIgnoreCase(manageID)){
                log.info("[GroupReg] not manage");
                return null;
            }

            CmdGroupChange cmd = new CmdGroupChange(content);
            if (!cmd.checkSign()){
                log.info("[GroupChange] error, check sign.");
                return null;
            }

            GroupData group = getGroupData(cmd.group);



            // 1.复制新群
            copyGroup(group, cmd.newGroupId, cmd.key);

            // 2. 发送全员通知
            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = group.osnID;
            notice.state = "GroupChange";
            notice.text = cmd.toMemberJson().toString();
            //systemNotifyGroup(group, notice.toJson());
            GroupNoticeServer.push(notice);

            // 3. 禁止一切群命令群
            group.mute = 1;
            group.type = 99;

            // 更新旧群的状态
            List<String> keys = new ArrayList<>();
            keys.add("type");
            db.group.update(group, keys);
            db.group.setAttribute(group, notice.text);

            // 4.等待对方回复以后，删除member

        } catch (Exception e) {
            log.error("", e);
            log.error("error content : " + content);
        }
        return null;
    }

    public static Void UserUnreg(MsgData msg, JSONObject json, JSONObject content) {
        /**
         * 该命令来自于manageID
         *
         * **/
        try {

            if (!manageID.equalsIgnoreCase(msg.from)){
                // 不是 manageID
                return null;
            }

            CmdUserUnreg cmd = new CmdUserUnreg(content);

            if (!isUser(cmd.user)){
                // 不是user账号
                return null;
            }
            if (!isInThisNode(cmd.user)){
                // 本节点没有该user
                return null;
            }

            // 判断一下时间
            if (db.osnUser.isUndone(cmd.user, cmd.time)){
                // 没有找到approver time 比time 大的
                // 删除用户信息
                delUserData(getUserData(cmd.user));
                db.osnUser.delete(cmd.user);

                // 从OSPN网络中删除
                popOsnID(cmd.user);
            }

        } catch (Exception e) {
            log.error("", e);
            log.error("error content : " + content);
        }
        return null;
    }

    public static Void GroupUnreg(MsgData msg, JSONObject json, JSONObject content) {
        /**
         * 该命令来自于manageID
         *
         * **/
        try {

            if (!manageID.equalsIgnoreCase(msg.from)){
                // 不是 manageID
                return null;
            }

            CmdGroupUnreg cmd = new CmdGroupUnreg(content);

            GroupData groupData = getGroupData(cmd.group);
            if (groupData == null){
                // 本节点没有该user
                return null;
            }

            long currentTime = System.currentTimeMillis();
            if (cmd.time + 1800000 < currentTime){
                // 时间已经超时了
                return null;
            }

            // 判断OSNG是不是新的群ID

            int ver = groupData.getVersion(cmd.group);
            if (ver != 2){
                return null;
            }

            // 通知owner 和 owner2
            GroupNoticeData notice = new GroupNoticeData();
            notice.groupID = cmd.group;
            notice.state = "GroupUnreg";
            notifyOne(groupData, groupData.owner, notice.toJson());
            notifyOne(groupData, groupData.owner2, notice.toJson());

            // 删除成员
            db.groupMember.deleteAll(groupData.osnID);
            // 删除群
            db.group.delete(groupData);


            groupMap.remove(groupData.osnID);

            // 从OSPN网络中删除
            popOsnID(cmd.group);



        } catch (Exception e) {
            log.error("", e);
            log.error("error content : " + content);
        }
        return null;
    }








    private static boolean createGroup(CmdGroupReg cmd) {

        GroupData groupData = new GroupData(cmd);

        MemberData memberData = new MemberData(
                cmd.owner,
                groupData.osnID,
                MemberType_Owner,
                groupData.owner,
                groupData.aesKey);
        groupData.addMember(memberData);
        groupData.type = GroupType_CrossGroup;

        // 存入数据库
        if (!db.group.insert(groupData)){
            log.info("[createGroup] error, group insert db error.");
            return false;
        }

        if (!db.groupMember.insert(memberData)){
            log.info("[createGroup] error, member insert db error.");
            return false;
        }
        groupMap.put(groupData.osnID, groupData);
        pushOsnID(groupData.osnID);
        newlyGroup(groupData, memberData.osnID);
        return true;
    }

    private static void addInitMembers(GroupData group, List<MemberInfo> members){
        List<MemberData> memberList = new ArrayList<>();
        List<String> memberIds = new ArrayList<>();
        memberIds.add(group.owner);
        for (MemberInfo md : members){
            if (memberIds.contains(md.osnID)) {
                // 去重复
                continue;
            }
            MemberData memberData = new MemberData(md, group);
            memberIds.add(md.osnID);
            memberList.add(memberData);
        }
        // 写入 数据库
        if (!db.groupMember.insertMembers(memberList)){
            log.info("[addMembers] error, db error");
            return;
        }

        // 更新到缓存
        for (MemberData md : memberList){
            group.addMember(md);
        }

    }

    private static void copyGroup(GroupData oldGroup, String newGroupId, String key){

        GroupData newGroup = new GroupData(oldGroup, newGroupId, key);

        // 写入数据库
        if (!db.group.insert(newGroup)){
            log.info("[copyGroup] error, db insert group");
            return;
        }
        if (!db.groupMember.insertMembers(newGroup.members.values())) {
            log.info("[copyGroup] error, db insert members");
            db.group.delete(newGroup);
            return;
        }

        groupMap.put(newGroupId,newGroup);
        pushOsnID(newGroupId);

    }

}
