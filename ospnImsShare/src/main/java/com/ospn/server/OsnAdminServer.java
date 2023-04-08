package com.ospn.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.ospn.OsnIMServer;
import com.ospn.command.MsgData;
import com.ospn.common.ECUtils;
import com.ospn.common.OsnUtils;
import com.ospn.core.IMData;
import com.ospn.data.*;
import com.ospn.service.DappService;
import com.ospn.service.GroupService;
import com.ospn.service.MessageService;
import com.ospn.service.UserService;
import com.ospn.utils.HttpUtils;
import com.ospn.utils.PerUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.util.*;

import static com.ospn.OsnIMServer.*;
import static com.ospn.api.GroupV2.createGroupFromHttp;
import static com.ospn.core.IMData.*;
import static com.ospn.data.Constant.*;
import static com.ospn.common.OsnUtils.*;
import static com.ospn.utils.CryptUtils.checkMessage;
import static com.ospn.utils.CryptUtils.makeMessage;
import static io.netty.util.CharsetUtil.UTF_8;

@Slf4j
public class OsnAdminServer extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final String adminKey;
    private static final String versionInfo = "v1.1 2021-3-27, base line, long link, findOsnID verify, osnid sync, keywordFilter";

    public OsnAdminServer() {
        adminKey = OsnIMServer.prop.getProperty("adminKey");
    }

    public static UserData RegsiterUser(String userName, String password, String owner2, String nickName, String portrait) {
        String[] newOsnID = ECUtils.createOsnID("user");
        UserData userData = new UserData();
        assert newOsnID != null;
        userData.osnID = newOsnID[0];
        userData.osnKey = newOsnID[1];
        userData.name = userName;
        userData.portrait = portrait;
        userData.owner2 = owner2;
        if (userData.owner2 == null){
            userData.owner2 = globalOwnerID;
        }
        log.info("user : " + userName);
        log.info("pwd : " + password);
        log.info("owner : " + userData.owner2);
        userData.password = Base64.getEncoder().encodeToString(sha256(password.getBytes()));
        userData.aesKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
        userData.msgKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
        userData.displayName = nickName;
        // userData.urlSpace = urlSpace;
        if (!db.user.insert(userData)) {
            log.info("insert error.");
            return null;
        }

        // 添加客服
        //addCustomer(userData.osnID);

        return userData;
    }

    public static UserData RegsiterUser2(String osnID, String name, String displayName, String owner2) {

        //String[] newOsnID = ECUtils.createOsnID("user");
        UserData userData = new UserData();
        //assert newOsnID != null;
        userData.osnID = osnID;
        userData.osnKey = "";
        userData.name = name;
        userData.owner2 = owner2;
        if (userData.owner2 == null){
            userData.owner2 = globalOwnerID;
        }
        //log.info("user : " + userName);
        //log.info("pwd : " + password);
        log.info("owner : " + userData.owner2);
        userData.password = "";
        userData.aesKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
        userData.msgKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
        userData.displayName = displayName;
        // userData.urlSpace = urlSpace;
        if (!db.user.insert(userData)) {
            log.info("insert error.");
            return null;
        }

        // 添加客服
        //addCustomer(userData.osnID);

        return userData;
    }

    private static void addCustomer(String user){
        for (String kf : OsnIMServer.customer){
            FriendData friendData = new FriendData(user, kf, 1);
            friendData.createTime = System.currentTimeMillis();
            db.friend.insert(friendData);
        }
    }

    private JSONObject replay(ErrorData error, String data) {
        JSONObject json = new JSONObject();
        json.put("errCode", error == null ? "0:success" : error.toString());
        json.put("data", data);
        if (error != null)
            log.info(error.toString());
        return json;
    }

    private JSONObject getData(JSONObject json) {
        try {
            String data = json.getString("data");
            return JSON.parseObject(OsnUtils.aesDecrypt(data, adminKey));
        } catch (Exception e) {

        }
        return null;
    }

    private void setData(JSONObject json) {
        String data = json.getString("data");
        if (data != null)
            json.put("data", OsnUtils.aesEncrypt(data, adminKey));
    }

    private JSONObject getOnlineUser(JSONObject json) {
        try {
            JSONArray array = new JSONArray();
            array.addAll(userMap.values());
            json.clear();
            json.put("userList", array);
            json = replay(null, json.toString());
            return json;
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject getUserInfo(JSONObject json) {
        try {
            String userID = json.getString("userID");
            UserData userData = getUserData(userID);
            if (userData == null)
                return replay(E_userNoFind, null);
            json.clear();
            json.put("userInfo", userData);
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject getGroupInfo(JSONObject json) {
        try {
            String groupID = json.getString("groupID");
            GroupData groupData = getGroupData(groupID);
            if (groupData == null)
                return replay(E_userNoFind, null);
            json.clear();
            json.put("groupInfo", groupData);
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONArray loadKeywords() {
        JSONArray keywords = new JSONArray();
        try {

        } catch (Exception e) {
            //log.error("", e);
        }
        return keywords == null ? new JSONArray() : keywords;
    }

    private JSONObject loadAppVersion() {
        JSONObject version = new JSONObject();
        try {

        } catch (Exception e) {
            //log.error("", e);
        }
        return version;
    }

    private JSONObject listQueueInfo() {


        try {

            JSONObject json = new JSONObject();

            json.put("CommandGroupImportantServer", CommandGroupImportantServer.getQueueSize());
            json.put("CommandLoginServer", CommandLoginServer.getQueueSize());
            json.put("CommandUserImportantServer", CommandUserImportantServer.getQueueSize());
            json.put("CommandWorkerServer", CommandWorkerServer.getQueueSize());
            json.put("GroupNoticeServer", GroupNoticeServer.getQueueSize());
            json.put("CompleteProcessServer1", MessageProcessServer.getQueueSize1());
            json.put("CompleteProcessServer2", MessageProcessServer.getQueueSize2());
            json.put("MessageResendServerIms", MessageResendServer.getQueueSize1());
            json.put("MessageResendServerOxs", MessageResendServer.getQueueSize2());
            json.put("MessageSaveServer", MessageSaveServer.getQueueSize());
            json.put("MessageSyncServer1", MessageSyncServer.getQueueSize1());
            json.put("MessageSyncServer2", MessageSyncServer.getQueueSize2());
            json.put("PushServer1", PushServer.getQueueSize1());
            json.put("PushServer2", PushServer.getQueueSize2());
            json.put("ResultServer", ResultServer.getQueueSize());


            json.put("session", sessionMap.size());


            return json;



        } catch (Exception e) {

        }

        return null;
    }

    private JSONObject resetpwd(JSONObject json) {
        try {
            String username = json.getString("username");
            String userID = json.getString("userID");
            String password = json.getString("password");
            UserData userData = userID == null ? getUserDataByName(username) : getUserData(userID);
            if (userData == null)
                return replay(E_userNoFind, null);
            String passwordOld = userData.password;
            userData.password = Base64.getEncoder().encodeToString(sha256(password.getBytes()));
            if (!db.updateUser(userData, Collections.singletonList("password"))) {
                userData.password = passwordOld;
                return replay(E_dataBase, null);
            }
            log.info("userID: " + userID + ", name: " + username);
            return replay(null, null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject register(JSONObject json) {
        try {
            String username = json.getString("username");
            String password = json.getString("password");
            String portrait = json.getString("portrait");
            String owner2 = json.getString("owner2");
            if (username == null || password == null)
                return replay(E_missData, null);
            else if (db.isRegisterName(username)) {
                UserData userData = db.readUserByName(username);
                json = new JSONObject();
                json.put("osnID", userData.osnID);
                return replay(E_userExist, json.toString());
            }


            String nickName = json.getString("nickName");
            if (nickName == null) {
                nickName = "OSPN-DAO User";
            }

            UserData userData = RegsiterUser(username, password, owner2, nickName,portrait);
            if (userData == null)
                return replay(E_registFailed, null);
            OsnIMServer.Inst.pushOsnID(userData);
            log.info("register user: " + username + ", osnID: " + userData.osnID);

            json = new JSONObject();
            json.put("osnID", userData.osnID);
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject register2(JSONObject json) {
        try {
            log.info("register2 : " + json);
            String osnID = json.getString("osnID");
            String owner2 = json.getString("owner2");
            if (osnID == null)
                return replay(E_missData, null);

            String displayName = json.getString("name");
            if (displayName == null) {
                displayName = "OSPN-DAO User";
            }
            if (displayName.equalsIgnoreCase("")) {
                displayName = "OSPN-DAO User";
            }


            Random random = new Random();

            String name = "U"+System.currentTimeMillis() + random.nextInt(1000000);


            UserData ud = db.user.readUserByID(osnID);
            if (ud != null) {
                json = new JSONObject();
                json.put("osnID", ud.osnID);
                return replay(E_userExist, json.toString());
            }

            UserData userData = RegsiterUser2(osnID, name, displayName, owner2);

            if (userData == null)
                return replay(E_registFailed, null);
            OsnIMServer.Inst.pushOsnID(userData);
            //log.info("register user: " + username + ", osnID: " + userData.osnID);

            json = new JSONObject();
            json.put("osnID", userData.osnID);
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject getTimestamp(JSONObject data) {
        JSONObject json = new JSONObject();
        json.put("timestamp", System.currentTimeMillis());
        return replay(null, json.toString());
    }

    private JSONObject unregister(JSONObject json) {
        try {

            // 时间戳 在正负3分钟内有效
            String osnID = json.getString("osnID");
            String owner2 = json.getString("owner2");
            String service = json.getString("serviceId");
            long time = json.getLongValue("timestamp");
            String sign = json.getString("sign");

            long timeNow = System.currentTimeMillis();
            if (time < timeNow && time > timeNow - 1000 * 60) {

                // 验证输入有效性
                String calc = osnID + owner2 + service + time;
                String hash = ECUtils.osnHash(calc.getBytes());
                if (!ECUtils.osnVerify(osnID, hash.getBytes(), sign)) {
                    return replay(E_verifyError, null);
                }

                UserData ud = db.user.readUserByID(osnID);
                if (ud == null) {
                    return replay(E_userNoFind, null);
                }


                // 删除user信息
                delUserData(ud);
                // 删除user好友信息
                db.friend.deleteAll(osnID);
                // 删除user群信息

                OsnIMServer.Inst.popOsnID(osnID);

                json = new JSONObject();
                json.put("errCode", "0:success");
                return replay(null, json.toString());

            } else {
                return replay(E_timeSync, null);
            }

        } catch (Exception e) {
            //log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject sendNotify(JSONObject json) {
        try {
            String command = json.getString("command");
            CommandData commandData = getCommand(command);
            if (commandData == null)
                return replay(E_errorCmd, null);
            long timestamp = Long.parseLong(json.getString("timestamp"));
            if (timestamp - System.currentTimeMillis() > 5 * 60 * 1000)
                return replay(E_timeSync, null);
            String to = json.getString("to");
            String from = json.getString("from");
            if (from == null || to == null || !isOsnID(from) || !isOsnID(to))
                return replay(E_missData, null);
            ErrorData error = checkMessage(json);
            if (error != null)
                return replay(error, null);
            return replay(Inst.sendNotify(json), null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject userNotify(JSONObject json) {
        try {
            String to = json.getString("to");
            String from = json.getString("from");
            String osnKey = json.getString("osnKey");
            String content = json.getString("content");
            if (to == null || from == null || osnKey == null)
                return replay(E_missData, null);
            JSONObject data = makeMessage("Message", from, to, content, osnKey, null);
            return replay(Inst.sendNotify(data), null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject getFriends(JSONObject json) {
        try {
            String userID = json.getString("userID");
            UserData userData = getUserData(userID);
            if (userData == null)
                return replay(E_userNoFind, null);
            json.clear();
            json.put("friends", userData.friends);
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject createGroup(JSONObject json) {
        try {

            String groupId = createGroupFromHttp(json);

            if (groupId == null) {
                return replay(new ErrorData("-1", ""), null);
            }

            JSONObject result = new JSONObject();
            result.put("groupId", groupId);

            return replay(null, result.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject disableUser(JSONObject json) {
        try {

            String userId = json.getString("user");
            if (userId == null) {
                return replay(new ErrorData("-1", "format error"), null);
            }

            UserData userData = db.user.readUserByID(userId);
            if (userData == null) {
                return replay(new ErrorData("-1", "user not find"), null);
            }

            if (!db.userDisable.insert(userData)) {
                return replay(new ErrorData("-1", "disable user db failed"), null);
            }

            db.user.delete(userId);
            if (userData.session != null) {
                delSessionData(userData.session);
            }

            delUserData(userData);

            return replay(null, null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject enableUser(JSONObject json) {
        try {

            String userId = json.getString("user");
            if (userId == null) {
                return replay(new ErrorData("-1", "format error"), null);
            }

            UserData userData = db.userDisable.readUserByID(userId);
            if (userData == null) {
                return replay(new ErrorData("-1", "user not find"), null);
            }

            if (!db.user.insert(userData)) {
                return replay(new ErrorData("-1", "disable user db failed"), null);
            }

            db.userDisable.delete(userId);
            pushOsnID(userId);


            return replay(null, null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject disableGroup(JSONObject json) {
        try {

            String group = json.getString("group");
            if (group == null) {
                return replay(new ErrorData("-1", "format error"), null);
            }

            GroupData groupData = db.group.read(group);
            if (groupData == null) {
                return replay(new ErrorData("-1", "user not find"), null);
            }

            if (!db.groupDisable.insert(groupData)) {
                return replay(new ErrorData("-1", "disable user db failed"), null);
            }

            db.group.delete(groupData);
            groupMap.remove(group);

            return replay(null, null);

        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject enableGroup(JSONObject json) {
        try {

            String group = json.getString("group");
            if (group == null) {
                return replay(new ErrorData("-1", "format error"), null);
            }

            GroupData groupData = db.groupDisable.read(group);
            if (groupData == null) {
                return replay(new ErrorData("-1", "user not find"), null);
            }

            if (!db.group.insert(groupData)) {
                return replay(new ErrorData("-1", "disable user db failed"), null);
            }

            db.groupDisable.delete(groupData);

            pushOsnID(group);

            return replay(null, null);

        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject createOsnID(JSONObject json) {
        try {
            String type = json.getString("type");
            if (type == null)
                return replay(E_missData, null);
            String[] osnID = ECUtils.createOsnID(type);
            JSONObject data = new JSONObject();
            assert osnID != null;
            data.put("osnID", osnID[0]);
            data.put("oskKey", osnID[1]);
            return replay(null, data.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject isRegister(JSONObject json) {
        try {
            String userID = json.getString("userID");
            UserData userData = getUserData(userID);
            if (userData == null)
                return replay(E_userNoFind, null);
            json.clear();
            return replay(null, json.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    private JSONObject setUserInfo(JSONObject json) {
        try {
            String userID = json.getString("userID");
            UserData userData = getUserData(userID);
            if (userData == null)
                return replay(E_userNoFind, null);
            UserData userKeys = new UserData();
            userKeys.osnID = userID;
            List<String> keys = userData.parseKeys(json, userKeys);
            if(keys.isEmpty()){
                return replay(E_missData, null);
            }
            log.info("keys: "+keys);
            if (!db.updateUser(userKeys, keys)) {
                return replay(E_dataBase, null);
            }
            return replay(null, null);
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }
    private JSONObject getUnreadCount(JSONObject json) {
        try {
            String userID = json.getString("userID");
            long timestamp = json.getLongValue("timestamp");
            if(userID == null){
                return replay(E_missData, null);
            }
            int count = OsnIMServer.db.getUnreadCount(userID, timestamp);
            JSONObject data = new JSONObject();
            data.put("count", count);
            return replay(null, data.toString());
        } catch (Exception e) {
            log.error("", e);
            return replay(new ErrorData("-1", e.toString()), null);
        }
    }

    /**以下几条是后面加上的
     * 主要处理消息的
     * 发送
     * 获取
     * 回执
     * **/
    private JSONObject sendMsg(JSONObject json){
        try {
            MsgData msg = new MsgData(json);
            RunnerData runnerData = new RunnerData(null, json);
            if (isUser(msg.to)){
                UserService.handleMessage(runnerData);
            } else if (isGroup(msg.to)){
                GroupService.handleMessage(runnerData);
            } else if (isService(msg.to)){
                //DappService.handleMessage(runnerData);
            }

        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }

    private JSONObject recvMessage(JSONObject json){
        try {
            SessionData sessionData = new SessionData(null);
            OsnIMServer.Inst.handleMessage(sessionData, json);
        } catch (Exception e){
            log.error("", e);
        }
        return replay(null, null);
    }

    private JSONObject complete(JSONObject json){
        try {

            MsgData msg = new MsgData(json);
            if (!msg.command.equalsIgnoreCase("Complete")){
                return null;
            }
            RunnerData runnerData = new RunnerData(null, json);
            MessageService.complete(runnerData);

        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }
    private JSONObject getMsg(JSONObject json){
        try {
            MsgData msg = new MsgData(json);
            if (!msg.command.equalsIgnoreCase("MessageSync")){
                return null;
            }

            JSONObject result = MessageService.getMessage(msg);
            if (result != null){
                return replay(null, result.toString());
            }

        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }
    private JSONObject history(JSONObject json){
        try {
            MsgData msg = new MsgData(json);
            if (!msg.command.equalsIgnoreCase("MessageSync")){
                return null;
            }
            JSONObject result = MessageService.history(msg);
            if (result != null){
                return replay(null, result.toString());
            }

        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }

    private JSONObject getServiceID(JSONObject json){
        try {
            JSONObject data = new JSONObject();
            data.put("time", System.currentTimeMillis());
            data.put("service", service.osnID);
            return replay(null, data.toString());
        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }

    private JSONObject userReg(JSONObject json){
        try {

            MsgData msg = new MsgData(json);
            if (msg.command.equalsIgnoreCase("UserReg")) {
                RunnerData runnerData = new RunnerData(null, json);
                DappService.handleMessage(runnerData);
            }
        } catch (Exception e){
            log.error("", e);
        }

        return replay(null, null);
    }





    public JSONObject handleAdmin(JSONObject json) {
        JSONObject result;
        try {
            result = getData(json);
            switch (json.getString("command")) {
                case "register":
                    result = register(result);
                    break;
                case "register2":
                    result = register2(result);
                    break;

                case "unregister":
                    result = unregister(result);
                    break;


                case "resetPassword":
                    result = resetpwd(result);
                    break;

                case "userOnline":
                    result = getOnlineUser(result);
                    break;
                case "userInfo":
                    result = getUserInfo(result);
                    break;
                case "groupInfo":
                    result = getGroupInfo(result);
                    break;
                case "sendNotify":
                    result = sendNotify(result);
                    break;
                case "userNotify":
                    result = userNotify(result);
                    break;
                case "getFriends":
                    result = getFriends(result);
                    break;

                case "createGroup":
                    result = createGroup(result);
                    break;

                case "disableUser":
                    result = disableUser(result);
                    break;

                case "enableUser":
                    result = enableUser(result);
                    break;

                case "disableGroup":
                    result = disableGroup(result);
                    break;

                case "enableGroup":
                    result = enableGroup(result);
                    break;



                case "createOsnID":
                    result = createOsnID(result);
                    break;
                case "isRegister":
                    result = isRegister(result);
                    break;
                case "setUserInfo":
                    result = setUserInfo(result);
                    break;
                case "getUnreadCount":
                    result = getUnreadCount(result);
                    break;



                case "getServiceID":
                    result = getServiceID(result);
                    break;
                case "userReg":
                    result = userReg(result);
                    break;



                case "sendMsg":
                    result = sendMsg(result);
                    break;
                case "getMsg":
                    result = getMsg(result);
                    break;
                case "complete":
                    result = complete(result);
                    break;
                case "history":
                    result = history(result);
                    break;

                default:
                    result = replay(E_errorCmd, null);
                    break;
            }
            setData(result);
        } catch (Exception e) {
            result = replay(new ErrorData("-1", e.toString()), null);
            log.error("", e);
        }
        return result;
    }

    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest fullHttpRequest) {
        try {
            JSONObject json;
            if (fullHttpRequest.method() == HttpMethod.POST) {
                String content = fullHttpRequest.content().toString(UTF_8);
                //json = JSON.parseObject(content);

                String uri = fullHttpRequest.uri();
                if (uri.equalsIgnoreCase("/message")) {
                    log.info("HTTP POST message recv.");
                    json = recvMessage(JSON.parseObject(content));
                } else {
                    json = JSON.parseObject(content);
                    json = handleAdmin(json);
                }

            } else if (fullHttpRequest.method() == HttpMethod.GET) {
                json = new JSONObject();
                String uri = fullHttpRequest.uri();
                if (uri.equalsIgnoreCase("/serviceid")) {
                    json.put("serviceID", service.osnID);
                } else if (uri.equalsIgnoreCase("/version")) {
                    json.put("version", versionInfo);
                } else if (uri.equalsIgnoreCase("/keywordFilter")) {
                    JSONArray keywords = loadKeywords();
                    json.put("keywords", keywords);
                } else if (uri.equalsIgnoreCase("/appVersion")) {
                    json.put("appVersion", loadAppVersion());
                } else if (uri.equalsIgnoreCase("/queueInfo")) {
                    json.put("queueInfo", listQueueInfo());
                } else if (uri.equalsIgnoreCase("/mapInfo")) {
                    json.put("mapInfo", IMData.getMapData());
                }else if (uri.equalsIgnoreCase("/helpInfo")) {
                    json.put("helpIn", helpIn);
                    json.put("helpOut", helpOut);
                }else if(uri.equalsIgnoreCase("/startCounter")) {
                    PerUtils.start();
                }else if(uri.equalsIgnoreCase("/stopCounter")) {
                    PerUtils.stop();
                }else if(uri.equalsIgnoreCase("/showCounter")){
                    json = PerUtils.show();
                } else
                    json.put("errCode", "unsupport uri");
            } else {
                json = new JSONObject();
                json.put("errCode", "unsupport method");
            }
            HttpUtils.sendReply(ctx, json);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }
}
