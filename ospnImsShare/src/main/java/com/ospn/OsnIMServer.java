package com.ospn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ospn.command.MsgData;
import com.ospn.common.ECUtils;
import com.ospn.common.OsnSender;
import com.ospn.common.OsnServer;
import com.ospn.common.OsnUtils;
import com.ospn.core.IMData;
import com.ospn.data.*;
import com.ospn.server.*;
import com.ospn.service.*;
import com.ospn.utils.DBUtils;

import com.ospn.utils.PerUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;



import static com.ospn.common.OsnUtils.*;
import static com.ospn.core.IMData.*;
import static com.ospn.data.CommandData.*;
import static com.ospn.data.Constant.*;
import static com.ospn.data.MessageData.toMessageData;
import static com.ospn.service.MessageService.sendReplyCode;
import static com.ospn.utils.CryptUtils.*;

@Slf4j
public class OsnIMServer extends OsnServer {
    public static void main(String[] args) {
        try {
            OsnUtils.init("ims");
            OsnIMServer.Inst = new OsnIMServer();
            OsnIMServer.Inst.StartServer();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static OsnSender osxSender = null;
    public static OsnIMServer Inst = null;
    public static DBUtils db = new DBUtils();
    public static Properties prop = new Properties();
    public static String configFile = "ospn.properties";
    public static String connectorKey;
    public static String initMessageMapFlag;
    public static String pushToConnectorFlag;
    public static String groupServer;
    public static String imType;
    public static List imsList;
    public static List<String> customer;
    public static TimerTaskService taskService = new TimerTaskService();





    public static class JsonSender implements OsnSender.Callback {

        public void onDisconnect(OsnSender sender, String error) {
            try {
                //log.info(sender.mIp + ":" + sender.mPort + "/" + sender.mTarget + ", error: " + error);
                Thread.sleep(2000);
                osxSender = OsnSender.newInstance(sender);
            } catch (Exception ignore) {
            }
        }

        public void onCacheJson(OsnSender sender, JSONObject json) {
            log.info("drop target: " + sender.mTarget + ", ip: " + sender.mIp);
        }

        public List<JSONObject> onReadCache(OsnSender sender, String target, int count) {
            return new ArrayList<>();
        }
    }
    private void StartServer() {
        try {
            taskService.run();
            db.initDB(configFile);
            prop.load(new FileInputStream(configFile));




            IMData.init(db, prop);

            connectorKey = prop.getProperty("connectorKey");
            initMessageMapFlag = prop.getProperty("initMessageMapFlag");
            pushToConnectorFlag = prop.getProperty("pushToConnectorFlag");
            groupServer = prop.getProperty("groupServer", "");
            String imsListTemp = prop.getProperty("imsList");
            if (imsListTemp != null) {
                String[] array = imsListTemp.split(",");
                imsList = Arrays.asList(array);
            }
            log.info("imsList : " + imsList);
            imType = prop.getProperty("imType", "normal");
            log.info("imType : " + imType);




            db.message.setSaveTime(saveTime);
            String customerStr = prop.getProperty("customer", null);
            if (customerStr != null){
                JSONObject json = JSONObject.parseObject(customerStr);
                if (json != null){
                    customer = json.getObject("kf", TypeReference.LIST_STRING);
                }
            }
            if (customer == null){
                customer = new ArrayList<>();
            }

            //log.info("[StartServer] customer : " + customer);


            setCommand("1", "Heart", 0, Inst::Heart);
            setCommand("1", "Result", 0, Inst::Result);
            setCommand("1", "MessageForward", 0, Inst::MessageForward);
            setCommand("1", "Complete", NeedVerify, Inst::Complete);


            //setCommand("1", "Message", NeedVerify | NeedBlock, Inst::Message);



            /** DAPP SERVICE **/
            //setCommand("1", "GetFriendInfo", NeedVerify | NeedOnline | NeedContent, Inst::GetFriendInfo);
            //setCommand("1", "GetFriendList", NeedVerify | NeedOnline, Inst::GetFriendList);
            //setCommand("1", "GetGroupList", NeedVerify | NeedOnline, Inst::GetGroupList);
            //setCommand("1", "SetUserInfo", NeedVerify | NeedOnline | NeedContent, Inst::SetUserInfo);
            //setCommand("1", "CreateGroup", NeedVerify | NeedOnline, Inst::CreateGroup);
            //setCommand("1", "SetFriendInfo", NeedVerify | NeedOnline | NeedContent, Inst::SetFriendInfo);
            //setCommand("1", "DelFriend", NeedVerify | NeedOnline | NeedContent, Inst::DelFriend);
            //setCommand("1", "SetTemp", 0, Inst::SetTemp);
            //setCommand("1", "FindObj", 0, Inst::FindObj);
            setCommand("1", "GetServiceInfo", 0, Inst::GetServiceInfo);
            //setCommand("1", "MessageSync", NeedVerify | NeedOnline | NeedContent, Inst::MessageSync);
            setCommand("1", "Login", 0, Inst::Login);
            setCommand("1", "Logout", 0, Inst::Logout);
            setCommand("1", "Register", 0, Inst::Register);
            setCommand("1", "GetLoginInfo", 0, Inst::GetLoginInfo);
            setCommand("1", "LoginV2", 0, Inst::LoginByOsnID);



            /** USER SERVICE **/
            //setCommand("1", "AgreeFriend", NeedVerify | NeedReceipt | NeedOnline | NeedContent, Inst::AgreeFriend);
            //setCommand("1", "RejectFriend", NeedVerify | NeedReceipt | NeedOnline | NeedContent, Inst::RejectFriend);
            //setCommand("1", "AddFriend", NeedVerify | NeedOnline | NeedSaveOut, Inst::AddFriend);
            //setCommand("1", "GetUserInfo", NeedVerify | NeedOnline, Inst::GetUserInfo);
            // message类型，不存
            //setCommand("1", "UserInfo", 0, Inst::Forwarder);
            // message类型，不存
            //setCommand("1", "GroupInfo", 0, Inst::Forwarder);
            // message类型，不存
            //setCommand("1", "MemberInfo", 0, Inst::Forwarder);
            // message类型，不存
            //setCommand("1", "ServiceInfo", 0, Inst::Forwarder);
            // message类型，需要保存
            //setCommand("1", "GroupUpdate", NeedVerify | NeedReceipt| NeedSave, Inst::GroupUpdate);




            /** GROUP SERVICE **/
            //setCommand("1", "AddMember", NeedVerify | NeedReceipt | NeedOnline, Inst::AddMember);
            //setCommand("1", "DelMember", NeedVerify | NeedReceipt | NeedOnline, Inst::DelMember);
            //setCommand("1", "JoinGroup", NeedVerify | NeedReceipt | NeedOnline | NeedSaveOut, Inst::JoinGroup);
            //setCommand("1", "AgreeMember", NeedVerify | NeedReceipt | NeedOnline, Inst::AgreeMember);
            //setCommand("1", "RejectMember", NeedVerify | NeedReceipt | NeedOnline, Inst::RejectMember);
            //setCommand("1", "GetGroupSign", NeedVerify | NeedOnline, Inst::GetGroupSign);
            //setCommand("1", "GetOwnerSign", NeedVerify | NeedOnline, Inst::GetOwnerSign);
            //setCommand("1", "Billboard",0, Inst::Billboard);
            //setCommand("1", "Mute",0, Inst::Mute);
            //setCommand("1", "JoinGrp",NeedVerify | NeedReceipt | NeedSave, Inst::JoinGrp);
            //setCommand("1", "UpgradeGroup", 0, Inst::UpgradeGroup);
            //setCommand("1", "SetGroupInfo", NeedVerify | NeedOnline, Inst::SetGroupInfo);
            //setCommand("1", "SetMemberInfo", NeedVerify | NeedOnline, Inst::SetMemberInfo);
            //setCommand("1", "RejectGroup", NeedVerify | NeedReceipt | NeedOnline, Inst::RejectGroup);
            //setCommand("1", "SystemNotify", 0, Inst::SystemNotify);
            //setCommand("1", "Invitation",NeedVerify | NeedReceipt | NeedSave, Inst::Invitation);
            //setCommand("1", "InviteGrp", NeedVerify | NeedSave | NeedBlock, Inst::InviteGrp);
            //setCommand("1", "QuitGroup", NeedVerify | NeedReceipt | NeedOnline, Inst::QuitGroup);
            //setCommand("1", "DelGroup", NeedVerify | NeedReceipt | NeedOnline, Inst::DelGroup);
            //setCommand("1", "GetGroupInfo", NeedVerify | NeedOnline, Inst::GetGroupInfo);
            //setCommand("1", "GetMemberInfo", NeedVerify | NeedOnline, Inst::GetMemberInfo);


            /** unknown command **/
            setCommand("1", "SetMessage", NeedVerify | NeedOnline | NeedSaveOut, Inst::SetMessage);
            setCommand("1", "MessageLoad", NeedVerify | NeedOnline | NeedContent, Inst::MessageLoad);
            setCommand("1", "GetConversationList", NeedVerify | NeedOnline, Inst::GetConversationList);
            setCommand("1", "GetConversationInfo", NeedVerify | NeedOnline | NeedContent, Inst::GetConversationInfo);
            setCommand("1", "SetConversationInfo", NeedVerify | NeedOnline | NeedContent, Inst::SetConversationInfo);
            setCommand("1", "Dynamic", NeedVerify | NeedOnline, Inst::Dynamic);
            setCommand("1", "DynamicUpdate", NeedVerify | NeedReceipt | NeedSave, Inst::DynamicUpdate);
            setCommand("1", "GetRequestList", NeedVerify | NeedOnline | NeedContent, Inst::GetRequestList);
            setCommand("1", "waitReceipt", 0, Inst::WaitReceipt);
            setCommand("1", "findOsnID", 0, Inst::FindOsnID);
            setCommand("1", "Broadcast", 0, Inst::Broadcast);
            setCommand("1", "userCert", NeedVerify | NeedOnline | NeedContent, Inst::UserCert);
            setCommand("1", "OrderPay", NeedVerify | NeedOnline, Inst::OrderPay);
            setCommand("1", "UserUpdate", NeedVerify, Inst::Forwarder);




            //IMData.initExtention();

            //AddService(imServicePort, new OsnIMSServer());
            AddService(imServicePort, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    arg0.pipeline().addLast(new MessageDecoder());
                    arg0.pipeline().addLast(new MessageEncoder());
                    arg0.pipeline().addLast(new OsnIMSServer());
                }
            });
            /*AddService(imServicePort+1, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    arg0.pipeline().addLast(new MessageDecoder());
                    arg0.pipeline().addLast(new MessageEncoder());
                    arg0.pipeline().addLast(new OsnIMSServer());
                }
            });
            AddService(imServicePort+2, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    arg0.pipeline().addLast(new MessageDecoder());
                    arg0.pipeline().addLast(new MessageEncoder());
                    arg0.pipeline().addLast(new OsnIMSServer());
                }
            });*/
            AddService(imNotifyPort, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    arg0.pipeline().addLast(new MessageDecoder());
                    arg0.pipeline().addLast(new MessageEncoder());
                    arg0.pipeline().addLast(new OsnOSXServer());
                }
            });
            AddService(imAdminPort, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    arg0.pipeline().addLast("http-decoder", new HttpRequestDecoder());
                    arg0.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                    arg0.pipeline().addLast("http-encoder", new HttpResponseEncoder());
                    arg0.pipeline().addLast("http-chunked", new ChunkedWriteHandler());
                    arg0.pipeline().addLast("handler", new OsnAdminServer());
                }
            });
            /*AddService(imWebsockPort, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    if(isSsl()){
                        AddCert(arg0, certPem, certKey);
                    }
                    arg0.pipeline().addLast("http-codec", new HttpServerCodec());
                    arg0.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                    arg0.pipeline().addLast("http-chunked", new ChunkedWriteHandler());
                    arg0.pipeline().addLast("handler", new OsnWSServer());
                }
            });*/
            AddService(imHttpPort, new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel arg0) {
                    if(isSsl()){
                        AddCert(arg0, certPem, certKey);
                    }
                    arg0.pipeline().addLast("http-codec", new HttpServerCodec());
                    arg0.pipeline().addLast("http-chunked", new ChunkedWriteHandler());
                    arg0.pipeline().addLast("handler", new OsnFileServer());
                }
            });

            File file = new File("cache");
            if (!file.exists()) {
                if (!file.mkdir())
                    log.info("mkdir cache failed");
            }
            file = new File("portrait");
            if (!file.exists()) {
                if (!file.mkdir())
                    log.info("mkdir portrait failed");
            }
            osxSender = OsnSender.newInstance(ipConnector, ospnServicePort, null, false, 2000, new JsonSender(), null);


            OsnSyncServer.initServer();

            CommandWorkerServer.init(prop);
            CommandLoginServer.init(prop);
            CommandGroupImportantServer.init(prop);
            CommandUserImportantServer.init(prop);
            //MessageResendServer.init();
            MessageProcessServer.init();
            MessageSyncServer.init();
            ResendOsxServer.init();
            MessageClearServer.init(prop);


            new Thread(MessageSaveServer::run).start();

            new Thread(GroupNoticeServer::run).start();

            new Thread(PushServer::run).start();

            new Thread(ResultServer::run).start();





            sendImsType();

            log.info("push service osnID : " + service.osnID);
            pushOsnID(service.osnID);

            if (manageID != null){
                if (!manageID.equalsIgnoreCase("")){


                    // 这里需要发送注册指令
                    JSONObject content = new JSONObject();
                    content.put("command", "NodeReg");
                    content.put("osnID", service.osnID);
                    content.put("ip", ipIMServer);
                    content.put("regTime", System.currentTimeMillis());


                    log.info("content:"+ content.toString());

                    JSONObject json = makeMessage("Message", service.osnID, manageID, content, service.osnKey, null);
                    sendOsxJson(json);
                    //sendUserJson(manageID, json);

                }
            }



            new Thread(OsnIMServer::pushAll).start();


            //PerUtils.worker();
        } catch (Exception e) {
            log.error("", e);
        }
    }



    static void initRedisMessageMap(){

        log.info("get redis.");
        Jedis jedis = db.message.getJedis();
        if (jedis == null) {
            return;
        }
        try {

            int count = 200;
            int offset = 0;

            while (true){

                List<MessageData> messages = db.message.listUnreadPage(offset, count);
                if (messages.size() > 0) {
                    // 存入redis缓存
                    offset = initRedisMessageMap(jedis, messages);
                    log.info("init redis message map, count(" + messages.size()+
                            ") offset(" +offset+
                            ")");
                }
                if (messages.size() < count) {
                    log.info("init redis message map end.");
                    break;
                }
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }


        log.info("close redis.");
        db.message.closeJedis(jedis);

    }

    static int initRedisMessageMap(Jedis jedis, List<MessageData> messages) {

        long offset = 0;
        for (MessageData msgData : messages) {
            if (msgData.id > offset) {
                offset = msgData.id;
            }
            db.message.createRedisMessageMap(jedis, msgData.toID, msgData.hash);
        }

        return (int)offset;
    }


    public static void pushAll(){
        try {

            if (initMessageMapFlag != null) {
                // 从数据库中读取数据，并存入redis message map
                initRedisMessageMap();
            }

            if (pushToConnectorFlag == null) {
                return;
            }



            int offset = 0;
            int count = 100;
            while (true) {
                List<String> users = db.user.listPage(offset, count);
                log.info("user count : " + users.size());
                for (String userId : users) {
                    pushOsnID(userId);
                    Thread.sleep(50);
                }
                if (users.size() < count) {
                    break;
                }
                offset += count;
            }

            offset = 0;
            while (true) {
                List<String> osnIdList = db.group.listPage(offset, count);
                log.info("group count : " + osnIdList.size());
                //log.info("group list : " + osnIdList);
                for (String osnID : osnIdList) {
                    pushOsnID(osnID);
                    Thread.sleep(50);
                }
                if (osnIdList.size() < count) {
                    break;
                }
                offset += count;
            }


        } catch (Exception e) {
            log.error("", e);
        }



    }

    public static void pushOsnID(CryptData cryptData) {
        JSONObject json = new JSONObject();
        json.put("command", "pushOsnID");
        json.put("osnID", cryptData.osnID);
        // 加密发送

        /*try {
            String cipher = aesEncrypt(cryptData.osnID, connectorKey);
            json.put("cipher", cipher);
        } catch (Exception e) {
            log.info(e.getMessage());
        }*/


        OsnIMServer.Inst.sendOsxJson(json);
        //log.info("osnID: " + cryptData.osnID);
    }
    public static void pushOsnID(String osnID) {
        JSONObject json = new JSONObject();
        json.put("command", "pushOsnID");
        json.put("osnID", osnID);

        // 加密发送
        /*try {
            String cipher = aesEncrypt(osnID, connectorKey);
            json.put("cipher", cipher);
        } catch (Exception e) {
            log.info(e.getMessage());
        }*/

        OsnIMServer.Inst.sendOsxJson(json);
    }

    public static void popOsnID(String osnID){
        JSONObject json = new JSONObject();
        json.put("command", "popOsnID");
        json.put("osnID", osnID);

        // 加密发送
        /*try {
            String cipher = aesEncrypt(osnID, connectorKey);
            json.put("cipher", cipher);
        } catch (Exception e) {
            log.info(e.getMessage());
        }*/

        OsnIMServer.Inst.sendOsxJson(json);
        //log.info("osnID: " + osnID);
    }
    public void sendImsType(){
        JSONObject json = new JSONObject();
        json.put("command", "setImsType");
        json.put("type", "share");
        sendOsxJson(json);
    }
    public void sendOsxJson(JSONObject json) {
        //if (IMData.standAlone)
        //    return;
        CommandData cmd = getCommand(json.getString("command"));

        if (cmd != null && cmd.needReceipt)
            db.insertReceipt(toMessageData(json, ReceiptStatus_Wait));
        //sendJson(ipConnector, ospnServicePort, json);
        osxSender.send(json);
    }
    private void saveClientJson(JSONObject json) {
        String command = json.getString("command");
        if(command == null)
            return;
        CommandData cmd = getCommand(command);
        if (cmd != null && (cmd.needSave || cmd.needSaveOut)) {
            if (!json.containsKey("saved")) {
                db.insertMessage(toMessageData(json, MessageStatus_Saved));
                json.put("saved", true);
            }
        }
    }
    private void sendClientJson(SessionData sessionData, JSONObject json) {
        saveClientJson(json);
        json.remove("saved");
        json.remove("aeskey");
        if (sessionData.webSock) {
            if (json.containsKey("ecckey") && sessionData.user != null)
                toAesMessage(json, sessionData.user.osnKey);
            OsnWSServer.sendClientJson(sessionData, json);
        } else {
            //log.info("[sendClientJson] hash code : " + sessionData.ctx.hashCode());

            //tempSendMsg.add(json);

            if (sessionData.ctx.channel().isWritable()) {
                //log.info("msg : " + msg);
                sessionData.ctx.channel().writeAndFlush(json);
                //return true;
            } else {
                log.info("channel write able is false");
            }

            //sessionData.ctx.writeAndFlush(json);



        }
    }
    private void sendClientJson(UserData userData, JSONObject json) {
        saveClientJson(json);
        if (userData.session == null || !userData.session.isAlive)
            log.info("user no login: " + userData.osnID);
        else
            sendClientJson(userData.session, json);
    }
    private void sendUserJson(String userID, JSONObject json) {
        UserData userData = getUserData(userID);
        if (userData == null)
            sendOsxJson(json);
        else
            sendClientJson(userData, json);
    }
    private Void sendReplyCode(RunnerData runnerData, ErrorData error, JSONObject json) {
        if (runnerData == null){
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
        //data.put("command", "Reply");
        //data.put("ver", "1");
        data.put("id", runnerData.json.getString("id"));
        data.put("errCode", error == null ? "0:success" : error.toString());
        //data.put("crypto", "none");
        if(json != null){
            data.put("content", json.toString());
        }
        //data.put("timestamp", System.currentTimeMillis());
        sendClientJson(sessionData, data);
        return null;
    }
    private Void sendReplyHeart(RunnerData runnerData) {
        JSONObject data = new JSONObject();
        data.put("id", runnerData.json.getString("id"));
        if (runnerData.sessionData.state == 0 || runnerData.sessionData.user == null){
            data.put("errCode", E_needLogin.toString());
        } else {
            data.put("errCode", "0:success");
        }

        sendClientJson(runnerData.sessionData, data);
        return null;
    }
    private ErrorData forwardMessage(SessionData sessionData, JSONObject json) {
        ErrorData error = null;
        try {
            String to = json.getString("to");
            String from = json.getString("from");
            String command = json.getString("command");
            if (isGroup(to)) {
                //GroupService.forwardMessage(sessionData, to, json);

                GroupData groupData = getGroupData(to);
                if (groupData == null) {
                    if (!sessionData.remote) {
                        sendOsxJson(json);
                        return null;
                    }
                    log.info("no find groupID: " + to);
                    return E_groupNoFind;
                }
                forwardGroup(groupData, json);
                return null;
            } /*else if (isUser(to)){
                UserService.forwardMessage(sessionData, to, json);
            }*/
            UserData userData = getUserData(to);
            if (sessionData.remote) {
                if (userData == null) {
                    log.info("no my user: " + to);
                    error = E_userNoFind;
                } else {
                    sendClientJson(userData, json);
                    //log.info("["+command+"]recv remote: " + command + ", from: " + from + " to: " + to);
                }
            } else {
                if (userData == null) {
                    sendOsxJson(json);
                    //log.info("["+command+"]send remote: " + command + ", from: " + from + ", to: " + to);
                } else {
                    sendClientJson(userData, json);
                    //log.info("["+command+"]recv local: " + command + ", from: " + from + ", to: " + to);
                }
            }

            return error;
        } catch (Exception e) {
            error = E_exception;
            log.error("", e);
        }
        return error;
    }

    public static boolean isInThisNode(String osnId){

        UserData userData = IMData.getUserData(osnId);
        if (userData != null){
            return true;
        }
        GroupData groupData = IMData.getGroupData(osnId);
        if (groupData != null){
            return true;
        }

        if (osnId.equalsIgnoreCase(service.osnID)) {
            return true;
        }
        /*LitappData litappData = IMData.getLitappData(osnId);
        if (litappData != null){
            return true;
        }*/
        return false;
    }

    public static boolean hasUser(String osnId){

        if (db.user.readUserByID(osnId) == null) {
            return false;
        }
        return true;
    }



    private void completeMessage(JSONObject json, boolean verify) {
        MessageService.completeMessage(json);
    }
    private void toWebMessage(SessionData sessionData, MessageData messageData) {
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
    private GroupData getMyGroup(SessionData sessionData, JSONObject json) {
        if (sessionData.remote)
            return sessionData.toGroup;
        String groupID = json.getString("to");
        GroupData groupData = getGroupData(groupID);
        if (groupData == null) {
            sendOsxJson(json);
            log.info("forward to groupID: " + groupID + ", command: " + json.getString("command"));
        }
        return groupData;
    }


    private void notifyDynamic(UserData userData, JSONObject data){
        for (String friend : userData.friends) {
            UserData userLocal = getUserData(friend);
            if(userLocal != null){
                updateDynamic(userLocal, data);
            }else {
                JSONObject json = wrapMessageX("DynamicUpdate", userData, friend, data, null);
                sendOsxJson(json);
            }
        }
    }
    private void notifyDynamic(GroupData groupData, JSONObject data){

        for (String member : groupData.members.keySet()) {
            UserData userData = getUserData(member);
            if(userData != null){
                updateDynamic(userData, data);
            }else{
                JSONObject json = makeMessage("DynamicUpdate", groupData.osnID, member, data, groupData.osnKey, null);
                sendOsxJson(json);
            }
        }
    }
    private void updateDynamic(UserData userData, JSONObject data){
        String type = data.getString("type");
        if(type.equalsIgnoreCase("create")){
            PyqList pyqList = new PyqList();
            pyqList.osnID = userData.osnID;
            pyqList.target = data.getString("osnID");
            pyqList.pyqID = data.getLong("pyqID");
            pyqList.createTime = data.getLong("createTime");
            db.insertPyqList(pyqList);
        }
    }

    private void forwardGroup(GroupData groupData, JSONObject json) {
        String userID = json.getString("from");
        log.info("forward group: [" + json.getString("command") + "] groupID: " + groupData.osnID + ", originalUser: " + json.getString("from"));

        // ---- delete by CESHI ---- //
        /*
        for (String u : groupData.userList) {
            if (u.equalsIgnoreCase(userID))
                continue;
            JSONObject pack = packMessage(json, groupData.osnID, u, groupData.osnKey);
            sendUserJson(u, pack);
        }
        */
        // ---- delete by CESHI end ---- //

        // ---- add by CESHI ---- //

        // 1. 首先需要对content进行解密
        // 2. 用groupdata里的aeskey来对content进行加密
        String content = reEncryptContent(json, groupData.aesKey, groupData.osnKey);
        // 3. 将content直接传入进行数据组合
        for (MemberData md : groupData.members.values()) {
            if (md.osnID.equalsIgnoreCase(userID))
                continue;
            JSONObject pack;
            if (md.receiverKey == null){
                pack = packMessage(json, groupData.osnID, md.osnID, groupData.osnKey);
            }
            else if (md.receiverKey.equalsIgnoreCase("")){
                pack = packMessage(json, groupData.osnID, md.osnID, groupData.osnKey);
            }
            else {
                pack = packGroupMessage(json, groupData.osnID, md, groupData.osnKey, content);
            }

            sendUserJson(md.osnID, pack);
        }

        // ---- add by CESHI end ---- //
    }
    private JSONObject getStoreInfo(JSONObject json){
        String userID = json.getString("userID");
        UserData userData = getUserData(userID);
        if(userData == null){
            return null;
        }
        JSONObject data = new JSONObject();
        List<FriendData> friends = db.listFriend(userID);
        if(!friends.isEmpty()){
            data.put("friendCount", friends.size());
            data.put("friendTime0", friends.get(0).createTime);
            data.put("friendTime1", friends.get(friends.size()-1).createTime);
        }
        List<GroupData> groups = db.listGroup(userID, 200);
        if(!groups.isEmpty()){
            data.put("groupCount", groups.size());
            data.put("groupTime0", groups.get(0).createTime);
            data.put("groupTime1", groups.get(groups.size()-1).createTime);
        }
        int msgCount = db.loadMessageStoreCount(userID);
        if(msgCount != 0){
            MessageData msg0 = db.loadMessageStore(userID, 0, false);
            if(msg0 != null){
                MessageData msg1 = db.loadMessageStore(userID, System.currentTimeMillis(), true);
                if(msg1 != null){
                    data.put("msgCount", msgCount);
                    data.put("msgTime0", msg0.createTime);
                    data.put("msgTime1", msg1.createTime);
                }
            }
        }
        return data;
    }
    private JSONObject wrapMessageX(String command, CryptData cryptData, String to, JSONObject data, JSONObject original){
        if(imShare)
            return wrapMessage(command, service.osnID, to, data, service.osnKey, original);
        return wrapMessage(command, cryptData.osnID, to, data, cryptData.osnKey, original);
    }


    public Void Heart(RunnerData runnerData) {
        try {
            PerUtils.heart();
            SessionData sessionData = runnerData.sessionData;

            if (sessionData != null) {
                sessionData.timeHeart = System.currentTimeMillis();

                if (sessionData.user != null) {
                    MessageSyncServer.pushOnlyCache(sessionData.user.osnID);
                }

                return sendReplyHeart(runnerData);
            }





            /*if (sessionData.user != null){
                log.info("Heart : " + sessionData.user.osnID);
            } else {
                log.info("Heart : null");
            }*/



        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void Register(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            if (!appRegisterUser) {
                sendReplyCode(runnerData, E_noSupport, null);
                return null;
            }
            JSONObject data = takeMessage(runnerData.json);
            if (data == null)
                return null;
            String username = data.getString("username");
            String password = data.getString("password");
            if (username == null || password == null) {
                sendReplyCode(runnerData, E_missData, null);
                return null;
            }
            if (db.isRegisterName(username)) {
                log.info("user already exist: " + username);
                sendReplyCode(runnerData, E_userExist, null);
                return null;
            }
            String owner2 = data.getString("owner2");
            UserData userData = OsnAdminServer.RegsiterUser(username, password, owner2, username, null);
            if (userData == null)
                sendReplyCode(runnerData, E_registFailed, null);
            else {
                data.clear();
                data.put("osnID", userData.osnID);
                sendReplyCode(runnerData, null, data);

                pushOsnID(userData);
            }
            log.info("userName: " + username + ", osnID: " + (userData == null ? "null" : userData.osnID));
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


    private Void Login_key(RunnerData runnerData){
        try {

            if (runnerData != null) {
                Void v = Login_key2(runnerData);
                log.info("Login_key2 end.");
                return v;
            }

            //log.info("Login_key begin.");

            JSONObject KickOffData = null;
            SessionData KickOffSession = null;
            //log.info("begin.");
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            sessionData.deviceID = json.getString("deviceID");
            //log.info("device id : " + sessionData.deviceID);
            if(json.getString("ver").equalsIgnoreCase("2"))
                return Login_key2(runnerData);
            String userID = json.getString("user");
            boolean isNameLogin = json.getString("type").equalsIgnoreCase("user");
            UserData userData = isNameLogin ? getUserDataByName(userID) : getUserData(userID);
            if (userData == null) {
                log.info("no register user: " + userID);
                sendReplyCode(runnerData, E_userNoFind, null);
                return null;
            }
            if (!json.containsKey("state")) {
                log.info("need state");
                sendReplyCode(runnerData, E_missData, null);
                return null;
            }
            String state = json.getString("state");
            if (state.equalsIgnoreCase("request")) {
                sessionData.challenge = System.currentTimeMillis();
                sessionData.state = LoginState_Challenge;
                sessionData.deviceID = json.getString("deviceID");
                JSONObject data = new JSONObject();
                data.put("challenge", sessionData.challenge);
                String content = aesEncrypt(data.toString(), isNameLogin ? userData.password : userData.aesKey);
                data.clear();
                data.put("data", content);
                sendReplyCode(runnerData, null, data);

                sessionData.state = 1;
                //sessionMap.put(sessionData.ctx, sessionData);

                //MessageSyncServer.push(userData.osnID);
                log.info("user: " + userID + ", state: " + state);
                return null;
            } else if (state.equalsIgnoreCase("verify")) {
                if (sessionData.state != LoginState_Challenge) {
                    sendReplyCode(runnerData, E_stateError, null);
                    log.info("state is verify,but session state not eq 1");
                    return null;
                }
                String content = aesDecrypt(json.getString("data"), isNameLogin ? userData.password : userData.aesKey);
                JSONObject data = JSON.parseObject(content);
                if (!data.getString("user").equalsIgnoreCase(userID) ||
                        data.getLongValue("challenge") != sessionData.challenge) {
                    sendReplyCode(runnerData, E_verifyError, null);

                    log.info("state is verify, data user not eq userID");
                    return null;
                }
                sessionData.state = LoginState_Finish;

                JSONObject rData = new JSONObject();
                rData.put("aesKey", userData.aesKey);
                rData.put("msgKey", userData.msgKey);
                rData.put("osnID", userData.osnID);
                rData.put("osnKey", userData.osnKey);
                rData.put("serviceID", service.osnID);



                // 断开session
                if (userData.session != null) {
                    log.info("device : " + userData.session.deviceID);
                    log.info("login device : " + sessionData.deviceID);
                    if (userData.session.deviceID != null && sessionData.deviceID != null &&
                            !sessionData.deviceID.equalsIgnoreCase(userData.session.deviceID)) {

                        String ctxInfo = "" + userData.session.ctx;
                        if (!ctxInfo.contains("0.0.0.0")) {
                            JSONObject dataKick = makeMessage("KickOff", service.osnID, userID, "{}", userData.osnKey, null);
                            KickOffSession = userData.session;
                            KickOffData = dataKick;
                            //sendClientJson(userData.session, data);

                            log.info("KickOff userID: " + userID + ", deviceID: " + userData.session.deviceID);
                        } else {
                            log.info(ctxInfo);
                        }

                    }
                    //sessionMap.remove(userData.session.ctx);
                    //delSessionData(userData.session);
                    userData.session = null;
                }




                if (userMap.containsKey(userData.osnID)) {
                    userMap.remove(userData.osnID);
                }
                userMap.put(userData.osnID, userData);

                sessionData.state = 1;
                //sessionMap.put(sessionData.ctx, sessionData);
                synchronized (userLock) {
                    userData.session = sessionData;
                }
                sessionData.user = userData;
                sessionData.fromUser = userData;
                sessionData.timeHeart = System.currentTimeMillis();
                userData.loginTime = sessionData.timeHeart;
                //db.updateUser(userData, Collections.singletonList("loginTime"));

                data.clear();
                if(userData.logoutTime == 0){
                    userData.logoutTime = System.currentTimeMillis();
                }
                rData.put("logoutTime", userData.logoutTime);
                data.put("data", aesEncrypt(rData.toString(), isNameLogin ? userData.password : userData.aesKey));

                sendReplyCode(runnerData, null, data);
                log.info("user: " + userData.osnID + ", name: " + userData.name);


                if (KickOffData != null && KickOffSession != null) {
                    sendClientJson(KickOffSession, KickOffData);
                    delSessionData(KickOffSession);
                }



                //MessageSyncServer.push(userData.osnID);
            } else {
                sendReplyCode(runnerData, E_stateError, null);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private JSONObject genLoginData(RunnerData runnerData){

        try {
            String type = runnerData.json.getString("type");
            if (type == null){
                log.info("error, type null");
                return null;
            }
            boolean isNameLogin = type.equalsIgnoreCase("user");
            String challenge = runnerData.json.getString("challenge");
            if (challenge == null){
                log.info("error, challenge null");
                return null;
            }
            UserData userData = runnerData.sessionData.user;
            if (userData == null) {
                return null;
            }
            challenge = aesDecrypt(challenge, isNameLogin ? userData.password : userData.aesKey);

            JSONObject data = new JSONObject();
            data.put("aesKey", userData.aesKey);
            data.put("osnID", userData.osnID);
            data.put("osnKey", userData.osnKey);
            data.put("serviceID", service.osnID);
            long cha = Long.parseLong(challenge)+1;
            data.put("challenge", cha);

            JSONObject result = new JSONObject();
            result.put("data", aesEncrypt(data.toString(), isNameLogin ? userData.password : userData.aesKey));

            return result;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }




    private Void Login_key2(RunnerData runnerData){

        Jedis loginJedis = null;

        try {

            if (runnerData == null) {
                log.info("runnerData null");
                return null;
            }


            JSONObject json = runnerData.json;


            SessionData sessionData = runnerData.sessionData;
            if (sessionData.state == 1) {

                boolean isNameLogin = json.getString("type").equalsIgnoreCase("user");

                if (isNameLogin) {

                    String loginToken = json.getString("token");
                    String userID = json.getString("user");

                    log.info("login begin. user : " + userID);

                    UserData userData = getUserDataByName(userID);
                    userData.token = loginToken;

                    if (loginJedis == null) {
                        log.info("get jedis begin.");
                        loginJedis = db.message.getJedis();
                        log.info("get jedis end.");
                        if (loginJedis == null) {
                            log.info("error, jedis is null. ");
                            sendReplyCode(runnerData, E_dataBase, null);
                            return null;
                        }
                    }

                    // 更新token
                    db.conversation.setLoginToken(loginJedis, userData.osnID, loginToken);

                    if (loginJedis != null) {
                        log.info("close jedis");
                        db.message.closeJedis(loginJedis);
                        loginJedis = null;
                    }

                }


                //logInfo("[Login_key2] error, state == 1");
                JSONObject replyData = genLoginData(runnerData);
                sendReplyCode(runnerData, null, replyData);
                log.info("login end. state == 1");
                return null;
            }






            String userID = json.getString("user");
            boolean isNameLogin = json.getString("type").equalsIgnoreCase("user");
            log.info("login begin. user : " + userID + " login type : " + json.getString("type"));



            //sessionData.deviceID = json.getString("deviceID");


            //logInfo("login user : " + userID);


            UserData userData = isNameLogin ? getUserDataByName(userID) : getUserData(userID);
            if (userData == null) {
                log.info("no register user: " + userID);
                sendReplyCode(runnerData, E_userNoFind, null);
                return null;
            }

            String loginToken = json.getString("token");
            if (!isNameLogin) {
                // check token
                if (loginToken == null) {
                    log.info("no token 1");
                    sendReplyCode(runnerData, E_tokenError, null);
                    return null;
                }
                /*if (userData.token == null) {
                    logInfo("no token 2");
                    sendReplyCode(runnerData, E_tokenError, null);
                    return null;
                }*/

                String userToken = userData.token;
                if (userToken == null) {

                    if (loginJedis == null) {

                        //log.info("get redis.");
                        loginJedis = db.message.getJedis();
                        if (loginJedis == null) {
                            log.info("error, jedis is null. ");
                            sendReplyCode(runnerData, E_dataBase, null);
                            return null;
                        }
                    }
                    userToken = db.conversation.getLoginToken(loginJedis, userData.osnID);
                    if (loginJedis != null) {
                        //log.info("close redis.");
                        db.message.closeJedis(loginJedis);
                        loginJedis = null;
                    }
                    if (userToken == null) {
                        log.info("no token 2");
                        sendReplyCode(runnerData, E_tokenError, null);
                        return null;
                    }

                }


                if (!loginToken.equalsIgnoreCase(userToken)) {
                    log.info("no token 3 : " + userToken);
                    sendReplyCode(runnerData, E_tokenError, null);
                    return null;
                }
            }




            String challenge = json.getString("challenge");
            if (challenge == null){
                log.info("error, input format error, no challenge.");
                sendReplyCode(runnerData, E_formatError, null);
                return null;
            }
            challenge = aesDecrypt(challenge, isNameLogin ? userData.password : userData.aesKey);
            if (challenge == null){
                log.info("password error: " + userID);
                sendReplyCode(runnerData, E_verifyError, null);
                return null;
            }
            challenge = String.valueOf(Long.parseLong(challenge)+1);

            //logInfo("challenge : " + challenge);



            JSONObject data = new JSONObject();
            data.put("aesKey", userData.aesKey);
            //data.put("msgKey", userData.msgKey);
            data.put("osnID", userData.osnID);
            data.put("osnKey", userData.osnKey);
            data.put("serviceID", service.osnID);
            data.put("challenge", challenge);



            if (userData.session != null) {

                /*logInfo("device : " + userData.session.deviceID);
                logInfo("login device : " + sessionData.deviceID);
                logInfo("ctx : " + userData.session.ctx.name());
                logInfo("new ctx : " + runnerData.sessionData.ctx.name());
                logInfo("ctx : " + userData.session.ctx);
                logInfo("new ctx : " + runnerData.sessionData.ctx);*/



                // 判断 老session 和新session 是否相同
                if (userData.session.ctx == runnerData.sessionData.ctx){
                    log.info("[Login_key2] same session");
                } else {
                    // kick off
                    //logInfo("1");

                    if (userData.session.deviceID != null) {
                        if (sessionData.deviceID != null) {
                            if (!sessionData.deviceID.equalsIgnoreCase(userData.session.deviceID)) {

                                String ctxInfo = "" + userData.session.ctx;
                                if (!ctxInfo.contains("0.0.0.0")) {

                                    //logInfo("2");
                                    //logInfo("userID : " + userID);
                                    //logInfo("service.osnID : " + service.osnID);
                                    //logInfo("service.osnKey : " + service.osnKey);
                                    JSONObject dataKick = makeMessage(
                                            "KickOff",
                                            service.osnID,
                                            userData.osnID,
                                            "{}",
                                            service.osnKey,
                                            null);
                                    //logInfo(data.toString());
                                    sendClientJson(userData.session, dataKick);

                                    log.info("KickOff userID: " + userID + ", deviceID: " + userData.session.deviceID);

                                } else {
                                    log.info(ctxInfo);
                                }


                            }
                        }
                    }

                    /*if (userData.session.deviceID != null && sessionData.deviceID != null &&
                            !sessionData.deviceID.equalsIgnoreCase(userData.session.deviceID)) {
                        data = makeMessage("KickOff", service.osnID, userID, "{}", userData.osnKey, null);
                        sendClientJson(userData.session, data);

                        logInfo("KickOff userID: " + userID + ", deviceID: " + userData.session.deviceID);
                    }*/

                    //logInfo("6");

                    // 删除老sesion
                    //logInfo("[Login_key2] del session :" + userData.session.ctx.hashCode());
                    delSessionData(userData.session);


                }




            }


            //logInfo("7");



            sessionData.state = 1;
            //sessionMap.put(sessionData.ctx, sessionData);
            // 替换user的session
            if (isNameLogin) {

                userData.token = loginToken;

                if (loginJedis == null) {
                    //log.info("get redis.");
                    loginJedis = db.message.getJedis();
                    if (loginJedis == null) {
                        log.info("error, jedis is null. ");
                        sendReplyCode(runnerData, E_dataBase, null);
                        return null;
                    }
                }
                db.conversation.setLoginToken(loginJedis, userData.osnID, loginToken);

                if (loginJedis != null) {
                    //log.info("close redis.");
                    db.message.closeJedis(loginJedis);
                    loginJedis = null;
                }

            }

            //userData.token = loginToken;


            userData.session = sessionData;
            //logInfo("[Login_key2] user : " +userID+ " new session : " + userData.session.ctx.hashCode());
            // 绑定session中的user
            sessionData.user = userData;
            sessionData.fromUser = userData;
            sessionData.timeHeart = System.currentTimeMillis();
            userData.loginTime = sessionData.timeHeart;

            //logInfo("8");

            if (!userMap.containsKey(userData.osnID)) {
                userMap.put(userData.osnID, userData);
            }


            JSONObject result = new JSONObject();

            result.put("data", aesEncrypt(data.toString(), isNameLogin ? userData.password : userData.aesKey));
            if (isNameLogin){
                log.info("password login success. user : " + userID);
                //log.info("password : " + userData.password);
                //log.info("data : " + data);
                //log.info("EncData : " + result);
            }

            sendReplyCode(runnerData, null, result);
            //log.info("user: " + userData.osnID + ", name: " + userData.name);

            MessageSyncServer.push(userData.osnID);


        } catch (Exception e) {
            sendReplyCode(runnerData, E_exception, null);
            //logError(e);
            log.info("error json : " + runnerData.json);
        }

        if (loginJedis != null) {
            db.message.closeJedis(loginJedis);
        }
        return null;
    }

    private Void Login_share(RunnerData runnerData){
        try {

            log.info("Login_share begin.");

            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String userID = json.getString("user");
            String userName = json.getString("name");
            if(userID == null) {
                log.info("miss osnID");
                return sendReplyCode(runnerData, E_missData, null);
            }
            UserData userData = getUserData(userID);
            if(userData == null){
                userData = new UserData();
                userData.osnID = userID;
                userData.osnKey = "";
                userData.name = userName;
                userData.password = "";
                userData.aesKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
                userData.msgKey = Base64.getEncoder().encodeToString(sha256(String.valueOf(System.currentTimeMillis()).getBytes()));
                userData.displayName = userName;
                //userData.urlSpace = urlSpace;
                if (!db.insertUserShare(userData)){
                    log.info("insertUserShare failed");
                    return sendReplyCode(runnerData, E_dataBase, null);
                }
            }

            userMap.put(userData.osnID, userData);
            if (userData.session != null && userData.session != sessionData) {
                if (userData.session.deviceID != null && sessionData.deviceID != null &&
                        !sessionData.deviceID.equalsIgnoreCase(userData.session.deviceID)) {
                    JSONObject data = makeMessage("KickOff", service.osnID, userID, "{}", userData.osnKey, null);
                    sendClientJson(userData.session, data);

                    log.info("KickOff userID: " + userID + ", deviceID: " + userData.session.deviceID);
                }
                delSessionData(userData.session);
            }
            sessionData.state = 1;
            //sessionMap.put(sessionData.ctx, sessionData);
            synchronized (userData) {
                userData.session = sessionData;
            }
            sessionData.user = userData;
            sessionData.fromUser = userData;
            sessionData.timeHeart = System.currentTimeMillis();
            userData.loginTime = sessionData.timeHeart;
            //db.updateUser(userData, Collections.singletonList("loginTime"));

            JSONObject data = new JSONObject();
            data.put("serviceID", service.osnID);
            data.put("logoutTime", userData.logoutTime);

            sendReplyCode(runnerData, null, data);
            log.info("user: " + userData.osnID + ", name: " + userData.name);

            pushOsnID(userData);

            //MessageSyncServer.push(userData.osnID);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    public Void Login(RunnerData runnerData) {
        PerUtils.login();
        return imShare ? Login_share(runnerData) : Login_key(runnerData);
    }
    private Void Logout(RunnerData runnerData){
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String userID = json.getString("from");

            delSessionData(sessionData);
            UserData userData = getUserData(userID);
            if(userData != null){

                log.info("[Logout] " + userID);
                userData.logoutTime = System.currentTimeMillis();
                userData.session = null;

                //delUserData(userData);
                //popOsnID(userID);
            }
            sendReplyCode(runnerData, null, null);

        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public Void GetLoginInfo(RunnerData runnerData) {
        return DappService.GetLoginInfo(runnerData);
    }

    public Void LoginByOsnID(RunnerData runnerData) {
        return DappService.LoginByOsnID(runnerData);
    }



    private Void SetMessage(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            if(!msgDelete){
                log.info("no support userID: " + sessionData.fromUser.osnID);
                return sendReplyCode(runnerData, E_noSupport, null);
            }
            GroupData groupData = null;
            JSONObject json = runnerData.json;
            String target = json.getString("to");
            if(isGroup(target)){
                groupData = getMyGroup(sessionData, runnerData.json);
                if (groupData == null)
                    return null;
            }
            JSONObject data = takeMessage(json);
            assert data != null;
            String hash = data.getString("messageHash");
            log.info("start delete "+(groupData==null?"user":"group")+" message: "+hash);
            MessageData messageData = db.queryMessage(hash);
            if(messageData == null){
                return sendReplyCode(runnerData, E_dataNoFind, null);
            }
            if(!messageData.fromID.equalsIgnoreCase(sessionData.fromUser.osnID)){
                return sendReplyCode(runnerData, E_noRight, null);
            }
            if(!db.deleteMessage(hash)){
                return sendReplyCode(runnerData, E_dataBase, null);
            }
            log.info("delete message success: "+hash);
            if(sessionData.remote){
                sendClientJson(sessionData.toUser, json);
            } else {
                json = packMessage(json, sessionData.fromUser.osnID, target, sessionData.fromUser.osnKey);
                sendUserJson(target, json);
                sendReplyCode(runnerData, null, null);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private Void MessageLoad(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = runnerData.data;
            String userID = json.getString("from");
            String target = data.getString("userID");
            int count = data.getInteger("count");
            boolean before = data.getBoolean("before");
            long timeStamp = data.containsKey("timestamp") ? data.getLong("timestamp") : System.currentTimeMillis();
            if (count > 100) {
                sendReplyCode(runnerData, E_dataOverrun, null);
                return null;
            }

            List<MessageData> messageInfos = db.loadMessages(userID, target, timeStamp, before, count);
            JSONArray array = new JSONArray();
            for (MessageData messageData : messageInfos) {
                toWebMessage(sessionData, messageData);
                array.add(messageData.data);
            }
            data.clear();
            data.put("msgList", array);
            sendReplyCode(runnerData, null, data);

            log.info("userID: " + userID + ", target: " + target + ", timestamp: " + timeStamp + ", before: " + before + ", count: " + count + ", result: " + array.size());
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


    private Void Complete(RunnerData runnerData) {
        sendReplyCode(runnerData, null, null);
        MessageProcessServer.push(runnerData);
        return null;
        //return MessageService.complete(runnerData);
    }


    private Void WaitReceipt(RunnerData runnerData) {
        try {
            JSONObject json = runnerData.json;
            String hash = json.getString("hash");
            MessageData messageData = db.queryReceipt(hash);
            if (messageData == null) {
                json = takeMessage(json);
                runnerData.json = json;
                handleMessage(runnerData.sessionData, json);
            } else {
                JSONObject data = takeMessage(json);
                if (data == null)
                    return null;
                completeMessage(data, false);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void Forwarder(RunnerData runnerData) {
        forwardMessage(runnerData.sessionData, runnerData.json);
        return null;
    }
    private Void GetConversationList(RunnerData runnerData) {
        try {
            JSONObject json = runnerData.json;
            List<String> conversationList = db.conversation.list(runnerData.sessionData.user.osnID);
            JSONObject data = new JSONObject();
            data.put("conversationList", conversationList);
            JSONObject result = makeMessage("ConversationList", service.osnID, runnerData.sessionData.fromUser.osnID, data, service.osnKey, json);
            sendClientJson(runnerData.sessionData, result);

            log.info("userID: " + json.getString("from") + ", conversationList: " + conversationList.size());
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void GetConversationInfo(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject data = runnerData.data;
            String target = data.getString("target");
            String info = db.conversation.get(sessionData.user.osnID, target);
            if (info == null){
                sendReplyCode(runnerData, E_dataNoFind, null);
            } else {
                data = new JSONObject();
                data.put("info", info);
                sendReplyCode(runnerData, null, data);
            }
            log.info("userID: " + sessionData.user.osnID + ", target: " + target
                    + ", info: " + (info != null ? info.length() : 0));
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void SetConversationInfo(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject data = runnerData.data;
            String target = data.getString("target");
            String info = data.getString("info");
            if(target == null){
                return sendReplyCode(runnerData, E_missData, null);
            }
            ErrorData error = null;
            if(info == null){
                if(!db.conversation.del(sessionData.user.osnID, target)){
                    error = E_dataBase;
                }
            } else {
                if (!db.conversation.set(sessionData.user.osnID, target, info)){
                    error = E_dataBase;
                }
            }
            sendReplyCode(runnerData, error, null);

            log.info("userID: " + sessionData.user.osnID + ", target: " + target + ", info: " + info);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void GetRequestList(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = runnerData.data;
            long timestamp = data.getLong("timestamp");
            int count = data.getIntValue("count");
            if (count > 20)
                count = 20;
            List<String> requests = new ArrayList<>();
            List<MessageData> requestList = db.loadRequest(sessionData.fromUser.osnID, timestamp, count);
            for (MessageData request : requestList) {
                toWebMessage(sessionData, request);
                json = new JSONObject();
                json.put("state", request.state);
                json.put("request", request.data);
                requests.add(json.toString());
            }
            data = new JSONObject();
            data.put("requestList", requests);
            sendReplyCode(runnerData, null, data);

            log.info("userID: " + json.getString("from") + ", timestamp: " + timestamp + ", count: " + count + ", size: " + requestList.size());
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


    private Void Result(RunnerData runnerData){
        return SearchService.Result(runnerData);
    }




    private Void GetServiceInfo(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String userID = json.getString("from");
            String serviceID = json.getString("to");

            if (serviceID == null)
                return null;

            //log.info("serviceID: " + serviceID + ", from: " + userID + ", remote: " + sessionData.remote);

            JSONObject result = null;
            if (serviceID.equalsIgnoreCase(service.osnID)) {
                JSONObject info = new JSONObject();
                info.put("type", "IMS");
                info.put("urlSpace", urlSpace);
                result = wrapMessage("ServiceInfo", serviceID, userID, info, service.osnKey, json);
            } else {
                LitappData litappData = getLitappData(serviceID);
                if (litappData != null) {
                    log.info("litapp name: " + litappData.name);
                    result = wrapMessage("ServiceInfo", serviceID, userID, litappData.toJson(), litappData.osnKey, json);
                } else
                    log.info("no my serviceID: " + serviceID);
            }

            if (result != null) {
                if (sessionData.remote)
                    sendOsxJson(result);
                else
                    sendClientJson(sessionData, result);
            } else {
                if (!sessionData.remote)
                    sendOsxJson(json);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void FindOsnID(RunnerData runnerData) {
        SessionData sessionData = runnerData.sessionData;
        JSONObject json = runnerData.json;
        JSONArray findedList = new JSONArray();
        JSONArray targetList = json.getJSONArray("targetList");
        String timeStamp = json.getString("timeStamp");
        if (timeStamp == null) {
            log.error("miss timeStamp");
            return null;
        }
        //log.info("recv targetList: "+targetList.toString());

        for (Object o : targetList) {
            String osnID = (String) o;
            if (isUser(osnID)) {
                UserData userData = getUserData(osnID);
                if (userData != null) {
                    JSONObject data = new JSONObject();
                    data.put("osnID", userData.osnID);
                    if(imShare){
                        JSONObject cert = userData.getUserCert();
                        if (cert != null) {
                            data.put("userCert", cert.toString());
                            findedList.add(data);
                        } else {
                            log.info("user no cert: " + userData.name);
                        }
                    } else {
                        data.put("sign", ECUtils.osnSign(userData.osnKey, timeStamp.getBytes()));
                        findedList.add(data);
                    }
                }
            } else {
                CryptData cryptData = null;
                if (isGroup(osnID)) {
                    cryptData = getGroupData(osnID);
                } else if (isService(osnID)) {
                    if (osnID.equalsIgnoreCase(service.osnID))
                        cryptData = service;
                    if (cryptData == null)
                        cryptData = getLitappData(osnID);
                }
                if (cryptData != null) {
                    JSONObject data = new JSONObject();
                    data.put("osnID", cryptData.osnID);
                    data.put("sign", ECUtils.osnSign(cryptData.osnKey, timeStamp.getBytes(StandardCharsets.UTF_8)));
                    findedList.add(data);
                }
            }
        }
        if (!findedList.isEmpty()) {
            JSONObject data = new JSONObject();
            data.put("command", "haveOsnID");
            data.put("ip", json.getString("ip"));
            data.put("timeStamp", timeStamp);
            data.put("querySign", json.getString("querySign"));
            data.put("targetList", findedList);
            sendOsxJson(data);

            log.info("send findedList: " + findedList + ", ip: " + json.getString("ip"));
        }
        return null;
    }
    private Void Broadcast(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = takeMessage(json);
            if (data == null)
                return null;
            String userID = json.getString("from");
            String type = data.getString("type");
            log.info("type: "+type+", userID: "+userID);
            switch(type){
                case "help":
                    String text = data.getString("text");
                    log.info("help info: "+text);
                    resetHelp();
                    if (!sessionData.remote) {
                        sendOsxJson(json);
                        sendReplyCode(runnerData, null, null);
                        ++helpOut;
                    } else {
                        ++helpIn;
                    }
                    break;
                case "getStoreInfo":
                    getStoreInfo(data);
                    break;
                case "findService":
                    sendReplyCode(runnerData, null, null);

                    List<LitappData> serviceInfos = new ArrayList<>();
                    String name = data.getString("keyword");
                    if(name != null){
                        List<LitappData> litappList = db.listLitapp();
                        for(LitappData litappData : litappList){
                            if(litappData.name.contains(name)){
                                serviceInfos.add(litappData);
                            }
                        }
                        if(!serviceInfos.isEmpty()){
                            data = new JSONObject();
                            data.put("type", "infos");
                            data.put("litapps", serviceInfos);
                            data = wrapMessage("ServiceInfo", service.osnID, userID, data, service.osnKey, json);
                            sendUserJson(userID, data);
                        }
                    }
                    break;
            }
        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }
    private Void UserCert(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject cert = takeMessage(json);
            if(cert != null){
                UserData userData = sessionData.fromUser;
                userData.setUserCert(cert);
                log.info("timestamp: "+cert.getString("timestamp"));
                if(!userData.shareSync){
                    userData.shareSync = true;
                    JSONObject data = new JSONObject();
                    data.put("type", "getStoreInfo");
                    data.put("userID", userData.osnID);
                    data = wrapMessage("Broadcast", service.osnID, null, data, service.osnKey, null);
                    sendOsxJson(data);
                }
            }
            sendReplyCode(runnerData, null, null);
        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }
    private Void OrderPay(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = takeMessage(json);
            data.clear();
            data.put("unpackID", String.valueOf(System.currentTimeMillis()));
            data.put("code", 0);
            data.put("msg", "");
            log.info("order info: "+data.toString());
            sendReplyCode(runnerData, null, data);
        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }
    private Void GetRedPacket(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            JSONObject data = takeMessage(json);
            log.info("data: "+data.toString());
            JSONObject result = new JSONObject();
            String owner = json.getString("to");
            String userID = json.getString("from");
            UserData userData = getUserData(owner);
            result.put("user", owner);
            result.put("fetcher", userID);
            result.put("price", "100");
            result.put("packetID", data.getString("packetID"));
            result.put("unpackID", data.getString("unpackID"));
            result.put("timestamp", System.currentTimeMillis());
            log.info("packet info: "+result.toString());
            data = makeMessage("UnpackUpdate", owner, userID, result, userData.osnKey, json);
            sendClientJson(sessionData, data);
        } catch (Exception e){
            log.error("", e);
        }
        return null;
    }
    private Void Dynamic(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String userID = json.getString("from");
            String osnID = json.getString("to");
            if(osnID == null){
                sendReplyCode(runnerData, E_missData, null);
                return null;
            }
            UserData userData = null;
            GroupData groupData = null;
            if(isGroup(osnID)){
                groupData = getMyGroup(sessionData, json);
                if (groupData == null) {
                    sendReplyCode(runnerData, null, null);
                    return null;
                }
                if(!groupData.hasMember(userID)){
                    sendReplyCode(runnerData, E_noRight, null);
                    return null;
                }
            }else{
                userData = getUserData(userID);
                if(userData == null){
                    sendReplyCode(runnerData, E_userNoFind, null);
                    return null;
                }
            }
            JSONObject data = takeMessage(json);
            if (data == null) {
                sendReplyCode(runnerData, E_cryptError, null);
                return null;
            }
            PyqData pyqData = new PyqData();
            pyqData.osnID = userID;
            pyqData.groupID = osnID;
            pyqData.pyqID = System.currentTimeMillis();
            pyqData.pyqType = data.getIntValue("pyqType");
            pyqData.pyqText = data.getString("pyqText");
            pyqData.pyqPicture = data.getString("pyqPicture");
            pyqData.pyqWebUrl = data.getString("pyqWebUrl");
            pyqData.pyqWebText = data.getString("pyqWebText");
            pyqData.pyqWebPicture = data.getString("pyqWebPicture");
            pyqData.pyqPlace = data.getString("pyqPlace");
            pyqData.pyqSyncTime = pyqData.pyqID;
            pyqData.createTime = pyqData.pyqID;
            if(!db.insertPyqData(pyqData)){
                sendReplyCode(runnerData, E_dataBase, null);
                return null;
            }
            log.info("user: "+userID+", type: " + pyqData.pyqType + "["+(isGroup(osnID)?"G]":"U]"));
            JSONObject dynamic = new JSONObject();
            dynamic.put("type", "create");
            dynamic.put("osnID", userID);
            dynamic.put("pyqID", pyqData.pyqID);
            dynamic.put("createTime", pyqData.createTime);
            if(groupData != null){
                notifyDynamic(groupData, dynamic);
            }else{
                notifyDynamic(userData, dynamic);
            }
            PyqList pyqList = new PyqList();
            pyqList.osnID = userID;
            pyqList.target = userID;
            pyqList.pyqID = pyqData.pyqID;
            pyqList.createTime = pyqData.createTime;
            db.insertPyqList(pyqList);
            sendReplyCode(runnerData, null, dynamic);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
    private Void DynamicUpdate(RunnerData runnerData) {
        try {
            SessionData sessionData = runnerData.sessionData;
            JSONObject json = runnerData.json;
            String from = json.getString("from");
            if(isGroup(from)){
                runnerData.data = takeMessage(json);
                if(runnerData.data != null)
                    updateDynamic(sessionData.toUser, runnerData.data);
            }else{
                if(imShare){
                    log.info("share no support user dynamic");
                    return null;
                }
                runnerData.data = takeMessage(json);
                if(runnerData.data != null)
                    updateDynamic(sessionData.toUser, runnerData.data);
            }
            if(sessionData.remote)
                sendClientJson(sessionData.toUser, json);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public ErrorData sendNotify(JSONObject json) {
        String to = json.getString("to");
        String from = json.getString("from");
        log.info("from: " + from + ", to: " + to);
        sendUserJson(to, json);
        return null;
    }


    private Void MessageForward(RunnerData runnerData) {

        if (runnerData == null) {
            return null;
        }

        if (runnerData.json == null) {
            return null;
        }

        try {

            MsgData msg = new MsgData(runnerData.json);


            if (isUser(msg.to)){
                return UserService.handleMessage(runnerData);
            }
            if (isGroup(msg.to)){
                //log.info("command : " + msg.command);
                return GroupService.handleMessage(runnerData);
            }
            if (isService(msg.to)){
                //log.info("command : " + msg.command);
                return DappService.handleMessage(runnerData);
            }
            log.info("[MessageForward] unknown account type : " + msg.to);
        } catch (Exception e) {
            log.error("", e);
            log.info("[MessageForward] error json : " + runnerData.json);
        }


        return null;
    }



    public static boolean isInThisNodeContainTemp(String osnId) {

        if (isInThisNode(osnId)) {
            return true;
        }

        // 如果是result，也要放过
        if (TempAccountService.getAccount(osnId) != null) {
            return true;
        }

        return false;
    }

    public void handleMessage(SessionData sessionData, JSONObject json) {

        try {

            String command = json.getString("command");
            log.info("command = " + command);


            //PerUtils.handle();
            /*String id = json.getString("id");
            if (id == null) {
                id = "0";
            }*/
            // 分发到不同的队列中

            RunnerData runnerData = new RunnerData(sessionData, json);
            if (CommandGroupImportantServer.match(runnerData)){
                //log.info("forward to group. ");
                return;
            }


            if (CommandLoginServer.match(runnerData)){
                return;
            }


            if (CommandUserImportantServer.match(runnerData)){
                //log.info("forward to user important. ");
                return;
            }


            if (MessageProcessServer.match(runnerData)){
                //log.info("forward to complete.");
                sendReplyCode(runnerData, null, null);
                return;
            }

            /*if (ResultServer.match(runnerData)){
                //log.info("forward to result.");
                return;
            }*/


            //sendReplyCode(runnerData, null, null);
            //log.info("[handleMessage] 5 id : " + id);



            CommandWorkerServer.push(runnerData);


        } catch (Exception e) {
            log.info(e.getMessage());
        }

    }


}
