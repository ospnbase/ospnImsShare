package com.ospn.utils.db;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.Constant;
import com.ospn.data.MessageData;
import com.ospn.utils.PerUtils;
import com.ospn.utils.RedisUtils;
import com.ospn.utils.data.RedisMessageInfo;
import com.ospn.utils.data.RedisMessageState;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

import static com.ospn.OsnIMServer.db;
import static com.ospn.data.MessageData.toMessageData;

@Slf4j
public class DBMessage extends DBBase {

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_message " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            "cmd char(64) NOT NULL," +
            "fromID char(128) NOT NULL," +
            "toID char(128) NOT NULL," +
            "state tinyint DEFAULT 0," +
            "timeStamp bigint NOT NULL," +
            "hash char(128) NOT NULL," +
            "hash0 char(128)," +
            "data text NOT NULL,"+
            "createTime bigint," +
            "UNIQUE(hash)," +
            "KEY `toID` (`toID`) USING BTREE" +
            ")";

    public static String MQCode = "msgMQ";
    public static String MQComplete = "msgComplete";
    public static String MQOSX = "osxMsgMQ";
    public static String MSG = "MSG";
    public static String MSGMAP = "MSGMAP";
    public static long deltaTime = 3600000;
    static Jedis globalJedis = null;

    RedisUtils redis;
    public DBMessage(ComboPooledDataSource data, RedisUtils data2){
        super(data);
        redis = data2;


    }

    public void setSaveTime(long saveTime){
        deltaTime = saveTime;
        log.info("[DBMessage] redis save time : " + deltaTime);
    }


    public boolean insertMessagesInRedis(List<JSONObject> msgs, String MQ) {
        Jedis jedis = redis.getJedis();
        if (jedis == null){
            log.info("[insertMessagesInRedis] error: jedis == null");
            return false;
        }
        if (msgs.size() == 0){
            log.info("[insertMessagesInRedis] error: msgs.size() == 0");
            return false;
        }
        boolean ret = false;

        try
        {
            for (JSONObject msg : msgs){
                // 这里每一条都要转
                MessageData messageData = toMessageData(msg, 0);
                JSONObject data = MessageData2JSONObject(messageData);
                insertMessage(data, jedis, MQ);
            }
            ret = true;
        } catch (Exception e){
            log.info("[insertMessagesInRedis] error: "+e.getMessage());
            ret = false;
        }

        jedis.close();
        return ret;
    }


    public boolean insertMessagesInRedis(Jedis jedis, List<JSONObject> msgs, String MQ) {

        if (jedis == null) {
            log.info("error, jedis is null.");
            return false;
        }

        if (MQ == null) {
            log.info("error, MQ is null.");
            return false;
        }

        boolean ret = false;

        try
        {
            for (JSONObject msg : msgs){
                // 这里每一条都要转
                if (msg == null) {
                    log.info("error, msg is null.");
                    continue;
                }
                MessageData messageData = new MessageData(msg, 0);
                if (messageData.data == null) {
                    log.info("error, MessageData data is null.");
                    log.info("error msg : " + msg);
                }
                JSONObject data = messageData.toJson();
                if (data == null) {
                    log.info("error, data is null.");
                    log.info("error msg : " + msg);
                    continue;
                }
                insertMessage(data, jedis, MQ);
            }
            ret = true;
        } catch (Exception e){
            log.info("[insertMessagesInRedis] error: "+e.getMessage());
            log.info("error msgs : " + msgs);
            ret = false;
        }

        return ret;
    }


    public boolean insertInRedis(MessageData messageData){
        JSONObject msg = MessageData2JSONObject(messageData);
        if (msg == null){
            log.info("[insertInRedis] error: msg == null");
            return false;
        }
        return insertInRedis(msg);
    }

    public boolean insertInRedis(MessageData messageData, String MQ){
        JSONObject msg = MessageData2JSONObject(messageData);
        if (msg == null){
            log.info("[insertInRedis] error: msg == null");
            return false;
        }
        //insertMessage(data, jedis, MQ);
        return insertInRedis(msg, MQ);
    }

    private boolean insertInRedis(JSONObject msg){
        Jedis jedis = redis.getJedis();
        if (jedis == null){
            log.info("[insertInRedis] error: jedis == null");
            return false;
        }
        boolean ret = true;

        try
        {
            insertMessage(msg, jedis, MQCode);

        }catch (Exception e){
            log.info("[insertInRedis] error：" + e.getMessage());
            ret = false;
        }
        redis.close(jedis);
        return ret;
    }

    private boolean insertInRedis(JSONObject msg, String MQ){
        Jedis jedis = redis.getJedis();
        if (jedis == null){
            log.info("[insertInRedis] error: jedis == null");
            return false;
        }
        boolean ret = true;

        try
        {
            insertMessage(msg, jedis, MQ);

        }catch (Exception e){
            log.info("[insertInRedis] error：" + e.getMessage());
            ret = false;
        }
        redis.close(jedis);
        return ret;
    }

    public boolean insertOsxInRedis(MessageData messageData){
        JSONObject msg = MessageData2JSONObject(messageData);
        if (msg == null){
            log.info("[insertOsxInRedis] error: msg == null");
            return false;
        }
        return insertOsxInRedis(msg);
    }

    private boolean insertOsxInRedis(JSONObject msg){

        Jedis jedis = redis.getJedis();
        if (jedis == null){
            log.info("[insertInRedis] error: jedis == null");
            return false;
        }
        boolean ret = true;

        try
        {
            insertMessage(msg, jedis, MQOSX);



        }catch (Exception e){
            log.info("[insertInRedis] error：" + e.getMessage());
            ret = false;
        }
        redis.close(jedis);
        return ret;
    }

    public boolean insertCompleteInRedis(MessageData messageData){
        JSONObject msg = MessageData2JSONObject(messageData);
        if (msg == null){
            log.info("[insertCompleteInRedis] error: msg == null");
            return false;
        }
        return insertCompleteInRedis(msg);
    }

    private boolean insertCompleteInRedis(JSONObject msg){

        // 完成以后直接丢弃，不再存储
        return true;

    }



    public List<MessageData> listFromRedis(String userID) {

        List<MessageData> msgs = new ArrayList<MessageData>();

        Jedis jedis = redis.getJedis();
        if (jedis == null) {
            log.info("[listFromRedis]error: jedis == null");
            return null;
        }

        //log.info("[listFromRedis] user : " + userID);
        //String bugMsg = null;

        try {

            Set<String> hashList = jedis.smembers(MSG+userID);

            //List<String> hashList = jedis.srandmember(MSG+userID, count);

            //log.info("[listFromRedis] hash count : " + hashList.size());

            for (String hash : hashList){
                String msg = jedis.get(hash);
                if (msg == null){
                    log.info("[listFromRedis] error: bad data, fix.... hash : " + hash);
                    jedis.srem(MSG+userID, hash);
                } else {
                    JSONObject json = JSONObject.parseObject(msg);
                    JSONObject data = json.getJSONObject("data");
                    MessageData messageData = MessageData.toMessageData(data, 0);
                    msgs.add(messageData);
                }
            }
            //log.info("[listFromRedis] hash :" + hashList.size() + " msg : " + msgs.size());


        } catch (Exception e) {
            log.info("[listFromRedis] error: " + e.getMessage());
            // 数据有问题，清除数据
            //clearBugData(jedis, userID, bugMsg);
        }



        redis.close(jedis);

        return msgs;
    }

    public List<MessageData> listFromRedis(List<String> hashList) {

        List<MessageData> msgs = new ArrayList<MessageData>();

        Jedis jedis = redis.getJedis();
        if (jedis == null) {
            log.info("[listFromRedis]error: jedis == null");
            return null;
        }

        //log.info("[listFromRedis] user : " + userID);
        //String bugMsg = null;

        try {

            for (String hash : hashList) {

                String msg = jedis.get(hash);

                if (msg != null){

                    JSONObject json = JSONObject.parseObject(msg);
                    JSONObject data = json.getJSONObject("data");
                    MessageData messageData = MessageData.toMessageData(data, 0);
                    msgs.add(messageData);

                }
            }
            //log.info("[listFromRedis] hash :" + hashList.size() + " msg : " + msgs.size());


        } catch (Exception e) {
            log.info("[listFromRedis] error: " + e.getMessage());
            // 数据有问题，清除数据
            //clearBugData(jedis, userID, bugMsg);
        }



        redis.close(jedis);

        return msgs;
    }


    private Jedis getJedisSuccess(){
        try {

            while (true) {
                Jedis jedis = redis.getJedis();
                if (jedis != null) {
                    return jedis;
                }
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }
        log.info("getJedis time out");
        return null;
    }

    public Jedis getJedis(){
        return redis.getJedis();
    }

    public Jedis getJedisLoop() {
        Jedis jedis = null;

        while (jedis == null) {

            jedis = getJedis();
            if (jedis != null) {
                return jedis;
            }

            log.info("get jedis looping...");

            try {
                Thread.sleep(1000);
            } catch (Exception e) {

            }

        }
        return null;
    }

    public void closeJedis(Jedis jedis){
        redis.close(jedis);
    }

    public Set<String> listMessageHashFromRedis(Jedis jedis, String userID) {

        try {

            Set<String> hashList = jedis.smembers(MSG+userID);
            return hashList;

        } catch (Exception e) {
            log.info("[listMessageHashFromRedis] error: " + e.getMessage());
        }

        return null;
    }

    public List<MessageData> listFromRedis(String userID, int count) {

        List<MessageData> msgs = new ArrayList<MessageData>();

        Jedis jedis = redis.getJedis();
        if (jedis == null) {
            log.info("[listFromRedis]error: jedis == null");
            return null;
        }

        //log.info("[listFromRedis] user : " + userID);
        //String bugMsg = null;

        try {

            List<String> hashList = jedis.srandmember(MSG+userID, count);

            log.info("[listFromRedis] hash count : " + hashList.size());

            for (String hash : hashList){
                String msg = jedis.get(hash);
                if (msg == null){
                    log.info("[listFromRedis] error: bad data, fix.... hash : " + hash);
                    jedis.srem(MSG+userID, hash);
                } else {
                    JSONObject json = JSONObject.parseObject(msg);
                    JSONObject data = json.getJSONObject("data");
                    MessageData messageData = MessageData.toMessageData(data, 0);
                    msgs.add(messageData);
                }
            }
            log.info("[listFromRedis] hash :" + hashList.size() + " msg : " + msgs.size());


        } catch (Exception e) {
            log.info("[listFromRedis] error: " + e.getMessage());
            // 数据有问题，清除数据
            //clearBugData(jedis, userID, bugMsg);
        }



        redis.close(jedis);

        return msgs;
    }

    public MessageData readFromRedis(String hash) throws Exception {
        Jedis jedis = redis.getJedis();
        if (jedis == null) {
            log.info("[readFromRedis] error: jedis == null");
            throw new Exception("redis error.");
        }

        //log.info("[readFromRedis] to : " + to);
        //log.info("[readFromRedis] hash : " + hash);

        MessageData messageData = null;
        String msg = null;
        try {
            msg = jedis.get(hash);
            if (msg != null){
                JSONObject json = JSONObject.parseObject(msg);
                if (json != null){
                    JSONObject data = json.getJSONObject("data");
                    if (data == null){
                        data = json;
                    }
                    messageData = MessageData.toMessageData(data,0);
                } else {
                    log.info("delete error message : " + msg);
                    jedis.del(hash);
                }
            } else {
                // 会出现这种情况，属于正常现象
                //log.info("[readFromRedis] no data : " + hash);
            }

        } catch (Exception e) {
            log.info("[readFromRedis] error: " + e.getMessage());

        }

        redis.close(jedis);
        return messageData;
    }

    public MessageData readFromRedis(Jedis jedis, String hash) {

        MessageData messageData = null;
        String msg = null;
        try {
            msg = jedis.get(hash);
            if (msg != null){
                JSONObject json = JSONObject.parseObject(msg);
                if (json != null){
                    JSONObject data = json.getJSONObject("data");
                    if (data == null){
                        data = json;
                    }
                    messageData = MessageData.toMessageData(data,0);
                } else {
                    log.info("delete error message : " + msg);
                    jedis.del(hash);
                }
            } else {
                // 会出现这种情况，属于正常现象
                //log.info("[readFromRedis] no data : " + hash);
            }

        } catch (Exception e) {
            log.info("[readFromRedis] error: " + e.getMessage());

        }

        return messageData;
    }


    public String completeFromRedis(String hash, String to) throws Exception {
        //

        Jedis jedis = redis.getJedis();
        if (jedis == null) {
            log.info("[completeFromRedis] error: jedis == null");
            throw new Exception("redis error.");
        }

        //log.info("[completeFromRedis] to : " + to);
        //log.info("[completeFromRedis] hash : " + hash);



        //boolean ret = true;
        String msg = null;
        try {
            msg = jedis.get(hash);

            jedis.srem(MSG+to, hash);
            jedis.del(hash);

        } catch (Exception e) {
            log.info("[completeFromRedis] error: " + e.getMessage());
            //ret = false;
        }

        redis.close(jedis);
        return msg;

    }

    public String completeFromRedis(Jedis jedis, String hash, String to) {


        String msg = null;
        try {
            msg = jedis.get(hash);
            // 移除to集合中的hash
            jedis.srem(MSG+to, hash);
            // 移除msg主体
            jedis.del(hash);

        } catch (Exception e) {
            log.info("[completeFromRedis] error: " + e.getMessage());

        }

        return msg;

    }

    public String getCompleteMessageInRedis(){
        Jedis jedis = redis.getJedis();
        if (jedis == null){
            log.info("[getCompleteMessageInRedis] error: jedis == null");
            return null;
        }

        String msg = null;

        try {

            msg = jedis.rpop(MQComplete);


        } catch (Exception e) {
            log.info("[getCompleteMessageInRedis] error: " + e.getMessage());
        }

        redis.close(jedis);
        return msg;
    }

    public void backToRedisMQ(Jedis jedis, String msgInfo, String MQ){

        try {

            jedis.rpush(MQ, msgInfo);

        } catch (Exception e) {
            log.info("[backToRedisMQ] error: " + e.getMessage());
        }

        //redis.close(jedis);
        return;
    }

    public void deleteRedisMessage(Jedis jedis, String hash, String to){


        try {
            jedis.del(hash);
            jedis.srem(MSG+to, hash);
        } catch (Exception e) {
            log.info("[deleteRedisMessage] error: " + e.getMessage());
        }

        //redis.close(jedis);
        return;
    }

    public RedisMessageInfo getNeedSaveMessage(Jedis jedis, String MQ) {


        RedisMessageInfo info = null;

        try {

            String msgInfo = jedis.rpop(MQ);
            if (msgInfo != null) {
                info = new RedisMessageInfo(msgInfo);

                if (info.state == 0) {
                    //
                    //log.info("time not arrive. (" + MQ + ") data : " + msgInfo);
                    jedis.rpush(MQ, msgInfo);
                }

            }



        } catch (Exception e) {
            log.info("[getRedisMessageInfo] error: " + e.getMessage());
        }

        //redis.close(jedis);
        return info;
    }



    private RedisMessageState getRedisMessageState(String msgInfo){

        //RedisMessageState result = new RedisMessageState();
        JSONObject infoJson = JSONObject.parseObject(msgInfo);
        if (infoJson == null){
            // 一般不会发生这种情况
            log.info("[RedisToDB] bad message : " + msgInfo);
            return new RedisMessageState(-1);
        }

        String hash = infoJson.getString("hash");
        long time = infoJson.getLong("time");

        if (hash == null){
            // 一般不会发生这种情况
            log.info("[RedisToDB] bad message : " + msgInfo);
            return new RedisMessageState(-1);
        }

        // 判断时间是否超过
        long currentTime = System.currentTimeMillis();
        if (time + deltaTime < currentTime){
            // 转存
            return new RedisMessageState(0, hash);

        } else {
            // 时间没到，要存回去
            return new RedisMessageState(1);
        }
    }





    public void createRedisMessageMap(Jedis jedis, String to, String hash) {
        try {
            if (jedis != null) {
                jedis.sadd(MSGMAP+to, hash);
            }

        } catch (Exception e) {

            log.info(e.getMessage());
        }
    }

    public void deleteRedisMessageMap(Jedis jedis, String to, String hash) {
        try {
            if (jedis != null) {
                jedis.srem(MSGMAP+to, hash);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }

    public Set<String> listRedisUserMessageMap(Jedis jedis, String to) {
        try {
            if (jedis != null) {
                return jedis.smembers(MSGMAP+to);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return null;
    }




    boolean insertMQMSG(Jedis jedis, JSONObject json){
        String fromID = json.getString("fromID");
        String toID = json.getString("toID");
        String data = json.getString("data");
        String hash = json.getString("hash");
        String hash0 = json.getString("hash0");
        String cmd = json.getString("cmd");
        long timeStamp = System.currentTimeMillis();//json.getLongValue("timeStamp");
        int state = json.getIntValue("state");

        if (fromID == null
                || toID == null
                || hash == null
                || data == null
                || cmd == null
        ){
            return false;
        }
        if (hash0 == null){
            hash0 = "";
        }

        //log.info("[insert] (MQ) hash : " + hash);

        createRedisMessageMap(jedis, toID, hash);

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            //log.info("cmd: " + messageData.cmd);
            String sql = "insert into t_message (fromID,toID,data,timeStamp,hash,hash0,state,cmd,createTime) values(?,?,?,?,?,?,?,?,?);";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, fromID);
            statement.setString(2, toID);
            statement.setString(3, data);
            statement.setLong(4, timeStamp);
            statement.setString(5, hash);
            statement.setString(6, hash0);
            statement.setInt(7, state);
            statement.setString(8, cmd);
            statement.setLong(9, System.currentTimeMillis());
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    boolean insertMQOSX(JSONObject json){
        String fromID = json.getString("fromID");
        String toID = json.getString("toID");
        String data = json.getString("data");
        String hash = json.getString("hash");
        String hash0 = json.getString("hash0");
        String cmd = json.getString("cmd");
        long timeStamp = System.currentTimeMillis();//json.getLongValue("timeStamp");
        int state = json.getIntValue("state");

        if (fromID == null
                || toID == null
                || hash == null
                || data == null
                || cmd == null
        ){
            return false;
        }
        if (hash0 == null){
            hash0 = "";
        }

        //log.info("[insert] (OSX) hash : " + hash);


        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_receipt (fromID,toID,data,timeStamp,hash,hash0,state,cmd,createTime) values(?,?,?,?,?,?,?,?,?);";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, fromID);
            statement.setString(2, toID);
            statement.setString(3, data);
            statement.setLong(4, timeStamp);
            statement.setString(5, hash);
            statement.setString(6, hash0);
            statement.setInt(7, state);
            statement.setString(8, cmd);
            statement.setLong(9, System.currentTimeMillis());
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean insert(Jedis jedis, JSONObject json, String MQ){

        if (MQ.equalsIgnoreCase(MQCode)){
            return insertMQMSG(jedis, json);
        }else if (MQ.equalsIgnoreCase(MQOSX)){
            return insertMQOSX(json);
        }

        return false;
    }



    public List<MessageData> list(String userID, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_message WHERE toID=? LIMIT ?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setInt(2, count);
            rs = statement.executeQuery();
            while (rs.next()) {
                MessageData messageData = toData(rs);
                if (messageData != null)
                    messageDataList.add(messageData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageDataList;
    }

    public List<MessageData> listUnread(String userID, int count){

        //log.info("db test");

        List<MessageData> messageDataList = new ArrayList<>();

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            String sql = "SELECT * FROM t_message WHERE toID=? AND state<>2 LIMIT ?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setInt(2, count);
            rs = statement.executeQuery();
            while (rs.next()) {
                MessageData messageData = toData(rs);
                if (messageData != null)
                    messageDataList.add(messageData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageDataList;
    }

    public List<MessageData> listUnreadPage(int offset, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_message WHERE id>? and state<>? LIMIT ?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setInt(1, offset);
            statement.setInt(2, 2);
            statement.setInt(3, count);
            rs = statement.executeQuery();
            while (rs.next()) {
                MessageData messageData = toData(rs);
                if (messageData != null)
                    messageDataList.add(messageData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageDataList;
    }
    public List<MessageData> listUnreadPage(String user, int offset, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_message WHERE toID=? and state<>2 LIMIT ?,?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, user);
            statement.setInt(2, offset);
            statement.setInt(3, count);
            rs = statement.executeQuery();
            while (rs.next()) {
                MessageData messageData = toData(rs);
                if (messageData != null)
                    messageDataList.add(messageData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageDataList;
    }

    public MessageData read(String hash) throws Exception{

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MessageData messageData = null;
        try {
            String sql = "select * from t_message where hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            rs = statement.executeQuery();
            if (rs.next())
                messageData = toData(rs);
        } catch (Exception e) {
            log.error("", e);
            throw new Exception("db error. do not delete redis.");
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageData;

    }

    public MessageData readOsx(String hash){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MessageData messageData = null;
        try {
            String sql = "select * from t_receipt where hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            rs = statement.executeQuery();
            if (rs.next())
                messageData = toData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageData;

    }


    public boolean updateRead(String hash) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "update t_message set state=? where hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setInt(1, Constant.ReceiptStatus_Complete);
            statement.setString(2, hash);

            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean updateOsxRead(String hash) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "update t_receipt set state=? where hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setInt(1, Constant.ReceiptStatus_Complete);
            statement.setString(2, hash);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }


    public void delete(Jedis jedis, String to, String hash){

        deleteRedisMessageMap(jedis, to, hash);

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_message WHERE hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delete jedis] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
    }

    public void delete(long id){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_message WHERE id=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, id);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delete id ] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
    }

    public void delete(String hash){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_message WHERE hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delete hash ] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
    }

    /*public void delStale(long timestamp){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_message WHERE timestamp<?";
            connection = comboPooledDataSource.getConnection();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, timestamp);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delStale ims ] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
    }*/

    void insertMessage(JSONObject msg, Jedis jedis, String MQ){

        if (msg == null){
            log.info("error, input msg null.");
            return;
        }

        if (jedis == null){
            log.info("error, input jedis null.");
            return;
        }

        if (MQ == null){
            log.info("error, input MQ null.");
            return;
        }

        try {

            //log.info("1");

            String hash = msg.getString("hash");
            String to = msg.getString("toID");
            String message = msg.toString();

            /*JSONObject data = msg.getJSONObject("data");
            if (data == null){
                log.info("[insertMessage] error : data == null");
                log.info("data : " + msg.getString("data"));
                return;
            }
            String command = data.getString("command");*/
            //log.info("[insertMessage] " + command + " : " + hash);
            //log.info("[insertMessage] hash : "+ hash);

            //log.info("2");

            // 1. 将message主体存起来
            String msgTemp = jedis.get(hash);
            if (msgTemp != null){
                log.info("[insertMessage] error, message existed.");
                return;
            }
            String result = jedis.set(hash, message);
            if (!"OK".equalsIgnoreCase(result)){
                log.info("[insertMessage] error, set ： " + result);
                return;
            }

            //log.info("3");

            // 2. 将hash存起来，方便于sync
            // 去重，先删，后加
            jedis.srem(MSG+to, hash);
            long temp2 = jedis.sadd(MSG+to, hash);
            //log.info("[insertMessage] sadd ：" + temp2);

            long time = System.currentTimeMillis();
            JSONObject json = new JSONObject();
            json.put("hash", hash);
            json.put("time", time);
            // 3. 存入队列，方便于转存到db
            long temp3 = jedis.lpush(MQ, json.toString());


            //log.info("[insertMessage] lpush ：" + temp3);
        } catch (Exception e) {
            log.info(e.getMessage());
            log.info("error msg : " + msg);
        }

    }

    JSONObject MessageData2JSONObject(MessageData messageData){
        if (messageData == null){
            return null;
        }
        JSONObject json = new JSONObject();
        json.put("cmd", messageData.cmd);
        json.put("fromID", messageData.fromID);
        json.put("toID", messageData.toID);
        json.put("data", messageData.data);
        json.put("hash", messageData.hash);
        json.put("hash0", messageData.hash0);
        json.put("timeStamp", messageData.timeStamp);
        json.put("state", messageData.state);
        json.put("createTime", messageData.createTime);
        return json;
    }

    private MessageData toData(ResultSet rs) {
        try {
            MessageData messageData = new MessageData();
            messageData.cmd = rs.getString("cmd");
            messageData.fromID = rs.getString("fromID");
            messageData.toID = rs.getString("toID");
            messageData.timeStamp = rs.getLong("timeStamp");
            messageData.data = rs.getString("data");
            messageData.hash = rs.getString("hash");
            messageData.hash0 = rs.getString("hash0");
            messageData.state = rs.getInt("state");
            messageData.createTime = rs.getLong("createTime");
            messageData.id = rs.getInt("id");
            return messageData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private MessageData getData(ResultSet rs) {
        try {
            MessageData messageData = new MessageData();
            messageData.data = rs.getString("data");
            if (messageData.data == null) {
                return null;
            }
            return messageData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
