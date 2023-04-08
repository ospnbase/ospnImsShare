package com.ospn.utils;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.core.IMDb;
import com.ospn.data.Constant;
import com.ospn.data.*;
import com.ospn.utils.db.*;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DBUtils implements IMDb {
    private static ComboPooledDataSource comboPooledDataSource = null;
    private static RedisUtils redisUtils = null;

    public DBGroup group;
    public DBGroupDisable groupDisable;
    public DBFriend friend;
    public DBGroupMember groupMember;
    public DBUser user;
    public DBUserDisable userDisable;
    public DBOSNUser osnUser;

    public DBMessage message;
    public DBReceipt receipt;
    public DBConversation conversation;

    public DBHistoryGroup groupHistory;

    public DBOsnid dbOsnid;


    public void initDB(String configFile) {
        comboPooledDataSource = new ComboPooledDataSource();
        try {
            String jdbcUrl = comboPooledDataSource.getJdbcUrl();
            String url01 = jdbcUrl.substring(0, jdbcUrl.indexOf("?"));
            String datasourceName = url01.substring(url01.lastIndexOf("/") + 1);

            String jdbc = jdbcUrl.replace(datasourceName, "");
            Connection connection = DriverManager.getConnection(jdbc, comboPooledDataSource.getUser(), comboPooledDataSource.getPassword());
            Statement statement = connection.createStatement();

            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS `" + datasourceName + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;");

            statement.close();
            connection.close();

            createTable();
            updateTable();

            redisUtils = new RedisUtils(configFile);

            group = new DBGroup(comboPooledDataSource);
            groupDisable = new DBGroupDisable(comboPooledDataSource);
            friend = new DBFriend(comboPooledDataSource);
            groupMember = new DBGroupMember(comboPooledDataSource);
            user = new DBUser(comboPooledDataSource);
            osnUser = new DBOSNUser(comboPooledDataSource);
            message = new DBMessage(comboPooledDataSource, redisUtils);
            receipt = new DBReceipt(comboPooledDataSource);
            conversation = new DBConversation(comboPooledDataSource, redisUtils);
            groupHistory = new DBHistoryGroup(comboPooledDataSource);
            dbOsnid = new DBOsnid(comboPooledDataSource);
            userDisable = new DBUserDisable(comboPooledDataSource);


        } catch (Exception e) {
            log.error("", e);
            System.exit(-1);
        }
    }

    private void createTable() {
        try {
            String[] sqls = {

                    DBService.sql,
                    DBGroup.sql,
                    DBGroupMember.sql,
                    DBUser.sql,
                    DBFriend.sql,
                    DBMessage.sql,
                    DBReceipt.sql,
                    DBConversation.sql,
                    DBOSNUser.sql,
                    DBHistoryGroup.sql,
                    DBOsnid.sql,
                    DBUserDisable.sql,
                    DBGroupDisable.sql

            };
            Connection connection = comboPooledDataSource.getConnection();
            Statement stmt = connection.createStatement();
            for (String sql : sqls)
                stmt.executeUpdate(sql);
            stmt.close();
            connection.close();
        } catch (Exception e) {
            log.error("", e);
            System.exit(-1);
        }
    }

    private void updateTable() {
        try {
            String[] sqls = {
                    "alter table t_user add column role text", //add 2021.5.15
                    "alter table t_groupMember add column type tinyint default 0", //add 2021.5.20
                    "alter table t_groupMember add column mute tinyint default 0", //add 2021.5.21
                    "alter table t_group add column joinType tinyint default 0", //add 2021.5.21
                    "alter table t_group add column passType tinyint default 0", //add 2021.5.21
                    "alter table t_group add column mute tinyint default 0", //add 2021.5.21
                    "alter table t_group add column `privateInfo` text",
                    "alter table t_group add column `describe` text",
                    "alter table t_group modify column name text not null", //add 2021.5.21
                    "alter table t_groupMember add column inviter char(128) NOT NULL", //add 2021.5.21
                    "alter table t_message add column hash0 char(128) not null",
                    "alter table t_receipt add column hash0 char(128) not null",
                    "alter table t_service add column createTime bigint",
                    "alter table t_group add column createTime bigint",
                    "alter table t_groupMember add column createTime bigint",
                    "alter table t_user add column createTime bigint",
                    "alter table t_friend add column createTime bigint",
                    "alter table t_message add column createTime bigint",
                    "alter table t_receipt add column createTime bigint",
                    "alter table t_conversation add column createTime bigint",
                    "alter table t_conversation add unique index i_conversation(userID,target)",
                    "alter table t_user add column loginTime bigint",
                    "alter table t_user add column logoutTime bigint"
            };
            Connection connection = comboPooledDataSource.getConnection();
            Statement stmt = connection.createStatement();
            for (String sql : sqls) {
                try {
                    stmt.executeUpdate(sql);
                } catch (Exception e) {
                    //log.error(e + ": " + sql);
                }
            }
            stmt.close();
            connection.close();
        } catch (Exception e) {
            log.error("", e);
            System.exit(-1);
        }
    }

    private void closeDB(Connection connection, PreparedStatement statement, ResultSet rs) {
        try {
            if (rs != null)
                rs.close();
        } catch (Exception e) {
            log.error("", e);
        }
        try {
            if (statement != null)
                statement.close();
        } catch (Exception e) {
            log.error("", e);
        }
        try {
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private UserData toUserData(ResultSet rs) {
        try {
            UserData userData = new UserData();

            userData.osnID = rs.getString("osnID");
            userData.osnKey = rs.getString("privateKey");
            userData.name = rs.getString("name");
            userData.password = rs.getString("password");
            userData.displayName = rs.getString("displayName");
            userData.nickName = rs.getString("nickName");
            userData.describes = rs.getString("describes");
            userData.aesKey = rs.getString("aesKey");
            userData.msgKey = rs.getString("msgKey");
            userData.maxGroup = rs.getInt("maxGroup");
            userData.portrait = rs.getString("portrait");
            userData.role = rs.getString("role");
            //log.info("id:" +userData.osnID + "-----" + "role:" + userData.role);
            userData.loginTime = rs.getLong("loginTime");
            userData.logoutTime = rs.getLong("logoutTime");
            userData.createTime = rs.getLong("createTime");
            userData.owner2 = rs.getString("owner2");
            return userData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private GroupData toGroupData(ResultSet rs) {
        try {
            GroupData groupData = new GroupData();
            groupData.osnID = rs.getString("osnID");
            groupData.name = rs.getString("name");
            groupData.owner = rs.getString("owner");
            groupData.owner2 = rs.getString("owner2");
            groupData.type = rs.getInt("type");
            groupData.joinType = rs.getInt("joinType");
            groupData.passType = rs.getInt("passType");
            groupData.mute = rs.getInt("mute");
            groupData.osnKey = rs.getString("privateKey");
            groupData.portrait = rs.getString("portrait");
            groupData.maxMember = rs.getInt("maxMember");
            groupData.createTime = rs.getLong("createTime");
            groupData.aesKey = rs.getString("aesKey");
            groupData.attribute = rs.getString("attribute");
            groupData.billboard = rs.getString("billboard");
            return groupData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private FriendData toFriend(ResultSet rs) {
        try {
            FriendData friendData = new FriendData();
            friendData.userID = rs.getString("osnID");
            friendData.friendID = rs.getString("friendID");
            friendData.remarks = rs.getString("remarks");
            friendData.state = rs.getInt("state");
            friendData.createTime = rs.getLong("createTime");
            return friendData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private MemberData toMemberData(ResultSet rs) {
        try {
            MemberData memberData = new MemberData();
            memberData.osnID = rs.getString("osnID");
            memberData.groupID = rs.getString("groupID");
            memberData.remarks = rs.getString("remarks");
            memberData.nickName = rs.getString("nickName");
            memberData.inviter = rs.getString("inviter");
            memberData.type = rs.getInt("type");
            memberData.mute = rs.getInt("mute");
            memberData.createTime = rs.getLong("createTime");
            memberData.receiverKey =  rs.getString("receiverKey");
            return memberData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private MessageData toMessageData(ResultSet rs) {
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
            messageData.id = rs.getLong("id");

            return messageData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    private LitappData toLitappData(ResultSet rs) {
        try {
            LitappData litappData = new LitappData();
            litappData.osnID = rs.getString("osnID");
            litappData.name = rs.getString("name");
            litappData.displayName = rs.getString("displayName");
            litappData.osnKey = rs.getString("privateKey");
            litappData.portrait = rs.getString("portrait");
            litappData.theme = rs.getString("theme");
            litappData.url = rs.getString("url");
            litappData.info = rs.getString("info");
            litappData.config = rs.getString("config");
            litappData.createTime = rs.getLong("createTime");
            return litappData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public boolean keepAlive() {

        //log.info("db test");
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select count(*) from t_service";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            rs = statement.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public GroupData readGroup(String groupID) {

        //log.info("db test");

        GroupData group = null;
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select * from t_group where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, groupID);
            rs = statement.executeQuery();
            if (rs.next())
                group = toGroupData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return group;
    }

    public List<String> listGroup(String userID, boolean isAll) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> groupList = new ArrayList<>();
        try {
            String sql = isAll ? "select groupID from t_groupMember where osnID=?"
                    : "select groupID from t_groupMember where osnID=? and type<>0 and status<>0";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            rs = statement.executeQuery();
            while (rs.next())
                groupList.add(rs.getString("groupID"));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return groupList;
    }

    public List<GroupData> listGroup(String osnID, int limit) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<GroupData> groupList = new ArrayList<>();
        try {
            String sql = osnID == null
                    ? "select * from t_group where id>=0 limit ?"
                    : "select * from t_group where id>(select id from t_group where osnID=?) limit ?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            if (osnID == null) {
                statement.setInt(1, limit);
            } else {
                statement.setString(1, osnID);
                statement.setInt(2, limit);
            }
            rs = statement.executeQuery();
            while (rs.next()) {
                GroupData groupData = toGroupData(rs);
                groupList.add(groupData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return groupList;
    }

    public List<MemberData> listMember(String groupID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MemberData> members = new ArrayList<>();
        try {
            String sql = "select * from t_groupMember where groupID=? and type<>0";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, groupID);
            rs = statement.executeQuery();
            while (rs.next())
                members.add(toMemberData(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return members;
    }

    public MemberData readMember(String groupID, String userID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MemberData memberData = null;
        try {
            String sql = "select * from t_groupMember where groupID=? and osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, groupID);
            statement.setString(2, userID);
            rs = statement.executeQuery();
            if (rs.next())
                memberData = toMemberData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return memberData;
    }

    public UserData readUserByName(String user) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        UserData userData = null;
        try {
            String sql = "select * from t_user where name=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, user);
            rs = statement.executeQuery();
            if (rs.next())
                userData = toUserData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return userData;
    }

    public UserData readUserByID(String user) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        UserData userData = null;
        try {
            String sql = "select * from t_user where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, user);
            rs = statement.executeQuery();
            if (rs.next())
                userData = toUserData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return userData;
    }

    public boolean insertUserShare(UserData userData) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "replace into t_user (osnID,privateKey,name,displayName,aesKey,msgKey,password,createTime) values(?,?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userData.osnID);
            statement.setString(2, userData.osnKey);
            statement.setString(3, userData.name);
            statement.setString(4, userData.displayName);
            statement.setString(5, userData.aesKey);
            statement.setString(6, userData.msgKey);
            statement.setString(7, userData.password);
            //statement.setString(8, userData.urlSpace);
            statement.setLong(8, System.currentTimeMillis());
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean deleteUser(String osnID) {



        if (!user.delete(osnID)){
            return false;
        }
        return friend.deleteAll(osnID);
    }

    public boolean updateUser(UserData userData, List<String> keys) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            StringBuilder columns = new StringBuilder();
            for (String k : keys) {
                if (columns.length() != 0)
                    columns.append(",");
                switch (k) {
                    case "displayName":
                        columns.append("displayName=?");
                        break;
                    case "nickName":
                        columns.append("nickName=?");
                        break;
                    case "describes":
                        columns.append("describes=?");
                        break;
                    case "portrait":
                        columns.append("portrait=?");
                        break;
                    case "aesKey":
                        columns.append("aesKey=?");
                        break;
                    case "maxGroup":
                        columns.append("maxGroup=?");
                        break;
                    case "password":
                        columns.append("password=?");
                        break;
                    /*case "urlSpace":
                        columns.append("urlSpace=?");
                        break;*/
                    case "loginTime":
                        columns.append("loginTime=?");
                        break;
                    case "logoutTime":
                        columns.append("logoutTime=?");
                        break;
                }
            }
            if (columns.length() == 0) {
                log.info("key error");
                return false;
            }
            String sql = "update t_user set " + columns.toString() + " where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int index = 1;
            for (String k : keys) {
                switch (k) {
                    case "displayName":
                        statement.setString(index++, userData.displayName);
                        break;
                    case "nickName":
                        statement.setString(index++, userData.nickName);
                        break;
                    case "describes":
                        statement.setString(index++, userData.describes);
                        break;
                    case "portrait":
                        statement.setString(index++, userData.portrait);
                        break;
                    case "aesKey":
                        statement.setString(index++, userData.aesKey);
                        break;
                    case "maxGroup":
                        statement.setInt(index++, userData.maxGroup);
                        break;
                    case "password":
                        statement.setString(index++, userData.password);
                        break;
                    /*case "urlSpace":
                        statement.setString(index++, userData.urlSpace);
                        break;*/
                    case "loginTime":
                        statement.setLong(index++, userData.loginTime);
                        break;
                    case "logoutTime":
                        statement.setLong(index++, userData.logoutTime);
                        break;
                }
            }
            statement.setString(index, userData.osnID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public List<FriendData> listFriend(String userID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<FriendData> friendList = new ArrayList<>();
        try {
            String sql = "select * from t_friend where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            rs = statement.executeQuery();
            while (rs.next())
                friendList.add(toFriend(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return friendList;
    }

    public boolean insertMessage(MessageData messageData) {
        return message.insertInRedis(messageData);
    }


    public boolean deleteMessage(String hash) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_message where hash=? or hash0=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
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

    public List<MessageData> loadMessages(String userID, String target, long timestamp, boolean before, int count) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = before
                    ? "select * from (select * from t_message where cmd='Message' and timeStamp<? and (fromID=? and toID=? or fromID=? and toID=?) order by timeStamp desc limit ?) tmp order by timeStamp"
                    : "select * from t_message where cmd='Message' and timeStamp>? and (fromID=? and toID=? or fromID=? and toID=?) limit ?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, timestamp);
            statement.setString(2, userID);
            statement.setString(3, target);
            statement.setString(4, target);
            statement.setString(5, userID);
            statement.setInt(6, count);
            rs = statement.executeQuery();
            while (rs.next()) {
                MessageData messageData = toMessageData(rs);
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

    public int loadMessageStoreCount(String userID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select count(*) from t_message where cmd='Message' and (fromID=? or toID=?)";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setString(2, userID);
            rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return 0;
    }

    public MessageData loadMessageStore(String userID, long timestamp, boolean before) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MessageData messageData = null;
        try {
            String sql = before
                    ? "select * from (select * from t_message where cmd='Message' and timeStamp<? and (fromID=? or toID=?) order by timeStamp desc limit 1) tmp order by timeStamp"
                    : "select * from t_message where cmd='Message' and timeStamp>? and (fromID=? or toID=?) limit 1";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, timestamp);
            statement.setString(2, userID);
            statement.setString(3, userID);
            rs = statement.executeQuery();
            if (rs.next()) {
                messageData = toMessageData(rs);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageData;
    }

    public List<MessageData> loadRequest(String userID, long timeStamp, int count) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> requestList = new ArrayList<>();
        try {
            String sql = "select * from (select * from t_message where cmd='AddFriend' and toID=? and timeStamp<? order by timeStamp desc limit ?) tmp order by timeStamp";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setLong(2, timeStamp);
            statement.setInt(3, count);
            rs = statement.executeQuery();
            while (rs.next())
                requestList.add(toMessageData(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return requestList;
    }

    public MessageData queryMessage(String hash) {

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
                messageData = toMessageData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return messageData;
    }

    public int getUnreadCount(String userID, long timestamp) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        int count = 0;
        try {
            String sql = "select count(*) from t_message where cmd='Message' and timeStamp>? and toID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, timestamp);
            statement.setString(2, userID);
            rs = statement.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return count;
    }

    public boolean insertReceipt(MessageData messageData) {
        message.insertInRedis(messageData, message.MQOSX);
        return true;
    }

    public MessageData queryReceipt(String hash) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select * from t_receipt where hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            rs = statement.executeQuery();
            if (rs.next())
                return toMessageData(rs);
            return null;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return null;
    }

    public List<LitappData> listLitapp() {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<LitappData> litappDataList = new ArrayList<>();
        try {
            String sql = "select * from t_litapp";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            rs = statement.executeQuery();
            while (rs.next()) {
                LitappData litappData = toLitappData(rs);
                if (litappData != null)
                    litappDataList.add(litappData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return litappDataList;
    }

    public LitappData readLitapp(String osnID) {
        return null;
    }

    public boolean insertPyqData(PyqData pyqData) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "replace into t_pyq(osnID,groupID,pyqID,pyqType,pyqText,pyqPicture,pyqWebUrl,pyqWebText,pyqWebPicture,pyqPlace,pyqSyncTime,createTime) values(?,?,?,?,?,?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, pyqData.osnID);
            statement.setString(2, pyqData.groupID);
            statement.setLong(3, pyqData.pyqID);
            statement.setInt(4, pyqData.pyqType);
            statement.setString(5, pyqData.pyqText);
            statement.setString(6, pyqData.pyqPicture);
            statement.setString(7, pyqData.pyqWebUrl);
            statement.setString(8, pyqData.pyqWebText);
            statement.setString(9, pyqData.pyqWebPicture);
            statement.setString(10, pyqData.pyqPlace);
            statement.setLong(11, pyqData.pyqSyncTime);
            statement.setLong(12, pyqData.createTime);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean insertPyqList(PyqList pyqList) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "replace into t_pyq_list(osnID,target,pyqID,createTime) values(?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, pyqList.osnID);
            statement.setString(2, pyqList.target);
            statement.setLong(3, pyqList.pyqID);
            statement.setLong(4, pyqList.createTime);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean isRegisterName(String name) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select count(*) from t_user where name=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, name);
            rs = statement.executeQuery();
            if (rs.next())
                return rs.getInt(1) != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

}
