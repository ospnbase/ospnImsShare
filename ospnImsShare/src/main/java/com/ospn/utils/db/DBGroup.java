package com.ospn.utils.db;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.GroupData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DBGroup extends DBBase {

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_group " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            "osnID char(128) NOT NULL UNIQUE, " +
            "name text NOT NULL, " +
            "owner char(128) NOT NULL, " +
            "privateKey char(255) NOT NULL, " +
            "portrait text not null, " +
            "type tinyint default 0, " +
            "joinType tinyint default 0, " +
            "passType tinyint default 0, " +
            "mute tinyint default 0, " +
            "maxMember int default 500, " +
            "createTime bigint ," +
            "aesKey char(128), " +           //add by CESHI
            "owner2 char(128), " +             // add by CESHI 企业所有者
            "billboard text, " +           // 公告牌
            "`describe` text," +
            "`privateInfo` text," +
            "attribute text)";              // add by CESHI


    public DBGroup(ComboPooledDataSource data){
        super(data);
    }

    public boolean insert(GroupData group) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_group(osnID,name,privateKey,owner,portrait,createTime,aesKey,owner2,maxMember)" +
                    " values(?,?,?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, group.osnID);
            statement.setString(2, group.name);
            statement.setString(3, group.osnKey);
            statement.setString(4, group.owner);
            statement.setString(5, group.portrait);
            statement.setLong(6, System.currentTimeMillis());
            statement.setString(7, group.aesKey);    //add by CESHI
            statement.setString(8, group.owner2);    //add by CESHI
            statement.setInt(9, group.maxMember);

            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public GroupData read(String groupID) {

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
                group = toData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return group;
    }


    public List<String> list(String userID, boolean isAll) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> groupList = new ArrayList<String>();
        try {
            String sql = isAll ? "select groupID from t_groupMember where osnID=?"
                    : "select groupID from t_groupMember where osnID=? and status<>0";
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

    public List<String> listPage(int offset, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> users = new ArrayList<>();
        try {
            String sql = "SELECT osnID FROM t_group LIMIT ?,?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setInt(1, offset);
            statement.setInt(2, count);
            rs = statement.executeQuery();
            while (rs.next()) {

                try {
                    users.add(rs.getString("osnID"));
                } catch (Exception e) {
                }

            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return users;
    }

    public List<GroupData> list(String osnID, int limit) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<GroupData> groupList = new ArrayList<GroupData>();
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
                GroupData groupData = toData(rs);
                groupList.add(groupData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return groupList;
    }

    public boolean delete(GroupData group) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_group where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, group.osnID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean newOwner(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("owner");
        return update(groupData, keys);
    }

    public boolean setAttribute(GroupData groupData, String attribute){
        List<String> keys = new ArrayList<>();
        keys.add("attribute");

        JSONObject jsonAttr = JSONObject.parseObject(attribute);
        if (jsonAttr.containsKey("signByOwner2")){
            return false;
        }

        groupData.attribute = attribute;

        // 这里是要做json合并
        //JSONObject jsonAttr2 = JSONObject.parseObject(groupData.attribute);
        //jsonAttr2.

        return update(groupData, keys);
    }

    public boolean setDescribe(GroupData groupData, String describe){
        List<String> keys = new ArrayList<>();
        keys.add("describe");

        JSONObject jsonAttr = JSONObject.parseObject(describe);

        groupData.describe = describe;

        return update(groupData, keys);
    }

    public boolean setPrivateInfo(GroupData groupData, String info){
        List<String> keys = new ArrayList<>();
        keys.add("privateInfo");

        JSONObject jsonAttr = JSONObject.parseObject(info);

        groupData.privateInfo = info;

        return update(groupData, keys);
    }

    public boolean setPortrait(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("portrait");
        return update(groupData, keys);
    }
    public boolean setName(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("name");
        return update(groupData, keys);
    }
    public boolean setPassType(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("passType");
        return update(groupData, keys);
    }
    public boolean setJoinType(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("joinType");
        return update(groupData, keys);
    }
    public boolean setType(GroupData groupData){
        List<String> keys = new ArrayList<>();
        keys.add("type");
        return update(groupData, keys);
    }
    public boolean update(GroupData groupData, List<String> keys) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            StringBuilder columns = new StringBuilder();
            for (String k : keys) {
                if (columns.length() != 0)
                    columns.append(",");
                if (k.equalsIgnoreCase("name"))
                    columns.append("name=?");
                else if (k.equalsIgnoreCase("portrait"))
                    columns.append("portrait=?");
                else if (k.equalsIgnoreCase("type"))
                    columns.append("type=?");
                else if (k.equalsIgnoreCase("joinType"))
                    columns.append("joinType=?");
                else if (k.equalsIgnoreCase("passType"))
                    columns.append("passType=?");
                else if (k.equalsIgnoreCase("mute"))
                    columns.append("mute=?");
                else if (k.equalsIgnoreCase("maxMember"))
                    columns.append("maxMember=?");
                else if (k.equalsIgnoreCase("attribute"))
                    columns.append("attribute=?");
                else if (k.equalsIgnoreCase("billboard"))
                    columns.append("billboard=?");
                else if (k.equalsIgnoreCase("owner"))
                    columns.append("owner=?");
                else if (k.equalsIgnoreCase("describe"))
                    columns.append("describe=?");
                else if (k.equalsIgnoreCase("privateInfo"))
                    columns.append("privateInfo=?");
            }
            if (columns.length() == 0) {
                log.info("key error");
                return false;
            }
            String sql = "update t_group set " + columns + " where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int index = 1;
            for (String k : keys) {
                if (k.equalsIgnoreCase("name"))
                    statement.setString(index++, groupData.name);
                else if (k.equalsIgnoreCase("portrait"))
                    statement.setString(index++, groupData.portrait);
                else if (k.equalsIgnoreCase("type"))
                    statement.setInt(index++, groupData.type);
                else if (k.equalsIgnoreCase("joinType"))
                    statement.setInt(index++, groupData.joinType);
                else if (k.equalsIgnoreCase("passType"))
                    statement.setInt(index++, groupData.passType);
                else if (k.equalsIgnoreCase("mute"))
                    statement.setInt(index++, groupData.mute);
                else if (k.equalsIgnoreCase("maxMember"))
                    statement.setInt(index++, groupData.maxMember);
                else if (k.equalsIgnoreCase("attribute"))
                    statement.setString(index++, groupData.attribute);
                else if (k.equalsIgnoreCase("billboard"))
                    statement.setString(index++, groupData.billboard);
                else if (k.equalsIgnoreCase("owner"))
                    statement.setString(index++, groupData.owner);
                else if (k.equalsIgnoreCase("describe"))
                    statement.setString(index++, groupData.describe);
                else if (k.equalsIgnoreCase("privateInfo"))
                    statement.setString(index++, groupData.privateInfo);

            }
            statement.setString(index, groupData.osnID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    private GroupData toData(ResultSet rs) {
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
            groupData.describe = rs.getString("describe");
            groupData.privateInfo = rs.getString("privateInfo");

            return groupData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
