package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.command.CmdSetUserInfo;
import com.ospn.data.MessageData;
import com.ospn.data.UserData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DBUser extends DBBase {
    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_user " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " osnID char(128) NOT NULL UNIQUE, " +
            " privateKey char(255) NOT NULL, " +
            " name nvarchar(20) NOT NULL, " +
            " displayName nvarchar(20) NOT NULL, " +
            " nickName nvarchar(20), " + //add 2020.12.8
            " aesKey char(64) NOT NULL, " +
            " msgKey char(64) NOT NULL, " +
            " maxGroup int default 100, " +
            " portrait text, " +
            " describes text, " + //add 2020.12.8
            " role text, " + //add 2022.09.27
            " password varchar(64) NOT NULL, " +
            " loginTime bigint, " +
            " logoutTime bigint, " +
            " createTime bigint, " +
            " owner2 char(128), " +
            " UNIQUE(name))";

    public DBUser(ComboPooledDataSource data){
        super(data);
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
                userData = toData(rs);
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
                userData = toData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return userData;
    }

    public boolean insert(UserData userData) {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_user (" +
                    "osnID,privateKey,name,displayName," +
                    "aesKey,msgKey,password,createTime," +
                    "owner2,portrait,describes,role)" +
                    " values(?,?,?,?,?,?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userData.osnID);
            statement.setString(2, userData.osnKey);
            statement.setString(3, userData.name);
            statement.setString(4, userData.displayName);
            statement.setString(5, userData.aesKey);
            statement.setString(6, userData.msgKey);
            statement.setString(7, userData.password);
            statement.setLong(8, System.currentTimeMillis());
            statement.setString(9,userData.owner2);
            statement.setString(10,userData.portrait);
            statement.setString(11,userData.describes);
            statement.setString(12,userData.role);

            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
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

    public boolean delete(String osnID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_user where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, osnID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean update(UserData userData){
        List<String> keyList = new ArrayList<>();
        if (userData.displayName != null){
            keyList.add("displayName");
        }
        if (userData.portrait != null){
            keyList.add("portrait");
        }
        if (userData.nickName != null){
            keyList.add("nickName");
        }
        if (userData.describes != null){
            keyList.add("describes");
        }
        /*if (userData.urlSpace != null){
            keyList.add("urlSpace");
        }*/
        if (userData.aesKey != null){
            keyList.add("aesKey");
        }

        return update(userData, keyList);
    }

    public boolean update(UserData userData, List<String> keys) {

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
                    case "role":
                        columns.append("role=?");
                        break;
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
                    case "role":
                        statement.setString(index++, userData.role);
                        break;
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

    public List<UserData> list(String osnID, int limit) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<UserData> userList = new ArrayList<>();
        try {
            String sql = osnID == null
                    ? "select * from t_user where id>=0 limit ?"
                    : "select * from t_user where id>(select id from t_user where osnID=?) limit ?";
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
                UserData userData = toData(rs);
                userList.add(userData);
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return userList;
    }

    public List<String> listPage(int offset, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> users = new ArrayList<>();
        try {
            String sql = "SELECT osnID FROM t_user LIMIT ?,?";
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

    public int getOsnUserCount() {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        int count = 0;
        try {
            String sql = "SELECT COUNT(*) FROM t_user WHERE privateKey=\"\"";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
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



    private UserData toData(ResultSet rs) {
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
            userData.loginTime = rs.getLong("loginTime");
            userData.logoutTime = rs.getLong("logoutTime");
            userData.createTime = rs.getLong("createTime");
            //log.info("id:" +userData.osnID + "-----" + "role:" + userData.role);
            return userData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
}
