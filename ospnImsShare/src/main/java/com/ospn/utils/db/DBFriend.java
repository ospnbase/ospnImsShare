package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.FriendData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DBFriend extends DBBase {

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_friend " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " osnID char(128) NOT NULL, " +
            " friendID char(128) NOT NULL, " +
            " remarks nvarchar(128), " + //add 2020.12.8
            " state tinyint DEFAULT 0, " +
            " createTime bigint)";

    public DBFriend(ComboPooledDataSource data){
        super(data);
    }


    public boolean insert(FriendData friendData) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_friend (osnID,friendID,state,createTime) values(?,?,?,?);";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, friendData.userID);
            statement.setString(2, friendData.friendID);
            statement.setInt(3, friendData.state);
            statement.setLong(4, System.currentTimeMillis());
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }


    public boolean update(FriendData friendData) {

        List<String> keyList = new ArrayList<>();

        if (friendData.remarks != null){
            keyList.add("remarks");
        }
        if (friendData.state != -1){
            keyList.add("state");
        }

        return update(friendData, keyList);
    }


    public boolean update(FriendData friendData, List<String> keys) {

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
                    case "remarks":
                        columns.append("remarks=?");
                        break;
                    case "state":
                        columns.append("state=?");
                        break;
                }
            }
            if (columns.length() == 0) {
                log.info("key error");
                return false;
            }
            String sql = "update t_friend set " + columns.toString() + " where osnID=? and friendID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int index = 1;
            for (String k : keys) {
                switch (k) {
                    case "remarks":
                        statement.setString(index++, friendData.remarks);
                        break;
                    case "state":
                        statement.setInt(index++, friendData.state);
                        break;
                }
            }
            statement.setString(index++, friendData.userID);
            statement.setString(index, friendData.friendID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public FriendData read(String userID, String friendID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select * from t_friend where osnID=? and friendID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setString(2, friendID);
            rs = statement.executeQuery();
            if (rs.next())
                return toData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return null;
    }

    public List<FriendData> list(String userID) {

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
                friendList.add(toData(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return friendList;
    }


    public boolean deleteAll(String userID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_friend where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean delete(String userID, String friendID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_friend where osnID=? and friendID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setString(2, friendID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    private FriendData toData(ResultSet rs) {
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

}
