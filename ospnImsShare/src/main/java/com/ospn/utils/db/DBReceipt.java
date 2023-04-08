package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.MessageData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DBReceipt extends DBBase {
    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_receipt " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " cmd char(64) NOT NULL, " +
            " fromID char(128) NOT NULL, " +
            " toID char(128) NOT NULL, " +
            " state tinyint DEFAULT 0, " +
            " timeStamp bigint NOT NULL, " +
            " hash char(128) NOT NULL, " +
            " hash0 char(128), " +
            " data text NOT NULL, " +
            " createTime bigint, " +
            " UNIQUE(hash))";

    public static long ms_hour = 3600000;
    public static long ms_minute = 60000;
    public static long cycleTime = ms_minute * 10;

    public DBReceipt(ComboPooledDataSource data){
        super(data);
    }


    public List<MessageData> list(String userID, int count){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_receipt WHERE toID=? LIMIT ?";
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
            String sql = "SELECT * FROM t_receipt WHERE state<>2 LIMIT ?,?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setInt(1, offset);
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

    public List<MessageData> listUnreadPage(int offset, int count, long time){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MessageData> messageDataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_receipt WHERE state<>2 and timeStamp>? LIMIT ?,?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, time);
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


    public void delete(String hash){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_receipt WHERE hash=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, hash);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delStale] delete count : " + count);
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
            String sql = "DELETE FROM t_receipt WHERE id=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, id);
            int count = statement.executeUpdate();
            System.out.println("[DBMessage::delStale] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
    }


    public void delStale(long timestamp){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "DELETE FROM t_receipt WHERE timestamp<?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setLong(1, timestamp);
            int count = statement.executeUpdate();
            System.out.println("[DBReceipt::delStale] delete count : " + count);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return;
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
            messageData.id = rs.getLong("id");
            return messageData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
