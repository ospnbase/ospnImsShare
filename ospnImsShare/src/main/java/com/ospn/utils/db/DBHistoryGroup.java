package com.ospn.utils.db;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.GroupData;
import com.ospn.utils.PerUtils;
import com.ospn.utils.data.GroupHistoryMessageData;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;



@Slf4j
public class DBHistoryGroup extends DBBase {

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_group_history " +
                    "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
                    "`group` char(128) NOT NULL, " +
                    "hash char(128) NOT NULL UNIQUE, " +
                    "msg text not null, " +
                    "content text not null, " +
                    "timestamp bigint, " +
                    "KEY `group` (`group`) USING BTREE" +
                    ")";


    public DBHistoryGroup(ComboPooledDataSource data){
        super(data);
    }

    public boolean insert(GroupHistoryMessageData msg) {


        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_group_history(group,hash,msg,content,timestamp)" +
                    " values(?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();
            PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, msg.group);
            statement.setString(2, msg.hash);
            statement.setString(3, msg.msg);
            statement.setString(4, msg.content);
            statement.setLong(5, System.currentTimeMillis());

            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public List<GroupHistoryMessageData> listDesc(String group) {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<GroupHistoryMessageData> dataList = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_group_history WHERE group=? ORDER BY id DESC LIMIT 40";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, group);
            rs = statement.executeQuery();
            while (rs.next()) {
                GroupHistoryMessageData msgData = toData(rs);
                dataList.add(msgData);
            }
        } catch (Exception e) {
            log.error("error", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return dataList;
    }

    /*public boolean delete(GroupHistoryMessageData group) {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_group where group=?";
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
    }*/

    private GroupHistoryMessageData toData(ResultSet rs) {
        try {
            GroupHistoryMessageData msg = new GroupHistoryMessageData();
            msg.group = rs.getString("group");
            msg.hash = rs.getString("hash");
            msg.msg = rs.getString("msg");
            msg.content = rs.getString("content");
            msg.timestamp = rs.getLong("timestamp");
            return msg;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
