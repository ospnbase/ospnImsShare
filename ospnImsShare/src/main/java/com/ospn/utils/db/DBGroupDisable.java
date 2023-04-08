package com.ospn.utils.db;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.GroupData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;




@Slf4j
public class DBGroupDisable extends DBBase {

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_group_disable " +
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


    public DBGroupDisable(ComboPooledDataSource data){
        super(data);
    }

    public boolean insert(GroupData group) {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_group_disable(osnID,name,privateKey,owner,portrait,createTime,aesKey,owner2,maxMember)" +
                    " values(?,?,?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();
            PerUtils.writeDB();
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

        GroupData group = null;
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select * from t_group_disable where osnID=?";
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

    public boolean delete(GroupData group) {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_group_disable where osnID=?";
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
