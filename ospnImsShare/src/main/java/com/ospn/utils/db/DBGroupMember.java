package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.MemberData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


@Slf4j
public class DBGroupMember extends DBBase {
    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_groupMember " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " osnID char(128) NOT NULL, " +
            " groupID char(128) NOT NULL, " +
            " remarks nvarchar(128), " + //add 2020.12.8
            " nickName nvarchar(20), " + //add 2020.12.8
            " type tinyint default 0, " +
            " mute tinyint default 0, " +
            " inviter char(128) NOT NULL, " +
            " state tinyint default 0, "+
            " createTime bigint, " +
            " receiverKey char(255)," +     //add by CESHI
            " status tinyint default 0)";   //add by CESHI

    public DBGroupMember(ComboPooledDataSource data){
        super(data);
    }


    public boolean insert(MemberData memberData) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_groupMember(osnID,groupID,type,inviter,createTime,receiverKey,status) values(?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, memberData.osnID);
            statement.setString(2, memberData.groupID);
            statement.setInt(3, memberData.type);
            statement.setString(4, memberData.inviter);
            statement.setLong(5, System.currentTimeMillis());
            statement.setString(6,memberData.receiverKey); //add by CESHI
            statement.setInt(7, memberData.status);//add by CESHI
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean insertMembers(Collection<MemberData> members) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_groupMember(osnID,groupID,type,inviter,createTime,receiverKey,status)" +
                    " values(?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            for (MemberData memberData : members) {
                statement.setString(1, memberData.osnID);
                statement.setString(2, memberData.groupID);
                statement.setInt(3, memberData.type);
                statement.setString(4, memberData.inviter);
                statement.setLong(5, System.currentTimeMillis());
                statement.setString(6,memberData.receiverKey);
                statement.setInt(7, memberData.status);
                statement.addBatch();
            }
            int[] counts = statement.executeBatch();
            connection.commit();
            return counts.length != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }


    public boolean insertMembers(List<MemberData> members) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_groupMember(osnID,groupID,type,inviter,createTime,receiverKey,status)" +
                    " values(?,?,?,?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            for (MemberData memberData : members) {
                statement.setString(1, memberData.osnID);
                statement.setString(2, memberData.groupID);
                statement.setInt(3, memberData.type);
                statement.setString(4, memberData.inviter);
                statement.setLong(5, System.currentTimeMillis());
                statement.setString(6,memberData.receiverKey);
                statement.setInt(7, memberData.status);
                statement.addBatch();
            }
            int[] counts = statement.executeBatch();
            connection.commit();
            return counts.length != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }


    public boolean setMemberType(MemberData member){

        List<String> keys = new ArrayList<>();
        keys.add("type");

        return update(member, keys);
    }

    public boolean setMemberStatus(MemberData member){

        List<String> keys = new ArrayList<>();
        keys.add("status");
        return update(member, keys);
    }

    public boolean setMemberNickName(MemberData member){
        List<String> keys = new ArrayList<>();
        keys.add("nickName");
        return update(member, keys);
    }



    public boolean update(MemberData member, List<String> keys) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            StringBuilder columns = new StringBuilder();
            for (String k : keys) {
                if (columns.length() != 0)
                    columns.append(",");
                if (k.equalsIgnoreCase("nickName"))
                    columns.append("nickName=?");
                else if (k.equalsIgnoreCase("remarks"))
                    columns.append("remarks=?");
                else if (k.equalsIgnoreCase("type"))
                    columns.append("type=?");
                else if (k.equalsIgnoreCase("status"))
                    columns.append("status=?");
                else if (k.equalsIgnoreCase("mute"))
                    columns.append("mute=?");
            }
            if (columns.length() == 0) {
                log.info("key error");
                return false;
            }
            String sql = "update t_groupMember set " + columns + " where osnID=? and groupID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int index = 1;
            for (String k : keys) {
                if (k.equalsIgnoreCase("nickName"))
                    statement.setString(index++, member.nickName);
                else if (k.equalsIgnoreCase("remarks"))
                    statement.setString(index++, member.remarks);
                else if (k.equalsIgnoreCase("type"))
                    statement.setInt(index++, member.type);
                else if (k.equalsIgnoreCase("status"))
                    statement.setInt(index++, member.status);
                else if (k.equalsIgnoreCase("mute"))
                    statement.setInt(index++, member.mute);
            }
            statement.setString(index++, member.osnID);
            statement.setString(index, member.groupID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean delete(String userID, String groupID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_groupMember where osnID=? and groupID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userID);
            statement.setString(2, groupID);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean deleteMembers(String groupID, List<String> members) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_groupMember where osnID=? and groupID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            for (String member : members) {
                statement.setString(1, member);
                statement.setString(2, groupID);
                statement.addBatch();
            }
            int[] counts = statement.executeBatch();
            connection.commit();
            return counts.length != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean deleteAll(String groupID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "delete from t_groupMember where groupID=" + groupID;
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }


    public List<MemberData> list(String groupID) {

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
                members.add(toData(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return members;
    }

    public List<MemberData> listAdmin(String groupID) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<MemberData> members = new ArrayList<>();
        try {
            String sql = "SELECT * FROM t_groupMember WHERE groupID=? AND TYPE=2 OR TYPE=3";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, groupID);
            rs = statement.executeQuery();
            while (rs.next())
                members.add(toData(rs));
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return members;
    }

    public MemberData read(String groupID, String userID) {

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
                memberData = toData(rs);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return memberData;
    }






    private MemberData toData(ResultSet rs) {
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

}
