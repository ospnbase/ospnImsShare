package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.command2.CmdUserReg;
import com.ospn.data.UserData;
import com.ospn.utils.PerUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DBOSNUser extends DBBase {
    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_osn_user " +
                    "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
                    " osnID char(128) NOT NULL, " +
                    " sign text , " +
                    " regTime bigint, " +
                    " approverTime bigint, " +
                    " state tinyint DEFAULT 0, " +
                    " owner2 char(128) NOT NULL )";

    public DBOSNUser(ComboPooledDataSource data){
        super(data);
    }

    public CmdUserReg readBySign(String sign) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        CmdUserReg userData = null;
        try {
            String sql = "select * from t_osn_user where sign=?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, sign);
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

    public boolean isUndone(String user, long time){

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select * from t_osn_user where osnID=? and state=1 and approverTime>?";
            connection = comboPooledDataSource.getConnection();PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, user);
            statement.setLong(2, time);
            rs = statement.executeQuery();
            if (rs.next()){
                return false;
            }
            return true;

        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean insert(CmdUserReg userData) {

        //log.info("db test");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "insert into t_osn_user (osnID,sign,regTime,owner2) values(?,?,?,?)";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, userData.user);
            statement.setString(2, userData.sign);
            statement.setLong(3, userData.regTime);
            statement.setString(4, userData.owner2);
            int count = statement.executeUpdate();
            //OsnUtils.log.info(userData.osnID + ", name: "+userData.name);
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
            String sql = "delete from t_osn_user where osnID=?";
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


    public boolean updateState(CmdUserReg cmd){
        List<String> keyList = new ArrayList<>();
        keyList.add("approverTime");
        keyList.add("state");

        cmd.state = 1;

        return update(cmd, keyList);
    }

    public boolean update(CmdUserReg cmd, List<String> keys) {

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
                    case "approverTime":
                        columns.append("approverTime=?");
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
            String sql = "update t_osn_user set " + columns.toString() + " where osnID=?";
            connection = comboPooledDataSource.getConnection();PerUtils.writeDB();
            statement = connection.prepareStatement(sql);
            int index = 1;
            for (String k : keys) {
                switch (k) {
                    case "approverTime":
                        statement.setLong(index++, cmd.approverTime);
                        break;
                    case "state":
                        statement.setInt(index++, cmd.state);
                        break;

                }
            }
            statement.setString(index, cmd.user);
            int count = statement.executeUpdate();
            return count != 0;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return false;
    }

    public boolean save(CmdUserReg cmd){

        // cmd 转 UserData
        if (readBySign(cmd.sign) != null) {
            return false;
        }

        return insert(cmd);
    }

    private CmdUserReg toData(ResultSet rs) {
        try {
            CmdUserReg userData = new CmdUserReg();
            userData.user = rs.getString("osnID");
            userData.owner2 = rs.getString("owner2");
            userData.sign = rs.getString("sign");
            userData.regTime = rs.getLong("regTime");
            userData.approverTime = rs.getLong("approverTime");
            userData.state = rs.getInt("state");
            return userData;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
}
