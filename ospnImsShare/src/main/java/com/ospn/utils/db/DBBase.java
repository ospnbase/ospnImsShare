package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


@Slf4j
public class DBBase {
    public ComboPooledDataSource comboPooledDataSource;

    public DBBase(ComboPooledDataSource obj){
        comboPooledDataSource = obj;
    }

    public void closeDB(Connection connection, PreparedStatement statement, ResultSet rs) {
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

}
