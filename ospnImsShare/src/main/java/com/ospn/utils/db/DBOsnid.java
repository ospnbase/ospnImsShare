package com.ospn.utils.db;

import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.data.Constant;
import com.ospn.data.GroupData;
import com.ospn.data.MessageData;
import com.ospn.utils.PerUtils;
import com.ospn.utils.RedisUtils;
import com.ospn.utils.data.RedisMessageInfo;
import com.ospn.utils.data.RedisMessageState;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

import static com.ospn.OsnIMServer.db;
import static com.ospn.data.MessageData.toMessageData;

@Slf4j
public class DBOsnid extends DBBase{
    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_osnid " +
                    "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
                    " type tinyint NOT NULL, " +
                    " osnID char(128) NOT NULL, " +
                    " timeStamp bigint NOT NULL, "+
                    " tip char(32), " +
                    " KEY `osnID` (`osnID`) USING BTREE" +
                    ")";

    public DBOsnid(ComboPooledDataSource data){
        super(data);
    }

    public List<String> getTip(String osnID) {

        List<String> tips = new ArrayList<>();

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            String sql = "select (tip) from t_osnid where osnID=?";
            connection = comboPooledDataSource.getConnection();
            PerUtils.readDB();
            statement = connection.prepareStatement(sql);
            statement.setString(1, osnID);
            rs = statement.executeQuery();
            if (rs.next()) {
                String tip = rs.getString("tip");
                tips.add(tip);
            }

        } catch (Exception e) {
            log.error("", e);
        } finally {
            closeDB(connection, statement, rs);
        }
        return tips;
    }

}
