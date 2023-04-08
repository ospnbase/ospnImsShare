package com.ospn.utils.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ospn.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class DBConversation extends DBBase {

    RedisUtils redis;

    public static String sql =
            "CREATE TABLE IF NOT EXISTS t_conversation " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " userID char(128) NOT NULL, " +
            " target char(128) NOT NULL, " +
            " data text NOT NULL, " +
            " createTime bigint, " +
            " unique index i_conversation(userID,target))";

    public static String CONV = "conver";
    public static String REDISFLAG = "LOGINTOKEN";

    public DBConversation(ComboPooledDataSource data, RedisUtils data2){
        super(data);
        redis = data2;
    }


    // 获取的是target
    public List<String> list(String userID) {

        Jedis jedis = redis.getJedis();
        if (jedis == null){
            return null;
        }
        boolean ret = false;
        List<String> targets = null;

        try{
            Set<String> tmp = jedis.smembers(CONV+userID);
            if (tmp != null){
                targets = new ArrayList<>(tmp);
            }
            // set userID set 用list会重复
            ret = true;
        } catch (Exception e){
            log.info("[DBConversation::list] error: " + e.getMessage());
        }

        redis.close(jedis);
        return targets;
    }


    public boolean set(String userID, String target, String data) {

        Jedis jedis = redis.getJedis();
        if (jedis == null){
            return false;
        }
        boolean ret = false;

        try{

            // set userID set 用list会重复
            jedis.sadd(CONV+userID, target);
            jedis.set(CONV+userID+target, data);

            ret = true;
        } catch (Exception e){
            log.info("[DBConversation::set] error: " + e.getMessage());
        }

        redis.close(jedis);
        return ret;
    }

    // 获取data
    public String get(String userID, String target) {

        Jedis jedis = redis.getJedis();
        if (jedis == null){
            return null;
        }
        boolean ret = false;
        String data = null;

        try{
            data = jedis.get(CONV+userID+target);

            ret = true;
        } catch (Exception e){
            log.info("[DBConversation::get] error: " + e.getMessage());
        }

        redis.close(jedis);
        return data;
    }

    public boolean del(String userID, String target) {

        Jedis jedis = redis.getJedis();
        if (jedis == null){
            return false;
        }
        boolean ret = false;

        try{

            // set userID set 用list会重复
            jedis.srem(CONV+userID, target);
            jedis.del(CONV+userID+target);

            ret = true;
        } catch (Exception e){
            log.info("[DBConversation::del] error: " + e.getMessage());
        }

        redis.close(jedis);
        return ret;
    }

    public boolean setLoginToken(Jedis jedis, String userID, String token) {

        try {

            if (jedis == null) {
                log.info("jedis is null");
            }
            //jedis.del(REDISFLAG+userID);
            //log.info("set token " + REDISFLAG+userID);
            jedis.set(REDISFLAG+userID, token);

            return true;

        } catch (Exception e) {
            log.error("", e);
            log.info("user id : " + userID);
            log.info("token : " + token);
        }
        return false;
    }

    public String getLoginToken(Jedis jedis, String userID) {

        if (jedis == null) {
            log.info("jedis is null");
        }

        //log.info("get token " + REDISFLAG+userID);
        return jedis.get(REDISFLAG+userID);
    }

}
