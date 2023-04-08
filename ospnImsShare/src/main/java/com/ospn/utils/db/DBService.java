package com.ospn.utils.db;

public class DBService {

    public static String sql = "CREATE TABLE IF NOT EXISTS t_service " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
            " osnID char(128) NOT NULL, " +
            " privateKey char(255) NOT NULL, " +
            " createTime bigint)";



}
