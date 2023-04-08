package com.ospn.utils;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Properties;


@Slf4j
public  class RedisUtils {
    //Redis服务器IP
    private String ADDR;// = "127.0.0.1";

    //Redis的端口号
    private int PORT;// = 6379;

    //访问密码
    private String AUTH;// = "pass1234";

    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_TOTAL = 512;

    //最小空闲连接数, 默认0
    private static int MIN_IDLE=32;

    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    //最大空闲连接数, 默认8个
    private static int MAX_IDLE = 64;

    //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = -1;

    private static int TIMEOUT = 1000000;

    //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
    private static boolean BLOCK_WHEN_EXHAUSTED = true;

    //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
    private static String EVICTION_POLICY_CLASSNAME="org.apache.commons.pool2.impl.DefaultEvictionPolicy";

    //是否启用pool的jmx管理功能, 默认true
    private static boolean JMX_ENABLED=true;

    //MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
    private static String JMX_NAME_PREFIX="pool";

    //是否启用后进先出, 默认true
    private static boolean LIFO=true;

    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
    private static long MIN_EVICTABLE_IDLE_TIME_MILLIS=1800000L;

    //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
    private static long SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS=1800000L;

    //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
    private static int NUM_TESTS_PER_EVICYION_RUN=3;

    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    //在获取连接的时候检查有效性, 默认false
    private static boolean TEST_ON_BORROW = false;

    //在空闲时检查有效性, 默认false
    private static boolean TEST_WHILEIDLE=false;

    //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
    private static long TIME_BERWEEN_EVICTION_RUNS_MILLIS=-1;

    private static JedisPool jedisPool = null;

    /**
     * 初始化Redis连接池
     */

    public RedisUtils(String configFile){
        try {

            Properties prop = new Properties();
            prop.load(new FileInputStream(configFile));
            ADDR = prop.getProperty("RedisAddr");
            PORT = Integer.parseInt(prop.getProperty("RedisPort"));
            AUTH = prop.getProperty("RedisAuth");


            JedisPoolConfig config = new JedisPoolConfig();
            config.setBlockWhenExhausted(BLOCK_WHEN_EXHAUSTED);
            //config.setMaxWaitMillis(MAX_WAIT);
            config.setMaxWait(Duration.ofMillis(MAX_WAIT));
            config.setEvictionPolicyClassName(EVICTION_POLICY_CLASSNAME);
            config.setJmxEnabled(JMX_ENABLED);
            config.setJmxNamePrefix(JMX_NAME_PREFIX);
            config.setLifo(LIFO);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxTotal(MAX_TOTAL);

            //config.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIME_MILLIS);
            config.setMinEvictableIdleTime(Duration.ofMillis(MIN_EVICTABLE_IDLE_TIME_MILLIS));
            config.setMinIdle(MIN_IDLE);
            config.setNumTestsPerEvictionRun(NUM_TESTS_PER_EVICYION_RUN);
            //config.setSoftMinEvictableIdleTimeMillis(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
            config.setSoftMinEvictableIdleTime(Duration.ofMillis(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS));
            config.setTestOnBorrow(TEST_ON_BORROW);
            config.setTestWhileIdle(TEST_WHILEIDLE);
            //config.setTimeBetweenEvictionRunsMillis(TIME_BERWEEN_EVICTION_RUNS_MILLIS);
            config.setTimeBetweenEvictionRuns(Duration.ofMillis(TIME_BERWEEN_EVICTION_RUNS_MILLIS));

            log.info("redis ip : " + ADDR);
            log.info("redis port : " + PORT);
            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH);

        } catch (Exception e) {
            log.info("[RedisUtils] user : " + e);
        }
    }

    /**
     * 获取Jedis实例
     * @return
     */
    public Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            log.info("[getJedis] error: " + e.getMessage());
            //e.printStackTrace();
            return null;
        }
    }

    /**
     * 释放jedis资源
     * @param jedis
     */
    public void close(final Jedis jedis) {

        try {

            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            log.info("[close] error: " + e.getMessage());
        }

    }
}
