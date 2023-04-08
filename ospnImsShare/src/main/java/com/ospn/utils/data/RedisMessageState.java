package com.ospn.utils.data;

public class RedisMessageState {
    public int state;
    public long time;
    public String hash;

    public RedisMessageState(){

    }
    public RedisMessageState(int state){
        this.state = state;
    }

    public RedisMessageState(int state, String hash){
        this.state = state;
        this.hash = hash;
    }
}
