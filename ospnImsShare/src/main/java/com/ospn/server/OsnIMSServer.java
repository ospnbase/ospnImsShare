package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.data.RunnerData;
import com.ospn.data.SessionData;
import com.ospn.data.UserData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;


import static com.ospn.core.IMData.*;

/**
 * child handle
 *
 * **/
@Slf4j
public class OsnIMSServer extends SimpleChannelInboundHandler<JSONObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JSONObject json) {


        JSONObject json2 = new JSONObject();
        json2.putAll(json);

        SessionData sessionData = getSessionData(ctx,false,false,json2);
        addSession(sessionData);


        OsnIMServer.Inst.handleMessage(sessionData, json2);
        ctx.fireChannelRead(json);
        ReferenceCountUtil.release(json);


    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        SessionData sessionData = getSessionData(ctx);
        if(sessionData != null) {
            UserData userData = sessionData.user;
            //log.info("user: " + (userData == null ? "null" : userData.name));
            delSessionData(sessionData);
        } else {
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }
}

/*
public class OsnIMSServer extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf)msg;
        int len = byteBuf.readableBytes();
        if (len == 4) {
            byte[] head = new byte[4];
            byteBuf.readBytes(head);
            return;
        }
        log.info("[channelRead] length : " + len);
        */
/*byte[] head = new byte[4];
        byteBuf.readBytes(head);
        int length = ((head[0]&0xff)<<24) | ((head[1]&0xff)<<16) | ((head[2]&0xff)<<8) | head[3]&0xff;
        log.info("[channelRead] length : " + length);*//*


        String str = byteBuf.toString(CharsetUtil.UTF_8);

        JSONObject json = JSONObject.parseObject(str);
        log.info("[channelRead] json : " + json);

        SessionData sessionData = getSessionData(ctx,false,false, json);
        addSession(sessionData);
        OsnIMServer.Inst.handleMessage(sessionData, json);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.info(cause.getMessage());
        ctx.close();
    }

}
*/

