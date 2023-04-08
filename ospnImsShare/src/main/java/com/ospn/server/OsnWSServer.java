package com.ospn.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.data.SessionData;
import com.ospn.data.UserData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.websocketx.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

import static com.ospn.common.OsnUtils.*;
import static com.ospn.core.IMData.*;
import static com.ospn.utils.CryptUtils.toAesMessage;

@Slf4j
public class OsnWSServer extends SimpleChannelInboundHandler<Object> {
    private final ConcurrentHashMap<ChannelHandlerContext, WebSocketServerHandshaker> mHandshaker = new ConcurrentHashMap<>();


    public static void sendClientJson(UserData userData, JSONObject json){
        TextWebSocketFrame textWebSocketFrame = new TextWebSocketFrame(json.toString());
        userData.session.ctx.writeAndFlush(textWebSocketFrame);
    }
    public static void sendClientJson(SessionData sessionData, JSONObject json){
        TextWebSocketFrame textWebSocketFrame = new TextWebSocketFrame(json.toString());
        sessionData.ctx.writeAndFlush(textWebSocketFrame);
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg)  {
        WebSocketServerHandshaker handshaker;
        try {
            if (msg instanceof FullHttpRequest) {
                FullHttpRequest request = (FullHttpRequest) msg;
                WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory("ws://" + ipIMServer + ":" + imWebsockPort + "/websocket", null, false);
                handshaker = wsFactory.newHandshaker(request);
                if (handshaker == null)
                    WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
                else {
                    handshaker.handshake(ctx.channel(), request);
                    mHandshaker.put(ctx, handshaker);
                }
            } else if (msg instanceof WebSocketFrame) {
                if (msg instanceof CloseWebSocketFrame) {
                    handshaker = mHandshaker.get(ctx);
                    if (handshaker != null) {
                        CloseWebSocketFrame closeWebSocketFrame = (CloseWebSocketFrame) msg;
                        handshaker.close(ctx.channel(), closeWebSocketFrame.retain());
                    }
                }
                else if (msg instanceof PingWebSocketFrame) {
                    WebSocketFrame webSocketFrame = (WebSocketFrame) msg;
                    ctx.channel().write(new PongWebSocketFrame(webSocketFrame.content().retain()));
                }
                else if(msg instanceof TextWebSocketFrame){
                    TextWebSocketFrame textWebSocketFrame = (TextWebSocketFrame)msg;
                    JSONObject json = JSON.parseObject(textWebSocketFrame.text());
                    SessionData sessionData = getSessionData(ctx,false,true,json);
                    OsnIMServer.Inst.handleMessage(sessionData, json);
                }
                else if(msg instanceof BinaryWebSocketFrame){
                    BinaryWebSocketFrame binaryWebSocketFrame = (BinaryWebSocketFrame)msg;
                    byte[] bData = binaryWebSocketFrame.content().array();
                    int length = ((bData[0]&0xff)<<24) | ((bData[1]&0xff)<<16) | ((bData[2]&0xff)<<8) | bData[3]&0xff;
                    if(length != bData.length-4)
                        log.info("length no equel: "+length+" != "+bData.length);
                    else {
                        JSONObject json = JSON.parseObject(new String(bData, 4, bData.length - 4));
                        SessionData sessionData = getSessionData(ctx,false,true,json);
                        OsnIMServer.Inst.handleMessage(sessionData, json);
                    }
                }
                else
                    log.info("unknown frame: "+msg.toString());
            } else
                log.info("unknown instance");
        }
        catch (Exception e){
            log.error("", e);
        }
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        SessionData sessionData = getSessionData(ctx);
        if(sessionData != null) {
            UserData userData = sessionData.user;
            log.info("user: " + (userData == null ? "null" : userData.name));
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
