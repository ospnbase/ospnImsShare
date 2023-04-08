package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.OsnIMServer;
import com.ospn.data.SessionData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


import static com.ospn.OsnIMServer.popOsnID;
import static com.ospn.core.IMData.getSessionData;

@Slf4j
public class OsnOSXServer extends SimpleChannelInboundHandler<JSONObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JSONObject json) throws Exception {
        String command = json.getString("command");
        if(command != null && command.equalsIgnoreCase("Heart"))
            ctx.writeAndFlush(json);
        else {
            SessionData sessionData = getSessionData(ctx, true, false, json);

            // 如果connector 发送来的消息不在则本节点，则丢弃消息，并pop
            if (sessionData.remote) {
                String osnId = getToId(json);
                if (osnId != null) {
                    if (!OsnIMServer.isInThisNodeContainTemp(osnId)) {
                        log.info("not in my node, osnId : " + osnId);
                        log.info("not in my node, json : " + json);
                        popOsnID(osnId);
                        return;
                    }
                }
            }

            OsnIMServer.Inst.handleMessage(sessionData, json);
        }
    }

    private String getToId(JSONObject json) {
        String toId = null;
        toId = json.getString("to");
        if (toId == null) {
            toId = json.getString("toID");
        }
        if (toId == null) {
            log.info("no osnID, bad json : " + json);
        }
        return toId;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }
}
