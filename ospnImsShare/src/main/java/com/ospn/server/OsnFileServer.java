package com.ospn.server;

import com.alibaba.fastjson.JSONObject;
import com.ospn.common.OsnUtils;
import com.ospn.core.IMData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.*;
import io.netty.handler.stream.ChunkedFile;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;



import static com.ospn.core.IMData.*;
import static com.ospn.utils.HttpUtils.*;

@Slf4j
public class OsnFileServer extends SimpleChannelInboundHandler<HttpObject> {
    private final String ipIMServer;
    private final String dnsIMServer;
    private final HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
    private final ConcurrentHashMap<ChannelHandlerContext,SessionMap> decoderMap = new ConcurrentHashMap<>();

    private static class SessionMap{
        public String osnID;
        public String resType;
        public String resName;
        public String token;
        HttpPostRequestDecoder decoder;
    }

    public OsnFileServer(){
        ipIMServer = IMData.prop.getProperty("ipIMServer");
        dnsIMServer = IMData.prop.getProperty("dnsIMServer");
    }

    void download(ChannelHandlerContext ctx, HttpObject httpObject){
        try{
            HttpRequest httpRequest = (HttpRequest)httpObject;
            QueryStringDecoder decoder = new QueryStringDecoder(httpRequest.uri());
            if(!decoder.parameters().containsKey("path")){
                sendError(ctx,HttpResponseStatus.BAD_REQUEST);
                log.info(HttpResponseStatus.BAD_REQUEST.toString());
                return;
            }
            List<String> paths = decoder.parameters().get("path");
            String key = paths.get(0);
            log.info("key: "+key);

            String path = getDir(key);
            File file = new File(path);
            if(!file.exists()) {
                sendError(ctx, HttpResponseStatus.NOT_FOUND);
                log.info("file no found: "+path);
                return;
            }
            RandomAccessFile randomAccessFile;
            try{
                randomAccessFile = new RandomAccessFile(file, "r");
            }catch(Exception e){
                sendError(ctx, HttpResponseStatus.NOT_FOUND);
                log.error("", e);
                return;
            }
            long offset = 0;
            long length = randomAccessFile.length();

            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            if(httpRequest.headers().contains("Range")){
                String[] range = httpRequest.headers().get("Range").split("=")[1].split("-");
                offset = Long.parseLong(range[0]);
                if(range.length == 2)
                    length = Long.parseLong(range[1]);
                response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");
                response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes "+offset+"-"+length+"/"+randomAccessFile.length());
                response.setStatus(HttpResponseStatus.PARTIAL_CONTENT);
                length -= offset;
            }
            else {
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, length);
            }
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            response.headers().set("Access-Control-Allow-Origin", "*");
            ctx.write(response);

            ChannelFuture sendFileFuture = ctx.write(new ChunkedFile(randomAccessFile, offset, length, 8192), ctx.newProgressivePromise());
            sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
                @Override
                public void operationComplete(ChannelProgressiveFuture future) {
                    log.info("key: "+key+" -- end");
                    try{
                        randomAccessFile.close();
                    }
                    catch (Exception e){
                        log.error("", e);
                    }
                }
                @Override
                public void operationProgressed(ChannelProgressiveFuture future,long progress, long total) {
                    //log.info("key: "+key+", "+progress+"/"+total);
                }
            });
            //ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
        catch (Exception e){
            log.error("", e);
            sendError(ctx,HttpResponseStatus.BAD_REQUEST);
        }
    }
    void upload(ChannelHandlerContext ctx, HttpObject httpObject){
        try{
            if (httpObject instanceof HttpRequest) {
                HttpRequest httpRequest = (HttpRequest) httpObject;
                HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(factory, httpRequest);
                //decoder.setDiscardThreshold(0);
                SessionMap sessionMap = new SessionMap();
                sessionMap.decoder = decoder;
                HttpHeaders httpHeaders = httpRequest.headers();
                if(httpHeaders != null){
                    sessionMap.osnID = httpHeaders.get("osnID");
                    sessionMap.resType = httpHeaders.get("resType");
                    sessionMap.resName = httpHeaders.get("resName");
                }
                decoderMap.put(ctx, sessionMap);
            }
            else if(httpObject instanceof HttpContent){
                HttpContent httpContent = (HttpContent)httpObject;
                SessionMap sessionMap = decoderMap.get(ctx);
                if(sessionMap == null)
                    return;
                HttpPostRequestDecoder decoder = sessionMap.decoder;
                if(decoder == null) {
                    log.error("decoder == null");
                    return;
                }
                decoder.offer(httpContent);
                while (decoder.hasNext()) {
                    InterfaceHttpData data = decoder.next();
                    if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
                        Attribute attribute = (Attribute) data;
                        log.info("name: " + attribute.getName());
                    } else if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
                        FileUpload fileUpload = (FileUpload) data;
                        if (fileUpload.isCompleted()) {
                            String key = getKey(sessionMap.resType, fileUpload.getFilename() + System.currentTimeMillis());
                            File file = new File(getDir(key));
                            if(fileUpload.isInMemory()){
                                FileOutputStream fileOutputStream = new FileOutputStream(file);
                                ByteBuf buf = fileUpload.getByteBuf();
                                if(buf.hasArray())
                                    fileOutputStream.write(buf.array());
                                else{
                                    byte[] bytes = new byte[buf.readableBytes()];
                                    buf.getBytes(buf.readerIndex(), bytes);
                                    fileOutputStream.write(bytes);
                                }
                                fileOutputStream.flush();
                                fileOutputStream.close();
                            }
                            else{
                                if(!fileUpload.getFile().renameTo(file))
                                    log.info("failed rename to file: " + file.getAbsolutePath());
                            }
                            JSONObject json = new JSONObject();
                            json.put("url", (isSsl() ? "https://" : "http://") +
                            (dnsIMServer == null || dnsIMServer.isEmpty() ? ipIMServer : dnsIMServer) +
                            ":" + imHttpPort + "/?path=" + key);

                            sendReply(ctx, json);

                            log.info("key: " + key + ", length: " + fileUpload.length() + " --- end");
                            log.info("url: " + json.getString("url"));
                            decoderMap.remove(ctx);
                            decoder.cleanFiles();
                        }
                    }
                }
            }
        }
        catch (Exception e){
            log.error("", e);
        }
    }
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject httpObject) {
        try {
            if (httpObject instanceof HttpRequest) {
                HttpRequest httpRequest = (HttpRequest) httpObject;
                if(httpRequest.method() == HttpMethod.POST)
                    upload(ctx, httpObject);
                else if(httpRequest.method() == HttpMethod.OPTIONS)
                    sendReply(ctx, new JSONObject());
                else
                    download(ctx, httpObject);
            }
            else if(httpObject instanceof HttpContent)
                upload(ctx, httpObject);
        }
        catch (Exception e){
            log.error("", e);
        }
    }
    public void channelInactive(ChannelHandlerContext ctx) {
        //decoderMap.remove(ctx);
        ctx.close();
    }
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }
}
