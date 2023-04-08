package com.ospn.server;

import com.ospn.command.GroupNoticeData;
import com.ospn.data.GroupData;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;



import static com.ospn.core.IMData.getGroupData;
import static com.ospn.service.GroupService.systemNotifyGroup;

@Slf4j
public class GroupNoticeServer {
    public static final ConcurrentLinkedQueue<GroupNoticeData> noticeList = new ConcurrentLinkedQueue<>();

    public static int getQueueSize(){
        return noticeList.size();
    }

    public static void run(){
        while(true){
            try{
                if(noticeList.isEmpty()){
                    synchronized (noticeList){
                        noticeList.wait();
                    }
                }
                GroupNoticeData notice = noticeList.poll();
                if (notice == null) {
                    //log.info("error, queue poll data null");
                    continue;
                }

                GroupData group = getGroupData(notice.groupID);
                systemNotifyGroup(group, notice.toJson());

                Thread.sleep(10);

            }catch (Exception e){
                log.error("", e);
            }
        }
    }

    public static void push(GroupNoticeData notice){
        noticeList.offer(notice);
        synchronized (noticeList){
            noticeList.notify();
        }
    }




}
