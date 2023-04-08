#!/bin/bash
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH
ps -ef | grep ospnIMS | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep ospnConnector | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 3
nohup java -cp .:ospnConnector.jar com.ospn.OsnConnector &
nohup java -cp .:ospnIMS.jar com.ospn.OsnIMServer &