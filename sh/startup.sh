#!/bin/startup.sh
jarName=LotteryCapture.jar
ps -ef|grep -v grep|grep $jarName|while read u p o
do
 kill -9 $p
done
exec java -jar -Xms128m -Xmx256m $jarName &
