demo运行
本项目因为有大量配置文件，要使用本项目需要将该模块从项目中拷贝出来，形成一个新的项目。

然后因为路网文件非常大，所以在本地运行项目前，需要先把路网文件考到data目录底下
格式示例：F:\code\172.20.104.248-svn\code\gd-road\branches\RealStormTrafficOffLine_20170726_zsr\src\main\resources\data\guangdong_polyline\shp文件
该shp文件的下载地址为：172.20.104.248 Z:\upload\shp文件\guangdong_polyline 公司ftp
用户名：guest
密码：guest

该项目作用是将有序离线gps数据进行道路匹配，单点多点速度匹配。

程序拷贝出来后，运行com.sibat.traffic.local下的*HistoryData会在 src/main/resources/data/out中得到运行结果
项目自带了数据，在 src/main/resources/data中

批量使用
该程序也实现了参数化，在util/Cfg.java中找到参数类型，在src/main/resources下.properties文件修改具体值
1.编辑一个shell脚本内容如

这个行代码拼接输入文件名包括路径
fileName=/data/datum/coach/2015-06-$1
echo ${fileName}

执行jar文件，四种数据四个主函数更改jar文件的类名如com.sibat.traffic.ProvRoadHistoryData，com.sibat.traffic.TaxiHistoryData，com.sibat.traffic.E6HistioryData 或者
java -cp RealStormTrafficOffLine-1.0-SNAPSHOT-jar-with-dependencies.jar com.sibat.traffic.TaxiHistoryData ${fileName} >test.log

2.运行命令，循环运行脚本文件
for i in {01,02,03,04,05,06,07}; do sh start_shell.sh $i;done;
