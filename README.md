# ts-benchmark

时间序列数据库基准测试工具  
ts-benchmark是用来测试时序数据库读写性能的测试工具

### 目前支持功能



1. 单线程导入数据
1. 多线程导入数据
1. 零负载响应时间
1. 混合负载下的吞吐量，响应时间
1. 导出对比图表


### Runtime Requirements
- A Linux system 
- Java Runtime Environment 1.8
- MAVEN 
- GIT1.7.10 or later 
- 当测试对象为tsfileDB，需要安装TsfileDB的JDBC

### Getting Started Simply




```
git clone https://git.oschina.net/myxof/TsFileDB-JDBC.git
cd TsFileDB-JDBC
git checkout issue_IEGJI
mvn clean install -Dmaven.test.skip=true

git clone https://git.oschina.net/zdyfjh2017/ts-benchmark.git
linux   
cd build
./starup.sh import tsfile -import.is.cc true  -dn 2 -sn 10 -ps 7000 -lcp 50000 -p tsfile.url=jdbc:tsfile://127.0.0.1:6667/#多线程写入数据   
windows   
mvn clean package -Dmaven.test.skip=true   
starup.bat  import tsfile  -dn 2 -sn 10 -ps 7000 -lcp 50000 -p tsfile.url=jdbc:tsfile://127.0.0.1:6667/   
 ```   
``` ./starup.sh perform tsfile -modules throughput  -tp.sum 1000000 -tp.ps.max 1000000 -tp.clients 1000  -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/ #吞吐量测试，一共发送1000000个请求，每秒最多发送 1000000个，客户端数为1000个```   
```./starup.sh perform tsfile -modules timeout  -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/#无负载响应时间测试```    
``` ./export_chart.sh #导出测试结果```   
 


### 参数描述   


- 第一个参数，程序运行```import``` 数据导入,```perform``` 性能测试   
- 第二个参数，目标测试数据库 目前支持四个参数 ```tsfile```,```opentsdb```,```cassandra```,```influxdb```   
- 其余参数分为两类  
第一类 程序运行参数   
```import``` 模式下的参数   
```-import.is.cc true``` 是否多线程导入,true 多线程写入，false单线程导入，```-dn 10``` 设备数，```-sn 50``` 传感器数，```-ps 7000```  数据采集间隔时间，单位为毫秒 ， ```-lcp 50000``` 一次调用插入接口所插入的数据，```-plr 0.01``` 传感器数据丢失率，默认0.01，```-flr 0.054``` 线性函数率，默认0.054， ```-fsir 0.036``` 正弦函数率，默认 0.036， ```-fsqr  0.054```方波率，默认0.054  ， ```-frr 0.512```随机函数率，默认0.512 ， ```-fcr 0.352``` 常数率，默认0.352；   
 函数比率只要有一个填了，其余未配置的比率全部默认为0。  
```perform``` 模式下的参数   
 ```-modules ``` 功能选择，```-modules timeout```  测试无任何负载下的响应时间，不需要配置其他任何程序运行参数```-modules throughput```  测试混合任何负载下的响应时间,负载比例可配置, ```-tp.ratio.write 0.2``` 写入比例0.2,```-tp.ratio.rwrite 0.2``` 写入历史数据比例0.2，```-tp.ratio.update 0.2``` 更新比例0.2,```-tp.ratio.sread 0.2``` 简单查询比例0.2，```-tp.ratio.aread 0.2```分析查询比例0.2.   
负载比率只要有一个填了，其余未配置的比率全部默认为0。

第二类 目标数据库参数```-p key=value```  
 tsfile:   
``` -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/```  tsfiledb的jdbc url   
influxdb:   
 ```-p influxdb.url=http://127.0.0.1::8086  ``` influxdb数据库url  
  ```-p influxdb.database=ruc_test1 ```  influxdb测试数据库database名称    
opentsdb:    
  ```-p OpenTSDB.url=http://127.0.0.1::4242/  ``` opentsdb数据库url   
cassandra:   
 ```-p Cassandra.url=127.0.0.1 ```    cassandra数据库url