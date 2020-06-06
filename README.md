## 简要介绍
采集链家杭州/宁波地区约3w条二手房，进行数据分析及训练模型房价预测，内容涵盖协程爬虫，邮件提示，代理IP，及Pandas函数运用。
采集完毕后可用如下语句导出数据到CSV文件
```
mongoexport -d House -c lianjiaNew -f houseCode,houseName,houseDirection,houseType,houseElevator,houseAddress,houseDistrict,houseRegion,houseFloor,houseSize,houseStatus,houseUnitPrice,houseAllPrice,houseYear,Latitude,Longitude --type=csv --out user.csv`
```

## 采集难点
1、超过100页只显示100页，即仅显示<3000条数据

**解决：**从每个区县的二手房入手，若某个区数量也大于100页，则再细分更小的地区。

## 使用说明
1、开启代理池，我这边用的猿人云，需要改成自己的SECRET
```
python yuanrenyun.py
```
2、启动爬虫
```
python lianjia_nb.py
```
3、分析详见data文件夹

## 遇到的问题
1、代理池运行期间，maximum number of running instances reached (1)

**解决：** `max_instances`默认值为1，它表示id相同的任务实例数，如scheduler.add_job(child_job, max_instances=10, id="example")。

2、logger对象打印两遍日志的问题

**解决：** propagate会把log record向根Logger传递，设置 propagate = False。

3、redis.exceptions.ConnectionError: Too many connections 

**解决**：当redis连接池最大连接数小于并发数的时候，多出来的并发数将会因为分配不到redis的资源而收到报错信息，所以需要调高max_connection。


