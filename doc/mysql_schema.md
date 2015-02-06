## 以MySql存储实现

### Create Tables
分别新建以下表格：

	1. msg_low_[topic]       \\低优先级信息表
	2. msg_middle_[topic]	\\中优先级信息表
	3. msg_high_[topic]	  \\高优先级信息表
	4. resend_low_[topic]    \\重发的消息记录表
	5. resend_middle[topic]    \\重发的消息记录表
	6. resend_high_[topic]    \\重发的消息记录表
	7. offset_[topic]         \\group consumer消费记录表
	8. resend_offset_[topic]         \\group consumer消费记录表
	9. meta_token	  	   \\producer&consumer的token映射表

其中[topic]为各申请的topic，即针对每个topic都将新建这些表。且由于mysql表名限制，所有topic必须是小写。

以1.msg_low_testtopic为例，其他优先级表与之类似。具体表格内容为：

| id (PK)| body | source_ip | token| ref_key|  timestamp| properties|
|:--|:--|:--|:--|:--|:--| :--|
| 3322| (byte)basd29...| (long)192598292| 231| order-4423| 1422588646 | (byte)key:value|

4.resend_low_testtopic表格内容为：

| id (PK)| group_id |start| end | priority|timestamp|
|:--|:--|:--|:--|:--| :--|
| 58| testGroup1| 17| 17| high|1422588942 |

7.offset_testtopic的具体表格内容为：

| id (PK)| group_id (UNIQUE)| offset | priority| 
|:--|:--|:--|:--|
| 11|group_test1| 3322| high| 


8.resend_offset_testtopic的具体表格内容为：

| id (PK)| group_id (UNIQUE)| offset | priority|
|:--|:--|:--|:--|
| 11|group_test1| 3322| high|

9.meta_token表内容为：

| id (PK)| token (UNIQUE)| type | 
|:--|:--|:--|
| 231|appid-340101;sid-p0912| p| 
| 232|appid-340101;sid-c2819| c| 

### SPI实现
0. 前提：各表自增的id不能断，否则会消费中断。暂未考虑有的Consumer时而作为Group消费，时而单个消费的情况。

1. Producer.append()
	
	    INSERT INTO msg_p_testtopic_high (body, source_ip, token, ref_key, timestamp, properties)
	    VALUES ('message body', '123940', '231', 'order-421', 1422588646, "price:125")

2. Consumer.read():3.     
	1) 单个Consumer，即无Group_id，也就是作为非持久型消费：
	
	1. 消费进度取各消息表的id最大值。

	2. 对各优先级表，提取id+1至id+1+batchSize的消息，进行消费：
	
			# 以high优先级为例，取出batchSize的消息。若不足，依次再取其他优先级的表。
			SELECT body, ref_key, properties FROM msg_p_testtopic_high WHERE id > ? + 1 limit ?
	
    2) 作为Group的Consumer进行消费：

	1. 获得整个Group的消费进度，这里仅以高优先级举例，实际上各优先级都需要查询进度。

			SELECT offset FROM msg_c_testtopic WHERE group_id = ? AND priority = 'high'
			#若无结果则说明该group未消费过，则取相应消息表的最小id值。

	2. 取消息，类似上文中的
	
			# 以high优先级为例，取出batchSize的消息。若不足，依次再取其他优先级的表。
			SELECT body, ref_key, properties FROM msg_p_testtopic_high WHERE id > ? + 1 limit ?

	3. 若消费成功，则更新offset:

			INSERT INTO msg_c_testtopic (group_id, OFFSET, priority) 
			VALUES (?, ?, ?) 
			ON DUPLICATE KEY UPDATE OFFSET = ?

3. resend:
	1. 写入重发记录:
				
			INSERT INTO msg_testtopic_resend (token, group_id, START, END, priority, TIMESTAMP)
			VALUES (231, NULL, 1, 5, 'low', 1422588646)

	2. 读重发记录，重发消息（仅支持Group Consumer）
	类似consume消息，区别是resend_low_testtopic中存的是(start, end),指向msg_low_testtopic中消息的id

			#去resend
			_offset_testtopic取offset
			SELECT offset from resend_offset_testtopic where group_id = ? AND priority = 'low'
			#取各优先级resend待resend的消息range
			SELECT start, end FROM resend_low_testtopic WHERE id = ?
			#再去各消息表，获得消息重发
			SELECT body, ref_key, properties FROM msg_p_testtopic_high WHERE id IN (?,?,?)
	

----------

另附具体建表代码详见hermes-broker/test/resources/msg.sql

    
