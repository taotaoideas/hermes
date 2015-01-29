## 以MySql存储实现

### Create Tables
分别新建以下表格：

    1. msg_p_[topic]_low
    2. msg_p_[topic]_middle
    3. msg_p_[topic]_high
    4. msg_c_[topic]

其中[topic]为各申请的topic，即针对每个topic都将新建这些表。且由于mysql表名限制，所有topic必须是小写。

以msg\_p\_testtopic_low为例，具体表格内容为

| id (PK)| body | source_ip | token| ref_key|
|:--|:--|:--|:--|:--|
| 3322| (byte)basd29...| (long)192598292| appid-340101;sid-p0912| order-4423|

而msg\_c\_testtopic的具体表格内容为

| id (PK)| token (UNIQUE) | group_id | offset | priority|
|:--|:--|:--|:--|:--|
| 231| appid-340101;sid-c102|group_test1| 3322| high|

### SPI实现
0. 前提：各表自增的id不能断，否则会消费中断。暂未考虑有的Consumer时而作为Group消费，时而单个消费的情况。

1. Producer.append()
	
	    INSERT INTO msg_p_testtopic_high (body, source_ip, token, ref_key)
	    VALUES ('message body', '123940', 'appid-340101;sid-p1sdfj', 'order-421')

2. Consumer.read():

    1) 单个Consumer，即无Group_id：
	
	1. 获得该Consumer各优先级的消费进度：
	
			SELECT offset FROM msg_c_testtopic WHERE token = ? AND priority = 'high' AND group_id IS NULL
			SELECT offset FROM msg_c_testtopic WHERE token = ? AND priority = 'middle' AND group_id IS NULL
			SELECT offset FROM msg_c_testtopic WHERE token = ? AND priority = 'low' AND group_id IS NULL
			#若未取到，则取offset为消息表的最小id值。

	2. 对各优先级表，提取offset+1或(offset+1至offset+1+batchSize)的消息，进行消费：
	
			# 以high优先级为例，取出batchSize的消息。若不足，依次再取其他优先级的表。
			SELECT body, ref_key FROM msg_p_testtopic_high WHERE id = ? + 1 limit ?
	
	3. 若消费成功，则更新offset：
			
			INSERT INTO msg_c_testtopic (token, OFFSET, priority) 
			VALUES (?, ?, ?) 
			ON DUPLICATE KEY UPDATE OFFSET = ?			

    2) 作为Group的Consumer进行消费：
	1. 获得整个Group的消费进度，这里仅以高优先级举例，实际各优先级都需要查询进度。

			SELECT offset FROM msg_c_testtopic WHERE token = ? AND group_id = ? AND priority = 'high'
			#正常是拿到相应offset，若无结果则可能是新Consumer，没有旧消费记录。则继续：
			SELECT offset FROM msg_c_testtopic WHERE group_id = ? AND priority = 'high'
			#若无结果则说明该group未消费过，则取相应消息表的最小id值。
		取出的offset可能有多个，取其中的最大值。
	2. 依旧取消息，类似上文中的1).2
	3. 若消费成功，则更新offset:

			INSERT INTO msg_c_testtopic (token, group_id, OFFSET, priority) 
			VALUES (?, ?, ?, ?) 
			ON DUPLICATE KEY UPDATE OFFSET = ?


另附具体建表代码：

    # build msg_p_[topic]_high
	CREATE TABLE `msg_p_testtopic_high` (
		`id` INT(11) NOT NULL AUTO_INCREMENT,
		`body` BLOB NOT NULL,
		`source_ip` INT(11) NOT NULL,
		`token` VARCHAR(50) NOT NULL,
		`ref_key` VARCHAR(50) NULL DEFAULT NULL,
		PRIMARY KEY (`id`)
	)
	COMMENT='Topic: testtopic;\r\nPriority: high.'
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
	AUTO_INCREMENT=5;
	# build msg_p_[topic]_middle
	CREATE TABLE `msg_p_testtopic_middle` (
		`id` INT(11) NOT NULL AUTO_INCREMENT,
		`body` BLOB NOT NULL,
		`source_ip` INT(11) NOT NULL,
		`token` VARCHAR(50) NOT NULL,
		`ref_key` VARCHAR(50) NULL DEFAULT NULL,
		PRIMARY KEY (`id`)
	)
	COMMENT='Topic: testtopic;\r\nPriority: middle.'
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
	AUTO_INCREMENT=3;
	# build msg_p_[topic]_low
	CREATE TABLE `msg_p_testtopic_low` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`body` BLOB NOT NULL,
	`source_ip` INT(11) NOT NULL,
	`token` VARCHAR(50) NOT NULL,
	`ref_key` VARCHAR(50) NULL DEFAULT NULL,
	PRIMARY KEY (`id`)
	)
	COMMENT='Topic: TestTopic;\r\nPriority: log.'
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
	AUTO_INCREMENT=3;

    # build msg_c_[topic]
	CREATE TABLE `msg_c_testtopic` (
		`id` INT(11) NOT NULL AUTO_INCREMENT,
		`group_id` VARCHAR(50) NULL DEFAULT NULL,
		`offset` INT(11) NOT NULL COMMENT 'refer to msg_p_[topic]_[priority]:id',
		`token` VARCHAR(50) NOT NULL,
		`priority` ENUM('low','midddle','high') NOT NULL,
		PRIMARY KEY (`id`)
		UNIQUE INDEX `token` (`token`)
	)
	COMMENT='consume status on testtopic.'
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
	AUTO_INCREMENT=2;
