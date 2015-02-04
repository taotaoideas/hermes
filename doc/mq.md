# 准备
## 如何合理划分Topic
topic是消息的一种组织方式，topic的划分和代码的组织一样需要遵循高内聚低耦合的原则，即从业务角度看，属于同一类型的消息可以放置在同一个topic中。

过于细粒度的topic划分会给降低topic的易用性，因为producer和consumer需要发送到和订阅更多的topic。而过于粗粒度的topic划分则可能会降低消息送达的实时性以及增加客户端处理的复杂度，如果consumer只关心某个topic中1%的消息，则需要花费大量的时间来进行消息的传输和过滤。

因此，常见的topic一般都是名词，而不是动词，某个名词的所有动作都可以用一个topic来承载，然后在消息内部通过tag属性进行具体区分。如果一个topic内部的消息子类型过多时则可以考虑使用多个topic。

## Topic申请
### 申请信息
* 名称([Topic名称规范](#topic_naming))
* 内容和用途说明
* 峰值QPS
* 每日消息量
* 消息长度
* 消息保留时间
* 数据库事务配置
* 是否持久化

### 申请结果
* 准备好相应的后端存储和MQ Broker


## ID申请
### 申请信息
* Topic名称
* BU
* 项目
* 负责人

### 申请结果
* MessageQueue User ID(以下简称QUID)



## Producer申请
### 申请信息
* Topic名称
* QUID
* Group ID

### 申请结果
* QUID可以发送消息到指定的Topic



## Consumer申请
### 申请信息
* Topic名称
* QUID
* Group ID

### 申请结果
* QUID可以接收指定Topic的消息

# 整体架构
(ToDo: 包括名字解释等)

# 开发
## Features
| Features      | MessageQueue 0.1 | MessageQueue Final  | [JMS 2.0][JMS_API] | RabbitMQ | [Kafka][Kafka_Url]| ActiveMQ|
| :---------------- |:--|:--|:--|:--|:--|:--|
| Publish/Subscribe | √ | √ | √ | √ | √  | √ |
| Message Priority  | √ | √ | √ | √ (API: BasicProperties.setPriority()) | ×(FIFO)| √ |
| At Least Once     | √ | √ | √ | √ (by Ack in both directions. [Link[Rabbit_At_least_once]) | √("Kafka guarantees at-least-once delivery by default")| √ |
| Consumer Group    | √ | √ | √ (API:Session.createSharedConsumer()) | √ (As "Clustering" or "Federation") | √ | √([Link][ActiveMQ_Consume_Group]) |
| Message Ordering  | × | × | × ([Link][JMS_Ordering]) | √ | √ ("total order over messages within a partition") | √ ([Link][ActiveMQ_Ordering]) |
| Re-consume        | × | √ | ? | × ( any messages "in wait" (persistent or not) are held opaquely (i.e. no cursor)) | √ [Link][Kafka_Re-consume] | ? |
| Transaction       | × | √ | √ ([Link][JMS_Transaction]) | √ ([Link][RabbitMQ_Transaction]: "only when transactions involve a single queue")| × | √ |
| Nack & Resend     | √ | √ | √ (by [configuration][JMS_Nack]) | √ (API: basicRecover():Ask the broker to resend unacknowledged messages.)| × | √ |
| Delivery Delay    | × | √ | √ (2.0后引入)| × | × | √ |
| Query Message     | × | √ | √ (can be done by Message Selector [Link][JMS_Query])| × (Can be done via Management Plugin) | × | √ |
| Selector/Filter   | × | √ | √ | √ | √ (By "TopicFilter" in High-level API) | √ |
| Wildcards         | × | × | × | √ | ? | √ |

## API

### Producer
#### Config （Todo: 根据实际，增加更多的配置信息）
需配置Topic, ProducerToken（运维分配的认证标识）,Producer模式，消息的TTL等。

	ProducerConfig config = new ProducerConfig();
	config.setTopic(new Topic("Order"));
	config.setProducerToken("APPID:340101;UID:P-888666;SID:$sflk@ai1234");
	config.setProducerMode(ProducerMode.SYNC); // sync or async Producer
	config.setTTL(24 * 3600);
	...
	Producer producer = MQFactory.creatProducer(config); 
	producer.send(new Message("A New Order"));

#### 同步发送：
以同步的方式发，发送出错会抛出异常。
Producer应配置为：

	config.setProducerMode(ProducerMode.SYNC);
**适用场景**:要等到服务器响应（甚至完成储存）才会返回，会有一定等待时间，适用发送消息数量较小，同步要求高的场景。

	//Sync Producer	
	try {
		producer.send(new Message("Sync Send");
	} catch (Exception ex) {
		// handle the send exeption...
	}

#### 异步发送：

使用MessageHook处理发送结果；
Producer应配置并添加MessageHook：

	...
	config.setProducerMode(ProducerMode.ASYNC);
	...
	Producer producer = MQFactory.creatProducer(config);
	producerr.addMessageHook(new MessageHook(){
		    @Override
			public void onSendSuccess(MessageContext context) {
		        ...
		    }
		    @Override
		    public void onSendFail(MessageContext context) {
		       ...
		    }
	});

**适用场景**：需大量发送信息，且可使用Callback对于发送失败的消息重新处理，例如订单系统等。

	//ASync Producer	
	producer.send(new Message("ASync Send"));

#### 消息优先级
单个Topic的发送不同优先级的Message。Consumer默认先收到高优先级的Message。

**适用场景**：需要区分消息重要性的场景。

	producer.send(new Message("Heartbeat", MessagePriority.LOW));
	producer.send(new Message("Fatal Exception" , MessagePriority.HIGH));

#### 消息存储可靠性
![](file/producer_storage.png)

异步发送时，Producer将消息存储于内存或磁盘文件中，再批量发送出去。磁盘文件模式可提供更高的可靠性。默认储存于内存中。具体配置如下：

	...
	config.setStorageToDisk(false); //default, storage to memory.
	config.setStorageToDisk(true); //storage to disk file.
	...

### Consumer
#### Config
例配置Topic，Token，AckMode，设置MessageListener，start()，则完成一个Consumer。

	ConsumerConfig config = new ConsumerConfig();
	config.setTopic(new Topic("Order"));
	config.setConsumerToken("APPID:340101;UID:C-777555;SID:as5Yfo2aAdw1");
	config.setAckMode(ConsumerMode.NO_ACK); // ack or no-ack consumer;
	...
	Consumer consumer = MQFactory.createConsumer(config);
	 
	consumer.setMessageListener(new MessageListener() {
	    @Override
	    public void onMessage(Message msg) {
	        System.out.println("Received: " + msg);
	    }
	});
	consumeer.start();

#### ack mode

Consumer需设置Ack模式：ack或no-ack。

**ack模式**: 收到消息(即onMessage函数处理无异常后）后自动ack，确认消息消费完成。若未ack，则超时重发。

	ConsumerConfig config = new ConsumerConfig();
	...
	config1.setAckMode(ConsumerMode.ACK);
	
	Consumer consumer =MQFactory.createConsumer(config)
	consumer.setMessageListener(new MessageListener() {
		@Override
		public void onMessage(Message msg) {
		// Handle the message...
	    }
	});

**no-ack模式**: Server端自动ack，即不保证消息一定到达，不保证消息一定消费成功。

	ConsumerConfig config = new ConsumerConfig();
	...
	config.setAckMode(ConsumerMode.NO_ACK);

#### nack机制

一条消费失败时，应调用Message.nack()，在一段等待时间（Topic申请阶段配置）之后使Server重发。在ack模式，默认是Client超时未ack()，Server端则认为消费失败；或Clinet端主动nack()则Server端立刻确认消费失败。在no-ack模式，nack()可以主动标记消费失败。

	Consumer consumer =MQFactory.createConsumer(config)
	consumer.setMessageListener(new MessageListener() {
		@Override
		public void onMessage(Message msg) {
		try{
		// Handle the message...
		} catch (Exception ex) {
		msg.nack();
		}
	    }
	});
#### Consumer Group
![](file/consumer_group.png)

多个Consumer组合成为一个Group，消费同一个Topic，自动做Fail-Over，和Load Balance。在同一个Group内，一条消息只会到达其中一个Consumer。

**适用场景**：消息量大，且需要保证高可用的场景。

	ConsumerConfig config = new ConsumerConfig();
	...
	config.setGroup("Backgroud_Job_Group");

## 典型使用场景
1. Producer(ASync) to Consumer(NO_ACK)
如日志发送系统，发送量大但对信息可靠性要求不高。

2. Producer(ASync) to Consumer(ACK)
如订单处理，发送量大且对信息可靠性要求非常高。（注：需Consumer端去重）

3. Multiple Topic Produce
如登录系统，可能发消息给风控、登录认证、营销等多个Topic。

4. Multiple Topic Consume
如用户分析系统，可能收取用户登录、用户消费、用户浏览等多个Topic。

5. 应用实战：火车票客服聊天系统利用Message Queue实现。

## 开发阶段工具
### 单机开发 
（ToDo:支持dev,QA,用户--发端与收端等角色的单机开发需求）
## Client与Broker的交互机制
### 主动连接
Client可以连接到任意一台Broker，Broker会根据请求信息(Topic、Group ID、Producer/Consumer)告知Client真正需要连接到的Broker地址，从而简化Client的配置。

1. Client获取种子Broker地址列表BLA
2. 连接列表中的任意一台Broker，发送Topic和Group ID等信息
3. Broker返回可用于此连接请求的Broker地址列表BLB
4. Client连接或负载均衡到BLB

### 被动重连
当Broker负载过高或者需要下线维护时，Broker会主动通知Client重新连接到新的Broker。

## Broker之间负载均衡
### Consumer
需要保证同一个Topic的同一个Consumer Group的所有Client连接到同一个Broker，从而可以使用简单统一的机制来实现consumer group语义。Broker之间通过ZK来争夺特定(Topic, Consumer Group)的服务权。当Consumer Client连接到Broker时，Broker通过ZK获取(Topic, Consumer Group)的服务权，如果成功获取则接受连接，否则将ZK中记录的当前正在提供服务的Broker地址返回给Client，Client会重新连接到该新Broker。
### Producer
服务于Producer Client的Broker没有特殊的要求，Broker可以允许Client连接到任何正常状态的Broker。

# 维护
## Topic信息
![](file/topic_info.png "topic info")


## Producer信息
![](file/producer_info.png "producer info")


## Consumer信息
![](file/consumer_info.png "consumer info")


## 消息追踪
用户可以根据消息的[User-supplied ID](#user-supplied_id)来查询消息的发送和消费情况

![](file/msg_tracing.png "message tracing")



## Web端消息发送工具
提供各个环境的消息发送Web端，用户可以通过非编程的方式发送消息，适用于快速补发消息或者测试场合。

![](file/resend_msg.png "resend message")


## 消费起点调整
正常情况下，MQ会记录每个消费者消费进度，当消费者重新连接时会从上次的进度开始继续消费。某些情况下，会需要对消费进度进行调整，来达到"跳过"某一段消息或重新消费某一段消息的效果。这是一个相对复杂和危险的操作，因此不能在Client API中使用，需要使用MQ的管理端来完成。

![](file/consumer_reset.png "consumer reset")

# Appendix
<a id="topic_naming"></a>
## Topic名称规范
字母、数字、点号，总长度不超过

<a id="group_naming"></a>
## GroupID名称规范
字母、数字、点号，总长度不超过

<a id="user-supplied_id"></a>
## User-supplied ID
用户可以为每个消息设置User-supplied ID(简称USID)，类型为字符串，长度不超过xx个字符，MQ会对USID进行索引，并提供搜索服务。消息的主体内容不会被索引，无法直接提供搜索。USID用于唯一标识某一条消息，用户可以根据具体业务设置USID，如使用订单号、事件号等能唯一标识消息的字符串。因为USID由用户提供，用户可以在日志等系统中存储USID，便于用户根据业务需要查询消息的发送和消费等情况。

<a id="storage_type"></a>
## 存储类型
* MySQL
* 文件
* Redis
* MongoDB

## Reference
1. 消息队列中间件：Kafka vs. RabbitMQ vs.  RocketMQ。[Link][Compares]
2. JMS 2.0: What's New? [Part1][JMS2.0_PART1_URL], [Part2][JMS2.0_PART2_URL], [API][JMS_API].


[JMS2.0_PART1_URL]: http://www.oracle.com/technetwork/articles/java/jms20-1947669.html "part1"
[JMS2.0_PART2_URL]: http://www.oracle.com/technetwork/articles/java/jms2messaging-1954190.html "part2"
[JMS_API]: https://jms-spec.java.net/2.0/apidocs/ "api docs"
[Compares]: http://alibaba.github.io/RocketMQ-docs/document/openuser/mqvsmq.pdf "from RocketMQ"
[Kafka_Url]: http://kafka.apache.org/documentation.html#introduction

[Rabbit_At_least_once]: https://www.rabbitmq.com/reliability.html
[ActiveMQ_Consume_Group]: http://activemq.apache.org/message-groups.html
[JMS_Ordering]: http://en.wikipedia.org/wiki/Java_Message_Service "JMS queue:..."
[ActiveMQ_Ordering]: http://activemq.apache.org/total-ordering.html
[Kafka_Re-consume]: http://www.slideshare.net/popcornylu/jcconf-apache-kafka
[JMS_Transaction]: http://docs.oracle.com/cd/E19798-01/821-1841/bncgh/index.html
[RabbitMQ_Transaction]: https://www.rabbitmq.com/semantics.html
[JMS_Nack]: http://docs.oracle.com/cd/E13222_01/wls/docs81/ConsoleHelp/domain_jmsqueue_config_redelivery.html
[JMS_Query]: http://docs.oracle.com/cd/E13222_01/wls/docs92/jms_admin/manage_msg.html