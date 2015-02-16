## MQ 2.16 讨论总结

### 首要的用户

- UBT
- Anti-Bot
- Recommandation (BI推荐，猜你喜欢)
- Fraud Detection
- 新发布系统

### 主要使用两类Queue

- **业务**使用的Queue：Kafka式量大，但对可靠性不敏感。
  后面接Storm，Storm分析数据将结果可再次写回Queue，供其他部门消费。
- **工具**使用的Queue:对可靠性要求高。
  例如，会包含发布系统、监控、变更、故障报警、流量报警等内容。

### 治理

主要会管理以下三个Domain的内容：
- Topic
- Producer
- Consumer

主要涉及的管理内容有：
- 规范使用
- 流量监控
- 错误报警
- 排障分析
- 安全控制

### Ops Metrics
- 消息传输延迟(从producer发出到Consumer接收到的时间)，粒度到每个Topic下每个Consumer. On every consumer.
- 消息（网络）流量：网络IO. On every producer, broker, consumer.
- 消息分发量（MPS-每秒消息处理数与Size）：整体Topic级别；具体到Producer和Consumer的级别。On every producer, consumer.
- 剩余处理时间：发生积压时，计算出消费完余量（大致的）时间。 On every topic
- （一定时间窗口如10s内）的消费时出错数：On every consumer.
- ...

### 关于Kafka Wrapper的设计

用户客户端Producer连接我们的KafkaBroker，KafkaBroker再去连接Kafka, 用户端Consumer也是连接我们的KafkaBroker来消费。

其中，有一个MetaServer作为中枢，指挥KafkaBroker与Kafka之间的交互。其功能除了联系ZK,根据Topic分配KafkaBorker给Producer和Consumer外，还有**时间同步**中央时间服务器,**负载均衡**的调节器的功能。





