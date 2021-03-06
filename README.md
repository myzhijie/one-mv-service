### 基于Mysql Binlog日志的多表全量和增量数据同步

实现功能：
1. 实现mysql单表的数据同步，可只同步部分字段和增加where条件过滤；
2. 实现基于left join的SQL的mysql多表数据同步，可只同步部分字段和增加where条件过滤；
3. 源表和目标表可不在同一个mysql 数据库实例。

实现原理概述：
1. 启动后模拟mysql slave读取当前mysql binlog的position；
2. 根据SQL语句用jdbc stream的方式将原数据表中的数据全部copy到目标表；
3. 开始从mysql master拉取binlog日志，随后在目标表中执行；

多表数据增量同步原理概述：
1. 主表数据新增时，在where条件内则目标表中对应新增；
2. 主表数据删除时，在where条件内则目标表中对应删除；
3. 主表数据更新时，更新前在where条件内且目标表中存在相应字段。则先删除，若更新后也在where条件内再新增对应数据到目标表；

4. 非主表数据新增时，更新目标中关联数据；
5. 非主表数据删除时，清空目标中关联数据；
6. 非主表数据更新时，更新目标中关联数据；

功能限制：
1. 多表同步时仅支持一个主表left join多个非主表，且主表和非主表为多对一或一对一的关系。

快速开始：
1. 执行sql/init.sql脚本在目标库；
2. 配置文件中根据示例配置上源库、目标库、同步表配置；
3. 启动项目，访问 http://localhost:8080/manager/start-transfer 即可；
4. 目标库中的视图表为被自动创建；

项目中主要引用框架（致敬）：

1. Maxwell ，参考：https://github.com/zendesk/maxwell
2. Spring boot 2.1.6 ，参考：https://spring.io/projects/spring-boot


