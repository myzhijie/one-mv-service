

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for t_mv_table_bootstrap
-- ----------------------------
CREATE TABLE `t_mv_table_bootstrap` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'DTS数据实例表',
  `source_table` varchar(64) NOT NULL COMMENT '源表名',
  `repeat_order` int(11) NOT NULL COMMENT '表需要多次bootstrap时的顺序',
  `custom_sql` text NOT NULL COMMENT '自定义bootstrap sql',
  `batch_num` int(11) NOT NULL default 100 COMMENT 'bootstrap时多少条回调一次',
  `handler_bean_name` varchar(128) NOT NULL COMMENT '处理类Bean名称',
  `note` varchar(256) DEFAULT NULL COMMENT '一个帮助你放附加信息的字段',
  `create_by` varchar(45) NOT NULL,
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_update_by` varchar(45) NOT NULL,
  `last_update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_table` (`source_table`,`repeat_order`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_bootstrap
-- ----------------------------
CREATE TABLE `t_mv_well_bootstrap` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `database_name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `table_name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `repeat_order` int(11) NOT NULL,
  `custom_sql` text NOT NULL COMMENT '自定义SQL',
  `batch_num` int(11) NOT NULL,
  `is_complete` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `inserted_rows` bigint(20) unsigned NOT NULL DEFAULT '0',
  `total_rows` bigint(20) unsigned NOT NULL DEFAULT '0',
  `created_at` datetime DEFAULT NULL,
  `started_at` datetime DEFAULT NULL,
  `completed_at` datetime DEFAULT NULL,
  `binlog_file` varchar(255) DEFAULT NULL,
  `binlog_position` int(10) unsigned DEFAULT '0',
  `client_id` varchar(255) CHARACTER SET latin1 NOT NULL DEFAULT 'maxwell',
  `comment` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_uni` (`database_name`,`table_name`,`client_id`,`repeat_order`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=411 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_columns
-- ----------------------------
CREATE TABLE `t_mv_well_columns` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema_id` int(10) unsigned DEFAULT NULL,
  `table_id` int(10) unsigned DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `charset` varchar(255) DEFAULT NULL,
  `coltype` varchar(255) DEFAULT NULL,
  `is_signed` tinyint(1) unsigned DEFAULT NULL,
  `enum_values` text CHARACTER SET utf8,
  `column_length` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `schema_id` (`schema_id`),
  KEY `table_id` (`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=782868 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_databases
-- ----------------------------
CREATE TABLE `t_mv_well_databases` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema_id` int(10) unsigned DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `charset` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `schema_id` (`schema_id`)
) ENGINE=InnoDB AUTO_INCREMENT=548 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_heartbeats
-- ----------------------------
CREATE TABLE `t_mv_well_heartbeats` (
  `server_id` int(10) unsigned NOT NULL,
  `client_id` varchar(255) CHARACTER SET latin1 NOT NULL DEFAULT 'maxwell',
  `heartbeat` bigint(20) NOT NULL,
  PRIMARY KEY (`server_id`,`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_positions
-- ----------------------------
CREATE TABLE `t_mv_well_positions` (
  `server_id` int(10) unsigned NOT NULL,
  `binlog_file` varchar(255) DEFAULT NULL,
  `binlog_position` int(10) unsigned DEFAULT NULL,
  `gtid_set` varchar(4096) DEFAULT NULL,
  `client_id` varchar(255) CHARACTER SET latin1 NOT NULL DEFAULT 'maxwell',
  `heartbeat_at` bigint(20) DEFAULT NULL,
  `last_heartbeat_read` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`server_id`,`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_schemas
-- ----------------------------
CREATE TABLE `t_mv_well_schemas` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `binlog_file` varchar(255) DEFAULT NULL,
  `binlog_position` int(10) unsigned DEFAULT NULL,
  `last_heartbeat_read` bigint(20) DEFAULT '0',
  `gtid_set` varchar(4096) DEFAULT NULL,
  `base_schema_id` int(10) unsigned DEFAULT NULL,
  `deltas` mediumtext CHARACTER SET utf8,
  `server_id` int(10) unsigned DEFAULT NULL,
  `position_sha` char(40) CHARACTER SET latin1 DEFAULT NULL,
  `charset` varchar(255) DEFAULT NULL,
  `version` smallint(5) unsigned NOT NULL DEFAULT '0',
  `deleted` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `position_sha` (`position_sha`)
) ENGINE=InnoDB AUTO_INCREMENT=137 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mv_well_tables
-- ----------------------------
CREATE TABLE `t_mv_well_tables` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema_id` int(10) unsigned DEFAULT NULL,
  `database_id` int(10) unsigned DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `charset` varchar(255) DEFAULT NULL,
  `pk` varchar(1024) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `schema_id` (`schema_id`),
  KEY `database_id` (`database_id`)
) ENGINE=InnoDB AUTO_INCREMENT=46078 DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
