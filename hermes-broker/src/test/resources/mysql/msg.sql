CREATE TABLE IF NOT EXISTS `meta_token` (
  `id` int(11) NOT NULL,
  `token` int(11) NOT NULL,
  `type` enum('p','c') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `token` (`token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='producer_token & consumer_token';

CREATE TABLE IF NOT EXISTS `msg_high_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` blob NOT NULL,
  `source_ip` int(11) NOT NULL,
  `token` varchar(50) NOT NULL,
  `ref_key` varchar(50) DEFAULT NULL,
  `properties` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='Topic: testtopic;\r\nPriority: high.';

CREATE TABLE IF NOT EXISTS `msg_low_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` blob NOT NULL,
  `source_ip` int(11) NOT NULL,
  `token` varchar(50) NOT NULL,
  `ref_key` varchar(50) DEFAULT NULL,
  `properties` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COMMENT='Topic: TestTopic;\r\nPriority: log.';

CREATE TABLE IF NOT EXISTS `msg_middle_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` blob NOT NULL,
  `source_ip` int(11) NOT NULL,
  `token` varchar(50) NOT NULL,
  `ref_key` varchar(50) DEFAULT NULL,
  `properties` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Topic: testtopic;\r\nPriority: middle.';

CREATE TABLE IF NOT EXISTS `offset_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) NOT NULL,
  `offset` int(11) NOT NULL COMMENT 'refer to msg_p_[topic]_[priority]:id',
  `priority` enum('low','midddle','high') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COMMENT='consume offset.';

CREATE TABLE IF NOT EXISTS `resend_high_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) DEFAULT '0',
  `start` int(11) NOT NULL,
  `end` int(11) NOT NULL,
  `timestamp` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='resend high priority messages.';

CREATE TABLE IF NOT EXISTS `resend_low_${topic}` (
  `id` int(11) DEFAULT NULL,
  `group_id` varchar(50) DEFAULT NULL,
  `start` int(11) DEFAULT NULL,
  `end` int(11) DEFAULT NULL,
  `timestamp` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='resend middle priority messages.';

CREATE TABLE IF NOT EXISTS `resend_middle_${topic}` (
  `id` int(11) DEFAULT NULL,
  `group_id` varchar(50) DEFAULT NULL,
  `start` int(11) DEFAULT NULL,
  `end` int(11) DEFAULT NULL,
  `timestamp` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='resend middle priority messages.';

CREATE TABLE IF NOT EXISTS `resend_offset_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) NOT NULL,
  `offset` int(11) NOT NULL COMMENT 'refer to msg_p_[topic]_[priority]:id',
  `priority` enum('low','midddle','high') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='consume offset.';
