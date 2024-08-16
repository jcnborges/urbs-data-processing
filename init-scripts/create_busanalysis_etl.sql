CREATE DATABASE `busanalysis_etl` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `busanalysis_etl`;

CREATE TABLE `etl_dim_bus_stop` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(500) DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `type` varchar(500) DEFAULT NULL,
  KEY `idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `etl_event` (
  `line_code` text,
  `vehicle` text,
  `id` int(11) DEFAULT NULL,
  `itinerary_id` int(11) DEFAULT NULL,
  `event_timestamp` text,
  `seq` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `etl_fat_itinerary` (
  `line_code` varchar(255) DEFAULT NULL,
  `id` int(11) DEFAULT NULL,
  `next_stop_id` int(11) DEFAULT NULL,
  `next_stop_delta_s` double DEFAULT NULL,
  `itinerary_id` int(11) DEFAULT NULL,
  `seq` int(11) DEFAULT NULL,
  `line_way` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `etl_itinerary` (
  `line_code` text,
  `id` int(11) DEFAULT NULL,
  `name` text,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `type` text,
  `itinerary_id` int(11) DEFAULT NULL,
  `line_way` text,
  `next_stop_id` int(11) DEFAULT NULL,
  `next_stop_delta_s` double DEFAULT NULL,
  `seq` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `etl_line` (
  `line_code` varchar(255) NOT NULL,
  `line_name` text,
  `service_category` text,
  `color` text,
  KEY `idx_line_code` (`line_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
