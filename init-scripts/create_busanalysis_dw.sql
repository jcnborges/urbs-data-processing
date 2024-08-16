CREATE DATABASE `busanalysis_dw` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `busanalysis_dw`;

CREATE TABLE `dim_bus_stop` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `legacy_id` int(11) DEFAULT NULL,
  `name` varchar(500) DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `type` varchar(500) DEFAULT NULL,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `legacy_id_UNIQUE` (`legacy_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15453 DEFAULT CHARSET=utf8;

CREATE TABLE `dim_line` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `line_code` varchar(255) NOT NULL,
  `line_name` text,
  `service_category` text,
  `color` text,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `line_code` (`line_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=593 DEFAULT CHARSET=utf8;

CREATE TABLE `fat_event` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `dim_line_id` int(11) DEFAULT NULL,
  `vehicle` text,
  `itinerary_id` int(11) DEFAULT NULL,
  `event_timestamp` datetime DEFAULT NULL,
  `seq` int(11) DEFAULT NULL,
  `dim_bus_stop_id` int(11) DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `fk_dim_bus_stop_idx` (`dim_bus_stop_id`),
  KEY `fk_dim_line_idx` (`dim_line_id`),
  KEY `fat_event_base_date_idx` (`base_date`) USING BTREE,
  CONSTRAINT `fk_dim_bus_stop` FOREIGN KEY (`dim_bus_stop_id`) REFERENCES `dim_bus_stop` (`id`),
  CONSTRAINT `fk_dim_line` FOREIGN KEY (`dim_line_id`) REFERENCES `dim_line` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=33779859 DEFAULT CHARSET=utf8;

CREATE TABLE `fat_itinerary` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `dim_line_id` int(11) DEFAULT NULL,
  `bus_stop_id` int(11) NOT NULL DEFAULT '0',
  `next_bus_stop_id` int(11) DEFAULT '0',
  `next_bus_stop_delta_s` double DEFAULT NULL,
  `itinerary_id` int(11) DEFAULT NULL,
  `seq` int(11) DEFAULT NULL,
  `line_way` varchar(255) DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `last_update` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_dim_next_bus_stop_idx` (`next_bus_stop_id`),
  KEY `fk_dim_bus_stop_idx` (`bus_stop_id`),
  KEY `fk_dim_line_idx` (`dim_line_id`),
  KEY `fat_itinerary_base_date` (`base_date`) USING BTREE,
  CONSTRAINT `fk_dim_bus_stop_1` FOREIGN KEY (`bus_stop_id`) REFERENCES `dim_bus_stop` (`id`),
  CONSTRAINT `fk_dim_line_itinerary` FOREIGN KEY (`dim_line_id`) REFERENCES `dim_line` (`id`),
  CONSTRAINT `fk_dim_next_bus_stop` FOREIGN KEY (`next_bus_stop_id`) REFERENCES `dim_bus_stop` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2482714 DEFAULT CHARSET=utf8;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `fn_haversine`(lat1 double, lng1 double, lat2 double, lng2 double) RETURNS double
READS SQL DATA  -- Add this line
BEGIN
    DECLARE R INT;
    DECLARE dLat DECIMAL(30,15);
    DECLARE dLng DECIMAL(30,15);
    DECLARE a1 DECIMAL(30,15);
    DECLARE a2 DECIMAL(30,15);
    DECLARE a DECIMAL(30,15);
    DECLARE c DECIMAL(30,15);
    DECLARE d DECIMAL(30,15);

    SET R = 6371000; -- Earth's radius in metres
    SET dLat = RADIANS( lat2 ) - RADIANS( lat1 );
    SET dLng = RADIANS( lng2 ) - RADIANS( lng1 );
    SET a1 = SIN( dLat / 2 ) * SIN( dLat / 2 );
    SET a2 = SIN( dLng / 2 ) * SIN( dLng / 2 ) * COS( RADIANS( lng1 )) * COS( RADIANS( lat2 ) );
    SET a = a1 + a2;
    SET c = 2 * ATAN2( SQRT( a ), SQRT( 1 - a ) );
    SET d = R * c;
    RETURN d;
END$$
DELIMITER ;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_cluster` AS select `d1`.`legacy_id` AS `bus_stop_centre`,`d2`.`legacy_id` AS `bus_stop_clustered`,`busanalysis_dw`.`fn_haversine`(`d1`.`latitude`,`d1`.`longitude`,`d2`.`latitude`,`d2`.`longitude`) AS `d` from (`busanalysis_dw`.`dim_bus_stop` `d1` join `busanalysis_dw`.`dim_bus_stop` `d2`) where (`busanalysis_dw`.`fn_haversine`(`d1`.`latitude`,`d1`.`longitude`,`d2`.`latitude`,`d2`.`longitude`) <= 600);
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_event` AS select `busanalysis_dw`.`dim_line`.`line_code` AS `line_code`,`busanalysis_dw`.`dim_line`.`line_name` AS `line_name`,`fat`.`vehicle` AS `vehicle`,`dim`.`latitude` AS `latitude`,`dim`.`longitude` AS `longitude`,`dim`.`name` AS `name`,`dim`.`legacy_id` AS `legacy_id`,`dim`.`type` AS `type`,`fat`.`seq` AS `seq`,`fat`.`itinerary_id` AS `itinerary_id`,`fat`.`event_timestamp` AS `event_timestamp`,`fat`.`base_date` AS `base_date` from ((`busanalysis_dw`.`fat_event` `fat` join `busanalysis_dw`.`dim_bus_stop` `dim` on((`fat`.`dim_bus_stop_id` = `dim`.`id`))) join `busanalysis_dw`.`dim_line` on((`fat`.`dim_line_id` = `busanalysis_dw`.`dim_line`.`id`)));
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_itinerary` AS select `busanalysis_dw`.`dim_line`.`line_code` AS `line_code`,`busanalysis_dw`.`dim_line`.`line_name` AS `line_name`,`fat`.`itinerary_id` AS `itinerary_id`,`busanalysis_dw`.`dim_bus_stop`.`legacy_id` AS `legacy_id`,`busanalysis_dw`.`dim_bus_stop`.`name` AS `name`,`busanalysis_dw`.`dim_bus_stop`.`type` AS `type`,`busanalysis_dw`.`dim_bus_stop`.`latitude` AS `latitude`,`busanalysis_dw`.`dim_bus_stop`.`longitude` AS `longitude`,`dim_next_bus_stop`.`name` AS `next_bus_stop_name`,`dim_next_bus_stop`.`legacy_id` AS `next_bus_stop_legacy_id`,`fat`.`next_bus_stop_delta_s` AS `next_bus_stop_delta_s`,`fat`.`seq` AS `seq`,`fat`.`base_date` AS `base_date` from (((`busanalysis_dw`.`fat_itinerary` `fat` join `busanalysis_dw`.`dim_bus_stop` on((`fat`.`bus_stop_id` = `busanalysis_dw`.`dim_bus_stop`.`id`))) left join `busanalysis_dw`.`dim_bus_stop` `dim_next_bus_stop` on((`fat`.`next_bus_stop_id` = `dim_next_bus_stop`.`id`))) join `busanalysis_dw`.`dim_line` on((`fat`.`dim_line_id` = `busanalysis_dw`.`dim_line`.`id`)));

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_dim_bus_stop`()
BEGIN
	TRUNCATE busanalysis_etl.etl_dim_bus_stop;
	INSERT INTO busanalysis_etl.etl_dim_bus_stop
	SELECT
		id
        ,MAX(name)
		,AVG(latitude)
		,AVG(longitude)
		,MAX(type)
	FROM busanalysis_etl.etl_itinerary
    GROUP BY id;

	INSERT INTO busanalysis_dw.dim_bus_stop(legacy_id, name, latitude, longitude, type)
	SELECT
		etl_dim.id
		,etl_dim.name
		,etl_dim.latitude
		,etl_dim.longitude
		,etl_dim.type
	FROM busanalysis_etl.etl_dim_bus_stop AS etl_dim
	LEFT JOIN busanalysis_dw.dim_bus_stop AS dim ON etl_dim.id = dim.legacy_id
	WHERE dim.legacy_id IS NULL;

	UPDATE busanalysis_dw.dim_bus_stop AS dim
	INNER JOIN busanalysis_etl.etl_dim_bus_stop AS etl_dim ON dim.legacy_id = etl_dim.id
	SET
		dim.name = etl_dim.name
		,dim.latitude = etl_dim.latitude
		,dim.longitude = etl_dim.longitude
		,dim.type = etl_dim.type
		,dim.last_update = current_timestamp()
	WHERE
		dim.name <> etl_dim.name
		OR dim.latitude <> etl_dim.latitude
		OR dim.longitude <> etl_dim.longitude
		OR dim.type <> etl_dim.type;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_dim_line`()
BEGIN
	INSERT INTO busanalysis_dw.dim_line(line_code, line_name, service_category, color)
	SELECT
		etl_dim.line_code
		,etl_dim.line_name
		,etl_dim.service_category
		,etl_dim.color
	FROM busanalysis_etl.etl_line AS etl_dim
	LEFT JOIN busanalysis_dw.dim_line AS dim ON etl_dim.line_code = dim.line_code
	WHERE dim.line_code IS NULL;

	UPDATE busanalysis_dw.dim_line AS dim
	INNER JOIN busanalysis_etl.etl_line AS etl_dim ON dim.line_code = etl_dim.line_code
	SET
		dim.line_name = etl_dim.line_name
		,dim.service_category = etl_dim.service_category
		,dim.color = etl_dim.color
		,dim.last_update = current_timestamp()
	WHERE
		dim.line_name <> etl_dim.line_name
		OR dim.service_category <> etl_dim.service_category
		OR dim.color <> etl_dim.color;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_fat_itinerary`(IN base_date_in DATE)
BEGIN
	TRUNCATE busanalysis_etl.etl_fat_itinerary;
	INSERT INTO busanalysis_etl.etl_fat_itinerary
	SELECT
		line_code
		,id
		,next_stop_id
		,next_stop_delta_s
		,itinerary_id
		,seq
		,line_way
	FROM busanalysis_etl.etl_itinerary;

	REPEAT
		DELETE FROM busanalysis_dw.fat_itinerary
		WHERE
			base_date = base_date_in
		LIMIT 10000;
	UNTIL ROW_COUNT() = 0 END REPEAT;

	INSERT INTO busanalysis_dw.fat_itinerary(dim_line_id, bus_stop_id, next_bus_stop_id, next_bus_stop_delta_s, itinerary_id, seq, line_way, base_date)
	SELECT
		dim_line.id
		,dim.id AS bus_stop_id
		,dim_next.id AS next_bus_stop_id
		,next_stop_delta_s
		,itinerary_id
		,seq
		,line_way
        ,base_date_in
	FROM busanalysis_etl.etl_fat_itinerary AS etl_fat
	INNER JOIN busanalysis_dw.dim_bus_stop AS dim ON dim.legacy_id = etl_fat.id
	LEFT JOIN busanalysis_dw.dim_bus_stop AS dim_next ON dim_next.legacy_id = etl_fat.next_stop_id
    INNER JOIN busanalysis_dw.dim_line AS dim_line ON dim_line.line_code = etl_fat.line_code;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_fat_event`(IN base_date_in DATE)
BEGIN
	REPEAT
		DELETE FROM busanalysis_dw.fat_event
		WHERE
			base_date = base_date_in
		LIMIT 10000;
	UNTIL ROW_COUNT() = 0 END REPEAT;
    
	INSERT INTO busanalysis_dw.fat_event(dim_line_id, vehicle, itinerary_id, event_timestamp, seq, dim_bus_stop_id, base_date)
	SELECT
		dim_line.id
		,evt.vehicle
		,evt.itinerary_id
		,CAST(evt.event_timestamp AS DATETIME) AS event_timestamp
        ,evt.seq
		,dim.id AS dim_bus_stop_id
		,base_date_in
	FROM busanalysis_etl.etl_event AS evt
	INNER JOIN busanalysis_dw.dim_bus_stop AS dim ON dim.legacy_id = evt.id
    INNER JOIN busanalysis_dw.dim_line AS dim_line ON dim_line.line_code = evt.line_code;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_all`(IN base_date DATE)
BEGIN
	CALL busanalysis_dw.sp_load_dim_bus_stop();
    CALL busanalysis_dw.sp_load_dim_line();
	CALL busanalysis_dw.sp_load_fat_event(base_date);
	CALL busanalysis_dw.sp_load_fat_itinerary(base_date);
END$$
DELIMITER ;