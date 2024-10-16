CREATE DATABASE IF NOT EXISTS `database_test_1`;
USE `database_test_1`;

CREATE TABLE `table_test_1` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data VARCHAR(100),
    date_hour_create DATETIME
)
PARTITION BY RANGE (YEAR(date_hour_create) * 100 + MONTH(date_hour_create)) (
    PARTITION p202301 VALUES LESS THAN (202302),
    PARTITION p202302 VALUES LESS THAN (202303),
    PARTITION p202303 VALUES LESS THAN (202304)
);

DELIMITER $$

DROP PROCEDURE IF EXISTS PopulateData$$

CREATE PROCEDURE PopulateData()
BEGIN
    DECLARE v_start DATE DEFAULT '2023-01-01';
    DECLARE v_end DATE DEFAULT '2023-12-31';
    DECLARE v_date DATE;
    SET v_date = v_start;
    WHILE v_date <= v_end DO
        INSERT INTO `table_test_1` (data, date_hour_create)
        VALUES (CONCAT('Data for ', v_date), v_date + INTERVAL FLOOR(RAND() * 86400) SECOND);
        SET v_date = v_date + INTERVAL 1 DAY;
    END WHILE;
END$$

DELIMITER ;

CALL PopulateData();
