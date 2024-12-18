-- 1. Remove e cria o banco de dados
DROP DATABASE IF EXISTS `database_test_1`;
CREATE DATABASE `database_test_1`;
USE `database_test_1`;

-- 2. Cria as tabelas com particionamento por RANGE COLUMNS (created_at)
CREATE TABLE `table_test_1` (
    id INT AUTO_INCREMENT,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION jan2023 VALUES LESS THAN ('2023-02-01 00:00:00')
);

CREATE TABLE `table_test_2` (
    id INT AUTO_INCREMENT,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION jan2023 VALUES LESS THAN ('2023-02-01 00:00:00')
);

-- 3. Define o delimitador para criar a procedure
DELIMITER $$

-- 4. Remove a procedure se ela já existir
DROP PROCEDURE IF EXISTS PopulateData$$

-- 5. Cria a procedure PopulateData com criação de partições
CREATE PROCEDURE PopulateData(IN p_table_name VARCHAR(64))
BEGIN
    DECLARE v_start DATETIME DEFAULT NOW() - INTERVAL 5 MONTH;  -- Data inicial decrementada em 5 meses
    DECLARE v_end DATETIME DEFAULT NOW();
    DECLARE v_date DATETIME;
    DECLARE v_partition_name VARCHAR(20);
    SET v_date = v_start;

    WHILE v_date <= v_end DO
        -- Define o nome da partição com base na data
        SET v_partition_name = CONCAT(LOWER(DATE_FORMAT(v_date, '%b')), YEAR(v_date));

        -- Adiciona a partição se não existir para o mês atual de v_date
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = DATABASE()  -- Certifica-se de usar o banco de dados atual
              AND TABLE_NAME = p_table_name
              AND PARTITION_NAME = v_partition_name
        ) THEN
            SET @sql = CONCAT(
                'ALTER TABLE `', p_table_name, '` ADD PARTITION (',
                'PARTITION ', v_partition_name, ' VALUES LESS THAN (\'',
                DATE_FORMAT(LAST_DAY(v_date) + INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'),
                '\'));'
            );
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        -- Insere o dado na tabela
        SET @insert_sql = CONCAT('INSERT INTO `', p_table_name, '` (created_at) VALUES (?);');
        PREPARE insert_stmt FROM @insert_sql;
        SET @v_date = v_date;
        EXECUTE insert_stmt USING @v_date;
        DEALLOCATE PREPARE insert_stmt;

        -- Incrementa a data
        SET v_date = v_date + INTERVAL 1 DAY;
    END WHILE;

    -- Adiciona a partição catch_all com MAXVALUE ao final
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table_name
          AND PARTITION_NAME = 'catch_all'
    ) THEN
        SET @sql = CONCAT('ALTER TABLE `', p_table_name, '` ADD PARTITION (PARTITION catch_all VALUES LESS THAN (MAXVALUE));');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$

-- 6. Restaura o delimitador padrão
DELIMITER ;

-- 7. Chama a procedure para popular as tabelas e criar as partições necessárias
CALL PopulateData('table_test_1');
CALL PopulateData('table_test_2');
