
/*

Simple partition management procedure

Takes 3 arg input:

partition_manager('name of table', 'days to retain', 'future days partitions') ;

logs its actions to the table part_man_out


Sample event:

DELIMITER $$
DROP EVENT IF EXISTS 'e_rotate_partitions' $$
CREATE EVENT 'e_rotate_partitions'
    ON SCHEDULE
    EVERY 1 DAY
    STARTS curdate()
    DO
    call partition_manager(tblname, retain_days, future_parts) $$
DELIMITER ;

*/

DELIMITER $$

DROP PROCEDURE IF EXISTS partition_manager $$

CREATE PROCEDURE partition_manager( IN tblname VARCHAR(100), IN retain_days INT, IN future_parts INT )

BEGIN
    
    DECLARE sql_drop_partitions         VARCHAR(1000) ;
    DECLARE sql_add_partitions          VARCHAR(1000) ;
    DECLARE l_old_partitions            VARCHAR(1000) ;
    DECLARE l_new_partitions            VARCHAR(1000) ;
    DECLARE i                           INT ;
    
    -- Get cutoff date
    DECLARE cuttoff_date BIGINT(20) ;
    SET cuttoff_date = (
        SELECT DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL retain_days DAY),'%Y%m%d')
    );
    
    SET i = 0 ;
    
    -- set default future day partitions to create as 7
    IF( future_parts IS NULL ) THEN
        SET future_parts = 7 ;
    END IF ;
    
    -- create logging table if it doesn't exist
    CREATE TABLE IF NOT EXISTS metrics_database.part_man_out (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
        `msg_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
        `text` VARCHAR( 10000 ) NOT NULL
    ) ENGINE = MYISAM ;
        
    INSERT INTO metrics_database.part_man_out(text) VALUES(CONCAT('tblname - ',tblname)) ;
    INSERT INTO metrics_database.part_man_out(text) VALUES(CONCAT('cuttoff_date - ',cuttoff_date)) ;
    INSERT INTO metrics_database.part_man_out(text) VALUES(CONCAT('future partitions - ',future_parts)) ;

    -- temporary table holds potential new partition names (date_string)
    DROP TEMPORARY TABLE IF EXISTS metrics_database.tmp_partition_days;
    CREATE TEMPORARY TABLE metrics_database.tmp_partition_days( date_string VARCHAR(8), epoch_limit INT ) ;
    
    WHILE i < future_parts DO
        INSERT INTO metrics_database.tmp_partition_days( date_string, epoch_limit )
            VALUES( DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL i DAY),'%Y%m%d'), UNIX_TIMESTAMP(CONCAT(DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL i DAY),'%Y-%m-%d'),' 00:00:00')) + 86400 );
        SET i = i + 1 ;
    END WHILE ;
        
    -- only create add partition statements for those partitions which do not exist
    SELECT GROUP_CONCAT(CONCAT('PARTITION p_',a.date_string,' VALUES LESS THAN (',a.epoch_limit,')'))
    INTO l_new_partitions
    FROM metrics_database.tmp_partition_days a
    WHERE a.date_string NOT IN (
        SELECT RIGHT(partition_name,8) 
        FROM information_schema.partitions 
        WHERE table_name = tblname)
    ORDER BY a.date_string ASC ;

    IF ( l_new_partitions IS NOT NULL ) THEN
    
        SET sql_add_partitions = CONCAT('ALTER TABLE `', tblname, '` ADD PARTITION(', l_new_partitions, ')');
        INSERT INTO metrics_database.part_man_out(text) VALUES(CONCAT('sql_add_partitions - ', sql_add_partitions)) ;
        SET @sqlstatement = sql_add_partitions ;
        PREPARE sqlquery FROM @sqlstatement;
        EXECUTE sqlquery;
        DEALLOCATE PREPARE sqlquery;
        
    END IF ;

    -- find and drop partitions older than p_retention_days
    SELECT GROUP_CONCAT(partition_name)
    INTO l_old_partitions
    FROM information_schema.partitions 
    WHERE RIGHT(partition_name,8) < cuttoff_date
    AND partition_name <> 'pEOW'
    AND table_name = tblname ;
       
    IF ( l_old_partitions IS NOT NULL ) THEN
       
        SET sql_drop_partitions = CONCAT('ALTER TABLE `', tblname, '` DROP PARTITION ', l_old_partitions);
        INSERT INTO metrics_database.part_man_out(text) VALUES(CONCAT('sql_drop_partitions - ', sql_drop_partitions)) ;
        SET @sqlstatement = sql_drop_partitions;
        PREPARE sqlquery FROM @sqlstatement;
        EXECUTE sqlquery;
        DEALLOCATE PREPARE sqlquery;

    END IF;
    
END $$

DELIMITER ;
