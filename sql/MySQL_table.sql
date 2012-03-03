create table if not exists metrics_database.collectdData( 
    uuid_bin BINARY( 16 ) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL, 
    measure BIGINT, 
    hostname VARCHAR(64) NOT NULL,
    ds_type VARCHAR(64),
    plugin VARCHAR(64)  NOT NULL,
    plugin_instance VARCHAR(64), 
    type VARCHAR(64),
    type_name VARCHAR(64), 
    type_instance VARCHAR(64) ,
    PRIMARY KEY ( event_timestamp, uuid_bin ),
    INDEX ( hostname, plugin )
) 
ENGINE=InnoDB 

PARTITION BY RANGE ( UNIX_TIMESTAMP(event_timestamp))
    ( PARTITION p_20120229 VALUES LESS THAN(1330560000))
;
