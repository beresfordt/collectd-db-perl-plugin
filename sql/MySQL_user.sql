CREATE USER 'collectd'@'%' IDENTIFIED BY 'changeme';
GRANT INSERT ON metrics_database.collectdData TO 'collectd'@'%' ;
FLUSH PRIVILEGES;
