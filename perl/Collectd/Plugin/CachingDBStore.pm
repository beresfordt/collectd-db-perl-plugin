package Collectd::Plugin::CachingDBStore ;

use strict ;
use warnings ;

use threads ;
use threads::shared ;
use Thread::Semaphore 2.12 ;
use Thread::Queue ;

use DateTime ;
use DateTime::TimeZone ;
# Should probably set this by config
my $LocalTZ = DateTime::TimeZone->new( name => 'Europe/London' ) ;

use Collectd qw( :all ) ;
use DBI qw( :sql_types ) ;
use Data::UUID ;
use Try::Tiny ;

plugin_register( TYPE_INIT, 'CachingDBStore', 'init' ) ;
plugin_register( TYPE_CONFIG, 'CachingDBStore', 'config' ) ;
plugin_register( TYPE_WRITE, 'CachingDBStore', 'write' ) ;
plugin_register( TYPE_FLUSH, 'CachingDBStore', 'flush' ) ;
plugin_register( TYPE_SHUTDOWN, 'CachingDBStore', 'shutdown' ) ;

my $configHash = {} ;
my @reqItem = ( 
    'RemoteDBHost',
    'RemoteDBPort',
    'RemoteDBName',
    'RemoteDBUser',
    'RemoteDBPassword',
    'SQLiteDir'
) ;
my $dataTypeLU = {
    0 => 'DS_TYPE_COUNTER',
    1 => 'DS_TYPE_GAUGE',
    2 => 'DS_TYPE_DERIVE',
    3 => 'DS_TYPE_ABSOLUTE', 
};

# max unix TS val is 2147483647 - using largest integer val which can be stored with that many digets for flush all conditions
my $massiveTSVal = '9999999999' ;

# SQLite
my $createTable = 'create table if not exists collectdData( 
    uuid TEXT PRIMARY KEY, 
    timestamp INTEGER, 
    measure INTEGER, 
    hostname TEXT, 
    ds_type TEXT,
    plugin TEXT,
    plugin_instance TEXT, 
    type TEXT,
    type_name TEXT, 
    type_instance TEXT
)' ;
my $createIndex = 'create index if not exists ts_idx on collectdData ( timestamp )' ;
my $insertIntoSQLite = 
'insert into collectdData(
    uuid, 
    timestamp,
    measure,
    hostname,
    ds_type,
    plugin,
    plugin_instance,
    type,
    type_name,
    type_instance ) 
values(?,?,?,?,?,?,?,?,?,?)' ;
my $extractFromSQLite = 'select * from collectdData where timestamp < ?' ;
my $deleteFromSQLite = 'delete from collectdData where uuid = ?' ;

# RemoteDB
my $insertIntoRemoteDB = 
'insert into metrics_database.collectdData( 
    uuid_bin,
    event_timestamp,
    measure,
    hostname,
    ds_type,
    plugin,
    plugin_instance,
    type,
    type_name,
    type_instance ) 
values(?,?,?,?,?,?,?,?,?,?)' ;

# Separate queues for cache and writing so cache loading can be performed concurrently with writing
my $CacheQueue = Thread::Queue->new() ;
my $WriteQueue = Thread::Queue->new() ;
my $TempQueue  = Thread::Queue->new() ;

# Using CacheLock semaphore to control access to CacheQueue
# as I want control of when it is to be up/down
# WriteQueue just using normal lock()
my $CacheLock = Thread::Semaphore->new() ;

sub init{
    
    # check all required config items have been passed in
    foreach my $item ( @reqItem ){
        if( !defined $configHash->{ $item } ){
            plugin_log( LOG_ERR, "DBStore: No $item configured." ) ;
            return 0 ;
        }
    }
    
    # Check that flushing values are numeric, and set defaults if not or not set
    if( !( defined $configHash->{CommitEvery} ) or !( $configHash->{CommitEvery} =~ /^\d+$/ ) ){
        plugin_log( LOG_WARNING, "CommitEvery config item not set, or non-numeric value, using default value of 1000" ) ;
        $configHash->{CommitEvery} = '1000' ;
    }
    if( !( defined $configHash->{FlushSQLiteLimit} ) or !( $configHash->{FlushSQLiteLimit} =~ /^\d+$/ ) ){
        plugin_log( LOG_WARNING, "FlushSQLiteLimit config item not set, or non-numeric value, using default value of 10000" ) ;
        $configHash->{FlushSQLiteLimit} = '10000' ;
    }
    if( !( defined $configHash->{CacheTime} ) or !( $configHash->{CacheTime} =~ /^\d+$/ ) ){
        plugin_log( LOG_WARNING, "CacheTime config item not set, or non-numeric value, using default value of 120" ) ;
        $configHash->{CacheTime} = '120' ;
    }
    
    # Check that sqlitedir exists and is read/writable
    unless( -e $configHash->{SQLiteDir} ){
        plugin_log( LOG_ERR, "Specified SQLiteDir does not exist." ) ;
        return 0 ;
    }
    unless( -d $configHash->{SQLiteDir} ){
        plugin_log( LOG_ERR, "Specified SQLiteDir is not a directory." ) ;
        return 0 ;
    }
    unless( ( -w $configHash->{SQLiteDir} ) and ( -r $configHash->{SQLiteDir} ) ){
        plugin_log( LOG_ERR, "Specified SQLiteDir is not read/writable by effective user." ) ;
        return 0 ;
    }
    
    # Create the sqlite tables and indexes if they dont exist
    lock( $WriteQueue ) ;

    my $SQLiteDbh ;
    my $sqliteConnectRv = try{
        $SQLiteDbh = DBI->connect(
            "dbi:SQLite:dbname=" . $configHash->{SQLiteDir} . "/CachingDBStore.db",
            "",
            "", 
            { RaiseError => 1, PrintError => 0, AutoCommit => 0 }
        ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to connect to SQLite DB file failed - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $sqliteConnectRv ;
    

    my $createTableRv = try{
        $SQLiteDbh->do( $createTable ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to create table failed - " . $_ ) ;
        $SQLiteDbh->rollback ;
        return 0 ;
    } ;
    return 0 unless $createTableRv ;
    $SQLiteDbh->commit ;


    my $createIndexRv = try{
        $SQLiteDbh->do( $createIndex ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to create index failed - " . $_ ) ;
        $SQLiteDbh->rollback ;
        return 0 ;
    } ;
    return 0 unless $createIndexRv ;
    $SQLiteDbh->commit ;
    
    # background a timer thread to periodically flush the in memory cache
    my $t = threads->create( \&cacheTimer, $configHash->{CacheTime} ) ;
    $t->detach ;
    
    plugin_log( LOG_INFO, "CachingDBStore.pm initialised" ) ;

    return 1 ;
}

sub config{
    my $config = shift ;
    
    # bung the config passed in by collectd into a more readable hash
    foreach my $i ( @{ $config->{children} } ){
        $configHash->{ $i->{key} } = $i->{values}[0] ;
    }
    
    return 1 ;
}

sub write{
    my ( $type, $dataSet, $valueList ) = @_ ;
    
    my @items ;

    # check that we have the same number of values as dataset (odd way of passing in the data tbh)
    if ( scalar ( @$dataSet ) != scalar ( @{ $valueList->{'values'} } ) ) {
        plugin_log( LOG_WARNING, "Size of dataset does not match size of value-list" ) ;
        return 0;
    }
    
    my $ug = new Data::UUID ;
    
    for ( my $i = 0; $i < scalar ( @$dataSet ); $i++ ) {
    
        my $uuid = $ug->create_str() ;
        
        # flatten input structure so it can be queued
        my $QueueItem = {
            uuid => $uuid,
            timestamp => $valueList->{time},
            measure => $valueList->{values}->[$i],
            hostname => $valueList->{host},
            ds_type => $dataTypeLU->{$dataSet->[$i]->{type}},
            plugin => $valueList->{plugin},
            plugin_instance => $valueList->{plugin_instance},
            type => $type,
            type_name => $dataSet->[$i]->{name},
            type_instance => $valueList->{type_instance},
        } ;
        
        push(@items, $QueueItem) ;
        
    }
    
    # Extract from queue by ts can take a couple of seconds - dont want the cache
    # locked for that long (causes lots of uc_update: Value too old moaning in log)
    # so bung to a temp queue if cachequeue locked
    if( $CacheLock->down_nb() ){
        foreach(@items){
            $CacheQueue->enqueue($_) ;
        }
        $CacheLock->up() ;
    }
    else{
        foreach(@items){
            $TempQueue->enqueue($_) ;
        }
    }
    
    return 1 ;
}

sub flush{

    # flush is a user initiated ( ie not automagic collectd triggered ) call

    my( $timeout, $identifier ) = @_ ;
    
    plugin_log( LOG_DEBUG, "Flush called with @_" ) ;
    
    # Identifier based flushing not yet implemented
    
    # WriteQueue first so we're not interfering with 
    # any in-progress writes to local or remote DB
    lock( $WriteQueue ) ;
    $CacheLock->down() ;
    
    # Empty temp queue
    if($TempQueue->pending()){
        $TempQueue->enqueue( undef ) ;
        while( my $item = $TempQueue->dequeue() ){
            $CacheQueue->enqueue( $item ) ;
        }
    }

    if( $timeout <= 0 ){
        $CacheQueue->enqueue( undef ) ;
        while( my $item = $CacheQueue->dequeue() ){
            $WriteQueue->enqueue( $item ) ;
        }
    }
    else{
        extractFromCacheQueue( $timeout ) ;
    }
    
    $CacheLock->up() ;
    
    # Now do same for anything cached in SQLite if we have connectivity to remote DB
    if( testRemoteDb() ){
        if( $timeout <= 0 ){
            extractFromSQLite( $massiveTSVal ) ;
        }
        else{
            extractFromSQLite( $timeout ) ;
        }
    }
    
    # write to RemoteDB if available
    if( testRemoteDb() ){
        writeToRemoteDB() ;
        # if any items remain in the writeQueue bung em into the SQLite store - shouldn't happen
        # but may as well be safe
        if( $WriteQueue->pending() ){
            writeToSQLite() ;
        }
    }
    else{
        writeToSQLite() ;
    }
    
    plugin_log( LOG_DEBUG, "Flush completed" ) ;
    
    return 1 ;
}

sub writeToRemoteDB{
    
    my $dbh ;
    my $sth ;
    
    lock( $WriteQueue ) ;
    
    plugin_log( LOG_DEBUG, "writeToRemoteDB called" ) ;
    
    my $connectRv = try{
        $dbh = DBI->connect('DBI:mysql:database=' . $configHash->{RemoteDBName} . 
                            ';host=' . $configHash->{RemoteDBHost} . 
                            ';port=' . $configHash->{RemoteDBPort}, 
                            $configHash->{RemoteDBUser}, 
                            $configHash->{RemoteDBPassword}, 
                            {RaiseError => 1, PrintError => 0, AutoCommit => 0}) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to connect to RemoteDB failed - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $connectRv ;
    

    my $insertPrepareRv = try{
        $sth = $dbh->prepare( $insertIntoRemoteDB ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to prepare statement failed - " . $insertIntoRemoteDB . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $insertPrepareRv ;


    my $ug = new Data::UUID ;
    
    # enqueue and undef so loop exits
    $WriteQueue->enqueue( undef ) ;
    my $i = 0 ;
    while( my $QueueItem = $WriteQueue->dequeue() ){
        
        my $rv ;

        my $dt = DateTime->from_epoch( epoch => $QueueItem->{timestamp}, time_zone => $LocalTZ ) ;
        my $date = $dt->ymd . " " . $dt->hms ;
        my $bin_uuid = $ug->from_string($QueueItem->{uuid}) ;
        
        # insert into RemoteDB - if we get DB error skip to next and re-queue failed insert item to CacheQueue
        my $insertExecuteTry = try{
            $rv = $sth->execute(
                $bin_uuid,
                $date, 
                $QueueItem->{measure}, 
                $QueueItem->{hostname}, 
                $QueueItem->{ds_type}, 
                $QueueItem->{plugin}, 
                $QueueItem->{plugin_instance}, 
                $QueueItem->{type}, 
                $QueueItem->{type_name}, 
                $QueueItem->{type_instance}
            ) ;
            return 1 ;
        }
        catch{
            plugin_log( LOG_ERR, "Attempt to insert failed - " . $_ ) ;
            $CacheLock->down ;
            $CacheQueue->enqueue( $QueueItem ) ;
            $CacheLock->up ;
            return 0 ;
        } ;
        next unless $insertExecuteTry ;
        
        # if insert failed re-queue item to CacheQueue and continue
        unless( $rv == 1 ){
            $CacheLock->down ;
            $CacheQueue->enqueue( $QueueItem ) ;
            $CacheLock->up ;
            next ;
        }
        
        $i++ ;
        
        if( $i % $configHash->{CommitEvery} == 0 ){
            $dbh->commit ;
        }
    }
    
    $dbh->commit ;
    
    plugin_log( LOG_DEBUG, "Successfully written $i items to RemoteDB" ) ;
    return 1 ;
}

sub writeToSQLite{
    
    my $sth ;
    
    lock( $WriteQueue ) ;
    
    plugin_log( LOG_DEBUG, "writeToSQLite called" ) ;
    
    my $SQLiteDbh ;
    my $sqliteConnectRv = try{
        $SQLiteDbh = DBI->connect(
            "dbi:SQLite:dbname=" . $configHash->{SQLiteDir} . "/CachingDBStore.db",
            "",
            "", 
            {RaiseError => 1, PrintError => 0, AutoCommit => 0}
        ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to connect to SQLite DB file failed - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $sqliteConnectRv ;


    my $sqlitePrepareRv = try{
        $sth = $SQLiteDbh->prepare( $insertIntoSQLite ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to prepare statement failed - " . $insertIntoSQLite . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $sqlitePrepareRv ;
    
    
    # enqueue an undef so loop exits
    $WriteQueue->enqueue( undef ) ;
    my $i = 0 ;
    while( my $QueueItem = $WriteQueue->dequeue() ){
        my $rv ;
        
        # insert into SQLite - if we get DB error return failure and re-queue to CacheQueue
        # explicit binding so it doesn't have to work out data type
        my $bindRv = try{
            $sth->bind_param( 1, $QueueItem->{uuid}, SQL_VARCHAR ) ; 
            $sth->bind_param( 2, $QueueItem->{timestamp}, SQL_BIGINT ) ; 
            $sth->bind_param( 3, $QueueItem->{measure}, SQL_BIGINT ) ; 
            $sth->bind_param( 4, $QueueItem->{hostname}, SQL_VARCHAR ) ; 
            $sth->bind_param( 5, $QueueItem->{ds_type}, SQL_VARCHAR ) ; 
            $sth->bind_param( 6, $QueueItem->{plugin}, SQL_VARCHAR ) ; 
            $sth->bind_param( 7, $QueueItem->{plugin_instance}, SQL_VARCHAR ) ; 
            $sth->bind_param( 8, $QueueItem->{type}, SQL_VARCHAR ) ; 
            $sth->bind_param( 9, $QueueItem->{type_name}, SQL_VARCHAR ) ; 
            $sth->bind_param( 10, $QueueItem->{type_instance}, SQL_VARCHAR ) ;
            return 1 ;
        }
        catch{
            plugin_log( LOG_ERR, "Failed attempt to bind values to prepared statement  - " . $insertIntoSQLite . " - " . $_ ) ;
            return 0 ;
        } ;
        return 0 unless $bindRv ;


        my $executeRv = try{
            $rv = $sth->execute ;
            return 1 ;
        }
        catch{
            plugin_log( LOG_ERR, "Attempt to insert failed - " . $_ ) ;
            $CacheLock->down ;
            $CacheQueue->enqueue( $QueueItem ) ;
            $CacheLock->up ;
            return 0 ;
        } ;
        return 0 unless $executeRv ;

        
        # if insert failed re-queue item and continue
        unless( $rv == 1 ){
            $CacheLock->down ;
            $CacheQueue->enqueue( $QueueItem ) ;
            $CacheLock->up ;
            next ;
        }
        
        $i++ ;
        
        if( $i % $configHash->{CommitEvery} == 0 ){
            $SQLiteDbh->commit ;
        }
    }
    
    plugin_log( LOG_DEBUG, "Successfully written $i items to SQLite" ) ;
    
    $SQLiteDbh->commit ;
    
    return 1 ;
}

sub testRemoteDb{
    
    my $dbh ;
    
    my $rv = try{
        $dbh = DBI->connect('DBI:mysql:database=' . $configHash->{RemoteDBName} . 
                            ';host=' . $configHash->{RemoteDBHost} . 
                            ';port=' . $configHash->{RemoteDBPort}, 
                            $configHash->{RemoteDBUser}, 
                            $configHash->{RemoteDBPassword},
                            {RaiseError => 1}
        ) ;
        plugin_log( LOG_DEBUG, "Connection RemoteDB connection test succeeded" ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Connection attempt to RemoteDB failed - " . $_ ) ;
        return 0 ;
    }

    $dbh->disconnect ;
    return $rv ;
}

sub extractFromCacheQueue{
    
    my $timeStamp = shift ;
    
    plugin_log( LOG_DEBUG, "extractFromCacheQueue called with $timeStamp" ) ;
    
    my $CacheDepth = $CacheQueue->pending() ;
    my @toExtract ;
    
    # Peek at each item in the queue - look but dont alter queue
    # If it has a timestamp which needs flushing remember it's index
    for ( my $i = 0 ; $i< $CacheDepth ; $i++ ){
        my $item = $CacheQueue->peek( $i ) ;
        if( $item->{timestamp} <= $timeStamp ){
            push( @toExtract, $i ) ;
        }
    }
    
    # Go through the array of indexes which need extracting
    # and get those items from the queue
    # for each item we remove all subsequent items in the queue
    # have their index decreased by 1, so adjust the index 
    # returned by the array accordingly
    my $count = scalar( @toExtract ) ;
    for( my $i = 0 ; $i < $count ; $i++ ){
        my $idx = $toExtract[$i] - $i ;
        my $data = $CacheQueue->extract( $idx ) ;
        $WriteQueue->enqueue( $data ) ;
    }
    
    plugin_log( LOG_DEBUG, "Enqueued $count items from CacheQueue to WriteQueue" ) ;
    
    return 1 ;
}

sub extractFromSQLite{
    
    # load all before timestamp x into writeQueue
    # only load maximum of $configHash->{FlushSQLiteLimit} ( defaults to 10,000 )
    
    my $timestamp = shift ;
    
    plugin_log( LOG_DEBUG, "extractFromSQLite called with $timestamp" ) ;
    
    my $sth ;
    my $dh ;
    
    lock( $WriteQueue ) ;
    
    my $SQLiteDbh ;
    my $sqliteConnectRv = try{
        $SQLiteDbh = DBI->connect(
            "dbi:SQLite:dbname=" . $configHash->{SQLiteDir} . "/CachingDBStore.db",
            "",
            "", 
            {RaiseError => 1, PrintError => 0, AutoCommit => 0}
        ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to connect to SQLite DB file failed - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $sqliteConnectRv ;
 

    my $prepareExtractRv = try{
        $sth = $SQLiteDbh->prepare( $extractFromSQLite ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to prepare select statement failed - " . $extractFromSQLite . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $prepareExtractRv ;


    my $prepareDeleteRv = try{
        $dh = $SQLiteDbh->prepare( $deleteFromSQLite ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to prepare delete statement failed - " . $deleteFromSQLite . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $prepareDeleteRv ;
    

    # sqlite assumes any value is text, so we need to explicitly bind
    my $bindQueryRv = try{
        $sth->bind_param( 1, $timestamp, SQL_BIGINT ) ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Failed attempt to bind value $timestamp to prepared statement  - " . $deleteFromSQLite . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $bindQueryRv ;
    

    my $selectExecuteRv = try{
        $sth->execute ;
        return 1 ;
    }
    catch{
        plugin_log( LOG_ERR, "Attempt to execute statement with param $timestamp failed - " . $extractFromSQLite . " - " . $_ ) ;
        return 0 ;
    } ;
    return 0 unless $selectExecuteRv ;

    
    plugin_log( LOG_DEBUG, "extractFromSQLite statements prepared and bound" ) ;
    
    # for each matching row Enqueue to WriteQueue
    # attempt to delete from SQLite
    # if SQLite del blows up, log error, remove enqueued item from WriteQueue and return
    # if SQLite del returns non-zero remove enqueued item and continue
    
    my $i = 0 ;
    while( my $tuple = $sth->fetchrow_hashref() ){
        $WriteQueue->enqueue( $tuple ) ;
        my $rv ;
        my $dhExecuteRv = try{
            $rv = $dh->execute( $tuple->{uuid} ) ;
            return 1 ;
        }
        catch{
            plugin_log( LOG_ERR, "Attempt to delete value from SQLite failed - " . $_ ) ;
            my $junk = $WriteQueue->extract( -1 ) ;
            return 0 ;
        } ;
        next unless $dhExecuteRv ;
        
        unless( $rv == 1 ){
            my $junk = $WriteQueue->extract( -1 ) ;
            next ;
        }
        $i++ ;
        
        # Can't commit here as older version of sqlite driver does not support
        # commits on one statement handle while another is still running
        # try having separate database handles for query and delete, though
        # probably wont work due to sqlites table locking
        #
        # if( $i % $configHash->{CommitEvery} == 0 ){
        #    $SQLiteDbh->commit ;
        #}
        
        # If remote db outage lasted for ages there could be a lot of cached data in sqlite
        # so we use excessive amounts of ram I am limiting the number of messages which are
        # loaded from the sqlite cache in any flush cycle
        if( $i >= $configHash->{FlushSQLiteLimit} ){
            plugin_log( LOG_WARNING, "Stopped reading from SQLite cache at $i items - if more exist they will be picked up next flush" ) ;
            # tell the statement handle we're done so it doesn't interfere with the vacuum
            $sth->finish ;
            last ;
        }
    }
    
    plugin_log( LOG_DEBUG, "Loaded $i items from SQLite db into the write queue" ) ;
    
    $SQLiteDbh->commit ;
    
    # Reclaim space from DB file using vacuum
    # turn on autocommit so vacuum doesn't complain
    $SQLiteDbh->{AutoCommit} = 1 ;
    $SQLiteDbh->do('VACUUM') ;
    
    return 1 ;
}

sub shutdown{
    
    # wait for lock on write queue so we dont interfere with pre-exisiting write job
    lock( $WriteQueue ) ;
    $CacheLock->down() ;
    
    if($TempQueue->pending()){
        $TempQueue->enqueue( undef ) ;
        while( my $item = $TempQueue->dequeue() ){
            $CacheQueue->enqueue( $item ) ;
        }
    }
    
    if($CacheQueue->pending()){
        $CacheQueue->enqueue( undef ) ;
        while( my $item = $CacheQueue->dequeue() ){
            $WriteQueue->enqueue( $item ) ;
        }
    }
    
    if( $WriteQueue->pending() ){
        writeToSQLite() ;
    }
    
    return 1 ;
}

sub cacheTimer{
    
    my $sleepTime = shift ;
    
    while( 1 ){
        sleep $sleepTime ;
        my $flushtime = time - $sleepTime ;
        flush( $flushtime, '0' ) ;
    }
    
}

return 1;

