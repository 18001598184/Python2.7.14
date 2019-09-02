#coding=utf-8 
import os,sys,platform,time
from datetime import tzinfo, timedelta, datetime
import ConfigParser,re,logging,traceback
import cx_Oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'


def fun_set_logger(log_file):
    global logger

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    LOG_FORMAT = "[%(asctime)s]-[%(levelname)s] %(message)s"
    #DEBUG_LOG_FORMAT = "[%(asctime)s]-[%(levelname)s]-[%(filename)s:%(lineno)d] %(message)s"

    # output log to file
    logger_FileHandler = logging.FileHandler(log_file)
    logger_FileHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
    logger_FileHandler.setLevel(logging.DEBUG)
    logger.addHandler(logger_FileHandler)
    # output log to command-line console
    logger_StreamHandler = logging.StreamHandler()
    logger_StreamHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
    logger_StreamHandler.setLevel(logging.INFO)
    logger.addHandler(logger_StreamHandler)

def fun_get_config(config_file):
    global current_timestamp
    global os_type
    global interval_minute
    global last_timestamp
    global src_db_host
    global src_db_name
    global src_db_username
    global src_db_password
    global tgt_db_host
    global tgt_db_name
    global tgt_db_username
    global tgt_db_password
    global bulk_size
    global full_sync_list
    global increase_sync_list
    global dblink_name
    
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    current_timestamp  = datetime.now()
    interval_minute    = config.get("TIME", "interval_minute")
    last_timestamp     = config.get("TIME", "last_timestamp")
    os_type            = platform.system().strip()
    src_db_host        = config.get("DB", "src_db_host")
    src_db_name        = config.get("DB", "src_db_name")
    src_db_username    = config.get("DB", "src_db_username")
    src_db_password    = config.get("DB", "src_db_password")
    tgt_db_host        = config.get("DB", "tgt_db_host")
    tgt_db_name        = config.get("DB", "tgt_db_name")
    tgt_db_username    = config.get("DB", "tgt_db_username")
    tgt_db_password    = config.get("DB", "tgt_db_password")
    bulk_size          = config.get("DB", "bulk_size")
    dblink_name        = config.get("DB","dblink_name")
    full_sync_list     = config.items("FULL_SYNC")
    increase_sync_list = config.items("INCREASE_SYNC")


def fun_check_config(config_file):
    global last_timestamp

    #1.check pkid config format and valid pkid in database
    src_db_str=src_db_username+'/'+src_db_password+'@'+src_db_host+':1521/'+src_db_name
    src_db = cx_Oracle.connect(src_db_str)
    src_cursor = src_db.cursor()
    for item in full_sync_list:
        if len(item[0].split("."))<>2 or len(item[0].split(".")[1])==0:
            logger.info('item='+str(item))
            logger.error('wrong PKID format in the config file, it should be:\n TABLE_NAME.PK_COLUMN_NAME=PK_VALUE.')
            sys.exit(-1)
        else:
            table_name=item[0].split('.')[0]
            pk_column=item[0].split('.')[1]
            sql_str='select max('+pk_column+') from '+table_name
            try:
                src_cursor.execute(sql_str)
            except Exception, e:
                src_db.close()
                logger.error(sql_str)
                logger.error('can not find '+table_name+'.'+pk_column+' in database, please check the config file.')
                sys.exit(-1)
    for item in increase_sync_list:
        if len(item[0].split("."))<>2 or len(item[0].split(".")[1])==0:
            logger.info('item='+str(item))
            logger.error('wrong PKID format in the config file, it should be:\n TABLE_NAME.PK_COLUMN_NAME=PK_VALUE.')
            sys.exit(-1)
        else:
            table_name=item[0].split('.')[0]
            pk_column=item[0].split('.')[1]
            sql_str='select max('+pk_column+') from '+table_name
            try:
                src_cursor.execute(sql_str)
            except Exception, e:
                src_db.close()
                logger.error('can not find '+table_name+'.'+pk_column+' in database, please check the config file.')
                sys.exit(-1)
    src_db.close()

    #2.check null timestamp and initial pkid
    if len(last_timestamp)==0:
        logger.warn('execute timestamp is null, initial timestamp and pkid.')
        last_timestamp=current_timestamp.strftime('%Y%m%d%H%M%S')
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        config.set("TIME", "last_timestamp", last_timestamp)
        config.write(open(config_file,'w'))

        src_db_str=src_db_username+'/'+src_db_password+'@'+src_db_host+':1521/'+src_db_name
        src_db = cx_Oracle.connect(src_db_str)
        src_cursor = src_db.cursor()
        i=0
        for item in increase_sync_list:
            if item[1]<>"0":
                table_name=item[0].split('.')[0]
                pk_column=item[0].split('.')[1]
                old_pk_value=item[1]
        
                sql_str='select max('+pk_column+') from '+table_name
                try:
                    src_cursor.execute(sql_str)
                    new_pk_value=str(src_cursor.fetchone()[0])
                except Exception, e:
                    src_db.close()
                    logger.error(traceback.format_exc().strip())
                    logger.error('sql_str : '+sql_str)
                    sys.exit(-1)
                logger.info(table_name+'.'+pk_column+': '+new_pk_value)
                config.set("INCREASE_SYNC", table_name+'.'+pk_column, new_pk_value)
                i=i+1
        if i>0:
            config.write(open(config_file,'w'))
        src_db.close()
        logger.info('======================FINISH======================')
        sys.exit(0)

    #3.check execute timestamp interval
    time_array=time.strptime(last_timestamp,'%Y%m%d%H%M%S')
    last_timestamp=datetime(time_array[0],time_array[1],time_array[2],time_array[3],time_array[4],time_array[5])
    diff_seconds=abs(current_timestamp-last_timestamp).seconds
    if diff_seconds < float(interval_minute)*60:
        logger.warn('last execute timestamp is '+last_timestamp.strftime('%Y%m%d%H%M%S')+', current timestamp is '+
                    current_timestamp.strftime('%Y%m%d%H%M%S')+', less than '+interval_minute+' minute, stop task.')
        logger.info('======================FINISH======================')
        sys.exit(0)

def fun_exec_sync(config_file):

    src_cursor = src_db.cursor()


    #1.check and create dblink on the target db
    #dblink_name='TRSDB220'
    src_sql_str='select count(1) from user_db_links dl where dl.DB_LINK=upper(\''+dblink_name+'\')'
    try: 
        src_cursor.execute(src_sql_str)
    except Exception as e:  
        logger.info('sql_error: '+ str(e))	
        return -1

    result=str(src_cursor.fetchone()[0])
    print "result",result
    if result=='0':
        tgt_sql_str='create database link "'+dblink_name+'" connect to '+tgt_db_username+' identified by '+tgt_db_password+' \n'+ \
                    '  using \'(description = \n'+ \
                    '    (address_list = (address = (protocol = tcp)(host = '+tgt_db_host+')(port = 1521))) \n'+ \
                    '    (connect_data = (server = dedicated)(service_name = '+tgt_db_username+')))\''
        try:
            src_cursor.execute(tgt_sql_str)
            logger.info('create database link from '+src_db_host+'/'+src_db_name+' to '+tgt_db_host+'/'+tgt_db_username+'.')
        except  Exception as e:  
            logger.info('sql_error: '+ str(e))	
            return -1 



    #2.merge base data

    logger.info("======================== FULL SYNC ==========================")	
    
    for item in full_sync_list:
        if item[1]<>"0":
            table_name = item[0].split('.')[0]
            pk_column = item[0].split('.')[1]
            src_sql_str = 'select count('+pk_column+') from ' + table_name + ' '
            try:
                src_cursor.execute(src_sql_str)
            except  Exception as e:  
                logger.info('sql_error: '+ e)	
                return -1             
            row_cnt = str(src_cursor.fetchone()[0])
            logger.info('full_sync: '+table_name +'  rows:'+ row_cnt)
            
            # build sql_str 
            src_sql_str='select (\n'+ \
                        '  select \'merge into '+table_name+'@'+dblink_name+ ' a\'||chr(10)||\n'+ \
                        '         \'using (select * from '+table_name+' ) b \'||chr(10)||\n'+ \
                        '         \'on (a.'+pk_column+' = b.'+pk_column+')\'||chr(10)||\n'+ \
                        '         \'when not matched then\'||chr(10)||\n'+ \
                        '         \'  insert(\'||to_char(wm_concat(chr(10)||\'    \'||tc1.column_name))||\')\'||chr(10)||\n'+ \
                        '         \'  values(\'||to_char(wm_concat(chr(10)||\'    b.\'||tc1.column_name))||\')\'||chr(10)\n'+ \
                        '    from  user_tab_columns tc1\n'+ \
                        '   where table_name=upper(\''+table_name+'\')\n'+ \
                        '  )||(\n'+ \
                        '  select \'when matched then\'||\n'+ \
                        '         \'  update set \'||to_char(wm_concat(chr(10)||\n'+ \
                        '         \'    a.\'||tc2.COLUMN_NAME||\'=b.\'||tc2.COLUMN_NAME)) upd\n'+ \
                        '    from  user_tab_columns tc2\n'+ \
                        '   where TABLE_NAME=upper(\''+table_name+'\') and COLUMN_NAME<>upper(\''+pk_column+'\')\n'+ \
                        '  )\n'+ \
                        ' from dual'
            try:
                src_cursor.execute(src_sql_str)
            except  Exception as e:  
                logger.info('sql_error: '+ str(e))	
                return -1 

            # merge sql str  
            src_sql_str=str(src_cursor.fetchone()[0])

            try:
                src_cursor.execute(src_sql_str)
            except  Exception as e:  
                logger.info('sql_error: '+ str(e))	
                return -1 

            src_db.commit()
    print "++++++++++++++++++++"
    # 增量同步
    #3.insert increase data and reset pkid in config file
    logger.info("======================== INCREASE SYNC ==========================")	

    config = ConfigParser.ConfigParser()
    config.read(config_file)
    i = 0
    for item in increase_sync_list:
        print item
        if item[1]<>"0":
            table_name = item[0].split('.')[0]
            pk_column = item[0].split('.')[1]

            # get current max_pk_id
            src_sql_str='select nvl(max('+pk_column+'),0) from '+table_name 
            try:
                src_cursor.execute(src_sql_str)
            except  Exception as e:  
                logger.info('sql_error: '+ str(e))	
                return -1 
            new_pk_value=int(src_cursor.fetchone()[0])

            # get tgt_max_pk_id
            tgt_sql_str='select nvl(max('+pk_column+'),0) from '+table_name +'@'+dblink_name 
            try:
                src_cursor.execute(tgt_sql_str)
            except  Exception as e: 
                logger.info('sql_error: '+ str(e))	
                return -1 
            old_pk_value = int(src_cursor.fetchone()[0])

            current_commit_pkid=0
            logger.info(' '+table_name+','+str(new_pk_value)+','+str(old_pk_value))
            if new_pk_value > old_pk_value :
                #get insert column order number and column name list from db sys views

                src_sql_str='select to_char(wm_concat(tc1.COLUMN_NAME||chr(10))) from user_tab_columns tc1\n'+ \
                            ' where TABLE_NAME=upper(\''+table_name+'\') and COLUMN_NAME not in (\'STAPOWER\',\'STAPOWER_DD\')'
                try:
                    src_cursor.execute(src_sql_str)
                except  Exception as e:  
                    logger.info('sql_error: '+ str(e))	
                    return -1                     
                tab_col=src_cursor.fetchone()[0]
                
                #fetch data from srouce db, then insert data into target db, commit by bulk_size
                #logger.info('  insert bulk_size = '+bulk_size)

                inc_rows = int(bulk_size)

                query_start_pkid = old_pk_value
                query_end_pkid = query_start_pkid + inc_rows
                src_cursor_pkid = src_db.cursor()

                logger.info("源库最大pkid:"+ str(new_pk_value)+"  目标库最大pkid: "+str(old_pk_value))

                while (query_start_pkid < new_pk_value ):

                    logger.info("开始同步:"+ str(query_start_pkid)+"  到 "+str(query_end_pkid))                    

                    #print "query_start_pkid",query_start_pkid,"new_pk_value",new_pk_value                       
                    src_sql_str='select '+pk_column+' from '+table_name+' where '+pk_column+'>'+str(query_start_pkid)+' and '+pk_column+'<='+str(query_end_pkid)
                    try:
                        src_cursor_pkid.execute(src_sql_str)
                        aa=src_cursor_pkid.fetchall()
                        row_num = src_cursor_pkid.rowcount
                        #print "sync %s row_num:%d" % (table_name,row_num)

                    except  Exception as e:  
                        logger.info('413:sql_error: '+ str(e))	
                        return -1                     


                    tgt_sql_str='insert into '+table_name+'@'+dblink_name+' (\n'+tab_col+') '+\
                                'select '+tab_col+' from '+table_name+\
                                ' where '+pk_column+'>'+str(query_start_pkid)+' and '+pk_column+'<='+str(query_end_pkid)
                                
                    #print tgt_sql_str            
                    try:
                        src_cursor.execute(tgt_sql_str)
                        logger.info('完成行数: '+ table_name+','+ str(row_num))	
                    except  Exception as e:  
                        logger.info('413:sql_error: '+ str(e))	
                        return -1                     
                                
                    try:
                        src_db.commit()
                    except  Exception as e:  
                        logger.info('408sql_error: '+ str(e))	
                        return -1     
                   
                    query_start_pkid = query_end_pkid
                    query_end_pkid  = query_start_pkid + inc_rows
                    

            #if current_commit_pkid>0:
            #    new_pk_value=current_commit_pkid
            logger.info('本轮同步完成: '+table_name+'.'+pk_column+' = '+str(new_pk_value))
            

    last_timestamp=current_timestamp.strftime('%Y%m%d%H%M%S')
    config.set("TIME", "last_timestamp", last_timestamp)
    config.write(open(config_file,'w'))



def connect_src():
    src_db_str=src_db_username+'/'+src_db_password+'@'+src_db_host+':1521/'+src_db_name
    try:    
        src_db = cx_Oracle.connect(src_db_str)
        return src_db
    except  Exception as e: 
        logger.info('408sql_error: '+ str(e))	
        print "Connect to DB error"
        return -1    

def connect_tgt():
    tgt_db_str=tgt_db_username+'/'+tgt_db_password+'@'+tgt_db_host+':1521/'+tgt_db_name
    try:
        tgt_db = cx_Oracle.connect(tgt_db_str)
        return tgt_db
    except  Exception as e: 
        logger.info('sql_error: '+ str(e))	
        print "Connect to DB error"
        return -1        

if __name__ == "__main__":
    log_file="sync_db.log"
    config_file='config.ini'
    fun_set_logger(log_file)
    logger.info('======================START======================')
    fun_get_config(config_file)
    fun_check_config(config_file)

    global src_db 

    src_db = connect_src()
    tgt_db = connect_tgt()
    

    while True:
        
        if not (src_db) :
            src_db = connect_src()
        elif not (tgt_db) :
            tgt_db = connect_tgt()
        else :
            fun_exec_sync(config_file)
            logger.info('=================== SLEEP 60 seconds ===================')
	    time.sleep(60)

