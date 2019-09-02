#coding=utf-8
import os,sys,platform,shutil,time
import datetime
import ConfigParser,re,logging,ftplib,zipfile,socket,traceback
import cx_Oracle,random

def fun_set_logger(log_file):
    global logger
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    LOG_FORMAT = "[%(asctime)s]-[%(levelname)s] %(message)s"
    # DEBUG_LOG_FORMAT = "[%(asctime)s]-[%(levelname)s]-[%(filename)s:%(lineno)d] %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # output log to file
    logger_FileHandler = logging.FileHandler(log_file)
    logger_FileHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger_FileHandler.setLevel(logging.DEBUG)
    logger.addHandler(logger_FileHandler)
    # output log to command-line console
    logger_StreamHandler = logging.StreamHandler()
    logger_StreamHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger_StreamHandler.setLevel(logging.INFO)
    logger.addHandler(logger_StreamHandler)

def fun_get_config(config_file):
    global config
    global current_timestamp
    global os_type
    
    global self_loop_execution
    global interval_second
    global last_timestamp

    global save_as_zip
    global backup_path
    global local_output
    global output_path
    global output_by_day
    global output_line_per_file
    global column_delimiter
    global write_tmp_file
    
    global db_host
    global db_name
    global db_username
    global db_password
    global db_output_nsl_lang
    
    global ftp_enable
    global ftp_host
    global ftp_port
    global ftp_user
    global ftp_password
    global ftp_passive
    global ftp_path
    global ftp_save_by_day

    global data_section_array

    config = ConfigParser.ConfigParser()
    config.read(config_file)                

    current_timestamp    = datetime.datetime.now()
    os_type              = platform.system().strip()
    
    self_loop_execution  = config.get("TIME", "self_loop_execution")
    interval_second      = config.get("TIME", "interval_second")
    last_timestamp       = config.get("TIME", "last_timestamp")
    save_as_zip          = config.get("FILE", "save_as_zip")
    backup_path          = config.get("FILE", "backup_path")
    if backup_path[-1]=='\\' or backup_path[-1]=='/':
        backup_path=backup_path[0:-1]
    local_output         = config.get("FILE", "local_output")
    output_path          = config.get("FILE", "output_path")
    if output_path[-1]=='\\' or output_path[-1]=='/':
        output_path=output_path[0:-1]
    output_by_day        = config.get("FILE", "output_by_day")
    output_line_per_file = config.get("FILE", "output_line_per_file")
    column_delimiter     = config.get("FILE", "column_delimiter")
    write_tmp_file       = config.get("FILE", "write_tmp_file")
    
    db_host              = config.get("DB",  "db_host")
    db_name              = config.get("DB",  "db_name")
    db_username          = config.get("DB",  "db_username")
    db_password          = config.get("DB",  "db_password")
    db_output_nsl_lang   = config.get("DB",  "db_output_nsl_lang")
    
    ftp_enable           = config.get("FTP", "ftp_enable")
    if ftp_enable=='1':
        ftp_host         = config.get("FTP", "ftp_host")
        ftp_port         = config.get("FTP", "ftp_port")
        ftp_user         = config.get("FTP", "ftp_user")
        ftp_password     = config.get("FTP", "ftp_password")
        ftp_passive      = config.get("FTP", "ftp_passive")
        ftp_path         = config.get("FTP", "ftp_path")
        ftp_save_by_day  = config.get("FTP", "ftp_save_by_day")

    data_section_array=[]
    for data_section in config.sections():
        if data_section[0:5]=='QUERY':
            data_section_array.append(data_section)

def fun_check_running_process():
    program_name=sys.argv[0]
    current_pid=os.getpid()

    if os_type=='Windows':
        import wmi
        v_wmi=wmi.WMI()
        for process in v_wmi.Win32_Process():
            if process.ProcessId==current_pid:
                current_program=process.CommandLine
        for process in v_wmi.Win32_Process():
            if process.CommandLine==current_program and process.ProcessId<>current_pid:
                existed_pid=process.ProcessId
                logger.info('found existed running process, exit current process.')
                logger.info('  program_name = '+program_name)
                logger.info('  current_pid = '+str(current_pid))
                logger.info('  existed_pid = '+str(process.ProcessId))
                sys.exit(0)
    elif os_type=='Linux':
        cmd_result=os.popen('ps ux|grep "python.*'+program_name.strip()+'"|grep -v \'grep\'|awk \'{print $2}\'').read().split('\n')
        cmd_result.pop()
        cmd_result.remove(str(current_pid))
        if len(cmd_result)>0:
            logger.info('found existed running process, exit current process.')
            logger.info('  program_name = '+program_name)
            logger.info('  current_pid = '+str(current_pid))
            logger.info('  existed_pid = '+str(cmd_result[0]))
            sys.exit(0)

def fun_export_data():
    global last_timestamp
    global current_timestamp

    try:
        src_db_string=db_username+'/'+db_password+'@'+db_host+':1521/'+db_name
        src_db = cx_Oracle.connect(src_db_string)
    except Exception, e:
        logger.error(str(e).strip())
        logger.error('could not connect to db_host['+db_host+'], please check the config file and database server status.')
        sys.exit(-1)
    src_cursor = src_db.cursor()
    os.environ['NLS_LANG']=db_output_nsl_lang

    #check null last_timestamp and initialize pkid
    if len(last_timestamp)==0:
        logger.info('execute timestamp is null, initial timestamp and pkid.')
        for data_section in data_section_array:
            data_section_item_array=config.items(data_section)
            query_table_name=''
            query_pk_column=''
            for data_section_item in data_section_item_array:
                if data_section_item[0].find('.')>=0:
                    query_table_name=data_section_item[0].split('.')[0]
                    query_pk_column=data_section_item[0].split('.')[1]
            if len(str(query_pk_column))>0:
                sql_str='select max('+query_pk_column+') from '+query_table_name
                try:
                    src_cursor.execute(sql_str)
                except Exception, e:
                    logger.error(str(e).strip()+'\n'+sql_str)
                    src_db.close()
                    sys.exit(-1)
                new_pk_value=str(src_cursor.fetchone()[0])
                config.set(data_section, query_table_name+'.'+query_pk_column, str(new_pk_value))
                config.write(open(config_file,'w'))
                logger.info('['+data_section+']: new '+query_table_name+'.'+query_pk_column+' = '+str(new_pk_value))
            else:
                logger.info('['+data_section+']: did not find pkid setting, skiped.')
        last_timestamp=current_timestamp.strftime('%Y%m%d%H%M%S')
        config.set("TIME", "last_timestamp", last_timestamp)
        config.write(open(config_file,'w'))
        logger.info('reset last_timestamp = '+last_timestamp)
        return(0)

    current_timestamp = datetime.datetime.now()
    time_array=time.strptime(last_timestamp,'%Y%m%d%H%M%S')
    last_timestamp_datetime=datetime.datetime(time_array[0],time_array[1],time_array[2],time_array[3],time_array[4],time_array[5])
    diff_seconds=abs(current_timestamp-last_timestamp_datetime).seconds
    if int(diff_seconds) < int(interval_second):
        logger.info('diff_seconds='+str(diff_seconds))
        logger.info('interval_second='+str(interval_second))
        return(0)

    #export data
    logger.info('USE ['+db_output_nsl_lang+'] AS OUTPUT CHARACTERSET.')
    for data_section in data_section_array:
        data_section_item_array=config.items(data_section)
        #logger.info('data_section='+data_section)
        query_enable='1'
        full_export='0'
        query_sql_string=''
        output_file_name=''
        query_table_name=''
        query_pk_column=''
        query_pk_value=''
        output_row_count=0
        section_output_line_per_file=''
        for data_section_item in data_section_item_array:
            #logger.info('  data_section_item='+str(data_section_item))
            if data_section_item[0]=='enable':
                query_enable=data_section_item[1]
                #logger.info('  query_enable='+query_enable)
            elif data_section_item[0]=='query_sql':
                query_sql_string=data_section_item[1]
                #logger.info('  query_sql_string='+query_sql_string)
            elif data_section_item[0]=='output_line_per_file':
                section_output_line_per_file=data_section_item[1]
            elif data_section_item[0]=='output_file_name':
                output_file_name=data_section_item[1]
                if len(output_file_name.split('[DT]'))==2:
                    output_file_name=output_file_name.split('[DT]')[0]+ \
                                     current_timestamp.strftime('%Y%m%d%H%M%S')+ \
                                     output_file_name.split('[DT]')[1]
                if len(output_file_name.split('[TS]'))==2:
                    output_file_name=output_file_name.split('[TS]')[0]+ \
                                     str(int(time.mktime(current_timestamp.timetuple())))+ \
                                     output_file_name.split('[TS]')[1]
                if len(output_file_name.split('[RN]'))==2:
                    output_file_with_random_num='1'
                else:
                    output_file_with_random_num='0'
                #logger.info('  output_file_name='+output_file_name)
            elif data_section_item[0].find('.')>=0:
                query_table_name=data_section_item[0].split('.')[0]
                query_pk_column=data_section_item[0].split('.')[1]
                query_pk_value=data_section_item[1]
                #logger.info('  '+query_table_name+'.'+query_pk_column+'='+query_pk_value)
            elif data_section_item[0]=='target_table':
                #add for zhanjiang 20180706
                target_table=data_section_item[1]

        # if not found pk_id setting, set full_export='1' and execute full export
        if len(str(query_pk_value))==0:
            full_export='1'
        # if section_output_line_per_file been set in query section, use it instead of global set in [FILE] section
        if len(section_output_line_per_file)==0:
            section_output_line_per_file=output_line_per_file

        if query_enable=='0':
            logger.info('['+data_section+']: enable = 0, disable output.')
        else:
            if full_export=='1':
                # execute full data output
                logger.info('['+data_section+']: did not find pkid setting, execute full data output.')
                # if output row count <= section_output_line_per_file, do not add file_num suffix to output file
                # else fetch data from srouce db, and write data into output_file with output_line_per_file
                src_cursor.execute('select count(1) from ('+query_sql_string+')')
                output_row_count=str(src_cursor.fetchone()[0])
                logger.info('    output_row_count = '+str(output_row_count))
                if int(output_row_count)<=int(section_output_line_per_file):
                    section_output_line_per_file='0'
                if int(section_output_line_per_file)>0:
                    logger.info('    output_line_per_file = '+section_output_line_per_file)
                # execute query_sql_string and fetch data
                try:
                    src_cursor.execute(query_sql_string)
                except Exception, e:
                    logger.error(str(e).strip()+'\n'+query_sql_string)
                    src_db.close()
                    sys.exit(-1)
                row_num=0
                file_num=10001
                while True:
                    real_output_file_name=output_file_name
                    # set random number in output file name
                    if output_file_with_random_num=='1':
                        real_output_file_name=real_output_file_name.split('[RN]')[0]+ \
                                              str(random.randint(10000,99999))+ \
                                              real_output_file_name.split('[RN]')[1]
                    # save output file by output_line_per_file
                    if int(section_output_line_per_file)>0:
                        src_result=src_cursor.fetchmany(int(section_output_line_per_file))
                        real_output_file_name=real_output_file_name.split('.')[0]+ \
                                              '_'+str(file_num)[1:]+'.'+ \
                                              real_output_file_name.split('.')[-1]
                        file_num=file_num+1
                    else:
                        src_result=src_cursor.fetchall()
                    # if query returns not null, write data to output file
                    if len(src_result)>0:
                        out_file = open(real_output_file_name, "wb")
                        # add for zhanjiang 20180706
                        #out_file.write('begin;\n')
                        #out_file.write('delete from '+target_table+' where 1=1;\n')
                        for row in src_result:
                            write_string=str(row[0])
                            for i in range(1,len(row)):
                                if row[i]==None:
                                    write_string=write_string+column_delimiter
                                else:
                                    write_string=write_string+column_delimiter+str(row[i])
                            out_file.write(write_string+'\n')
                        #out_file.write('commit;\n')
                        logger.info('    output ['+str(row_num+len(src_result))+'] rows to ['+out_file.name+']')
                        out_file.close()
                        # add for zhanjiang 20180706
                        #logger.info('execute '+real_output_file_name)
                        #cmd_result=os.popen('mysql -h 219.132.57.36 -P 3306 -u nanjingsengen -p"1Q2we3as#$%^&a123" sengen<'+real_output_file_name).read()
                        #logger.info(cmd_result)
                        # call fun_save_data_file(), save output file or upload to ftp
                        fun_save_data_file(out_file.name)
                        time.sleep(0.2)
                    else:
                        break
            else:
                # execute increasement output started with old_pk_value
                old_pk_value=int(query_pk_value)
                logger.info('['+data_section+']:')
                logger.info('    old '+query_table_name+'.'+query_pk_column+' = '+str(old_pk_value))
                sql_str='select max('+query_pk_column+') from '+query_table_name
                try:
                    src_cursor.execute(sql_str)
                except Exception, e:
                    logger.error(str(e).strip()+'\n'+sql_str)
                    src_db.close()
                    sys.exit(-1)
                new_pk_value=str(src_cursor.fetchone()[0])

                if int(new_pk_value)>int(old_pk_value):
                    # if output row count <= section_output_line_per_file, do not add file_num suffix to output file,
                    # else fetch data from srouce db, and write data into output_file with output_line_per_file
                    src_cursor.execute('select count(1) from ('+query_sql_string+')',{'start_pkid':old_pk_value,'end_pkid':new_pk_value})
                    output_row_count=str(src_cursor.fetchone()[0])
                    logger.info('    output_row_count = '+str(output_row_count))
                    if int(output_row_count)<=int(section_output_line_per_file):
                        section_output_line_per_file='0'
                    if int(section_output_line_per_file)>0:
                        logger.info('    output_line_per_file = '+section_output_line_per_file)
                    # execute query_sql_string, and fetch data
                    try:
                        src_cursor.execute(query_sql_string,{'start_pkid':old_pk_value,'end_pkid':new_pk_value})
                    except Exception, e:
                        logger.error(str(e).strip()+'\n'+query_sql_string)
                        logger.error('start_pkid: ['+str(old_pk_value)+'], end_pkid: ['+str(new_pk_value)+']')
                        src_db.close()
                        sys.exit(-1)
                    row_num=0
                    file_num=10001
                    while True:
                        real_output_file_name=output_file_name
                        # set random number in output file name
                        if output_file_with_random_num=='1':
                            real_output_file_name=real_output_file_name.split('[RN]')[0]+ \
                                                  str(random.randint(10000,99999))+ \
                                                  real_output_file_name.split('[RN]')[1]
                        # save output file by output_line_per_file
                        if int(section_output_line_per_file)>0:
                            src_result=src_cursor.fetchmany(int(section_output_line_per_file))
                            real_output_file_name=real_output_file_name.split('.')[0]+ \
                                                  '_'+str(file_num)[1:]+'.'+ \
                                                  real_output_file_name.split('.')[-1]
                            file_num=file_num+1
                        else:
                            src_result=src_cursor.fetchall()
                        # if query returns not null, write data to output file
                        if len(src_result)>0:
                            out_file = open(real_output_file_name, "wb")
                            #out_file.write('begin;\n')
                            for row in src_result:
                                write_string=str(row[0])
                                for i in range(1,len(row)):
                                    if row[i]==None:
                                        write_string=write_string+column_delimiter
                                    else:
                                        write_string=write_string+column_delimiter+str(row[i])
                                out_file.write(write_string+'\n')
                            #out_file.write('commit;\n')
                            logger.info('    output ['+str(row_num+len(src_result))+'] rows to ['+out_file.name+']')
                            out_file.close()
                            # add for zhanjiang 20180706
                            #logger.info('execute '+real_output_file_name)
                            #cmd_result=os.popen('mysql -h 219.132.57.36 -P 3306 -u nanjingsengen -p"1Q2we3as#$%^&a123" sengen<'+real_output_file_name).read()
                            #logger.info(cmd_result)
                            # call fun_save_data_file(), save output file or upload to ftp
                            fun_save_data_file(out_file.name)
                            time.sleep(0.2)
                        else:
                            break
                    config.set(data_section, query_table_name+'.'+query_pk_column, str(new_pk_value))
                    config.write(open(config_file,'w'))
                logger.info('    new '+query_table_name+'.'+query_pk_column+' = '+str(new_pk_value))

    # reset the last_timestamp to current_timestamp when finished all data output process
    last_timestamp=current_timestamp.strftime('%Y%m%d%H%M%S')
    config.set("TIME", "last_timestamp", last_timestamp)
    config.write(open(config_file,'w'))
    src_db.close()

def fun_save_data_file(file_name):
    global output_path
    global write_tmp_file
    global output_by_day
    current_day=current_timestamp.strftime('%Y%m%d')

    # check empty file
    file_size=os.path.getsize(file_name)
    if file_size==0:
        os.remove(file_name)
        logger.info('        file size of ['+file_name+'] is zero, do not save and deleted.')
        return(0)

    # make zip file
    if save_as_zip=='1':
        zip_file_name=file_name+'.zip'
        myzip = zipfile.ZipFile(zip_file_name,'w',zipfile.ZIP_DEFLATED)
        myzip.write(file_name)
        myzip.close()
        os.remove(file_name)
        file_name=zip_file_name
        file_size=os.path.getsize(file_name)

    # save data file to backup path
    if not os.path.exists(backup_path+'/'+current_day):
        os.makedirs(backup_path+'/'+current_day)
    shutil.copy(file_name,backup_path+'/'+current_day)

    # save data file to local output path
    if local_output=='1':
        if output_by_day=='1':
            local_output_path=output_path+'/'+current_day
        else:
            local_output_path=output_path
        if not os.path.exists(local_output_path):
            os.makedirs(local_output_path)
        if write_tmp_file=='1':
            shutil.copy(file_name,local_output_path+'/'+file_name+'.tmp')
            shutil.move(local_output_path+'/'+file_name+'.tmp',local_output_path+'/'+file_name)
        else:
            shutil.copy(file_name,local_output_path)
        logger.info('        save "'+file_name+'" to local_output path "'+local_output_path+'"')

    # upload data file to remote ftp server
    if ftp_enable=='1':
        # initialize ftp object
        try:
            socket.setdefaulttimeout(15)
            ftp = ftplib.FTP()
            ftp.connect(ftp_host,ftp_port)
            ftp.login(ftp_user,ftp_password)
            ftp.set_pasv(int(ftp_passive))
        except Exception, e:
            logger.error(str(e).strip())
            logger.error('could not connect to ftp server['+ftp_host+'], please check the config file and ftp server status.')
            ftp.close()
            sys.exit(-1)
        # check and create ftp upload path
        try:
            ftp.cwd(ftp_path)
        except ftplib.error_perm:
            try:
                ftp.mkd(ftp_path)
                ftp.cwd(ftp_path)
            except ftplib.error_perm:
                logger.error('ftp_path='+ftp_path)
                logger.error('U have no authority to make dir')
                sys.exit(-1)
        if ftp_save_by_day=='1':
            try:
                ftp.cwd(current_day)
            except ftplib.error_perm:
                try:
                    ftp.mkd(current_day)
                    ftp.cwd(current_day)
                except ftplib.error_perm:
                    logger.error('U have no authority to make dir')
                    sys.exit(-1)
        # execute upload
        try:
            if write_tmp_file=='1':
                myfile = open(file_name, 'rb')
                ftp.storbinary('STOR ' + file_name+'.tmp', myfile)
                myfile.close()
                ftp.rename(file_name+'.tmp', file_name)
            else:
                myfile = open(file_name, 'rb')
                ftp.storbinary('STOR ' + file_name, myfile)
                myfile.close()
        except Exception, e:
            logger.warn(traceback.format_exc().strip())
            return(-1)

        logger.info('        upload "'+file_name+'" to '+ftp_host)
        ftp.quit()
    # remove original output file
    os.remove(file_name)


if __name__ == "__main__":
    # change current path to program root directory
    os.chdir(sys.path[0])

    # set logger and get config
    log_file="rd_share_data.log"
    config_file='config.ini'
    fun_set_logger(log_file)
    fun_get_config(config_file)

    #check running process
    fun_check_running_process()

    logger.info('======================START=======================')
    if self_loop_execution=='1':
        logger.info('self_loop_execution = '+self_loop_execution)
        logger.info('interval_second = '+interval_second)
        while True:
            fun_export_data()
            logger.info('sleep 5 seconds')
            time.sleep(5)
    else:
        fun_export_data()
    logger.info('======================FINISH======================')

    sys.exit(0)
