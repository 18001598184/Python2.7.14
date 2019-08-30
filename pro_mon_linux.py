#coding:utf-8
import time,datetime,os,sys
#import logging
import psutil,commands
import cx_Oracle,ConfigParser
import requests
#import chardet


# Name: PRO_MON_LINUX
# Version = 1.0
# Author = fengping
# Create date =2018/11/20
#
# Version = 2.0
# Modify date = 2018/12/21
#
# Version = 3.0
# Modify date = 2019/1/10
#   Send msg by TX:url
#   So if the DB error, it will also send msg.



#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

 
def fun_set_logger(log_file):

    logger.setLevel(logging.INFO)
    LOG_FORMAT = "[%(asctime)s]-[%(levelname)s] %(message)s"
    
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

 
def check_disk_space(chk_percent):
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """
    error_code = 0
    error_msg = ""

    io = psutil.disk_partitions()
    #print '----------------DISK INFORMATION -----------'
    for i in io:
        disk_name = i.mountpoint
        o = psutil.disk_usage(i.mountpoint)
	disk_total = int(o.total/1024.0/1024/1024)
	disk_free = int(o.free/1024/1024/1024.0)
        disk_use_percent = int(o.percent)

        if is_debug:
            CHK_DSK = disk_name + '           '
            CHK_DSK = CHK_DSK[0:10]
	    log_info =  " %s total:%dG Free:%dG Used:%d%% " % (CHK_DSK,disk_total,disk_free,disk_use_percent)
	    print log_info


	if disk_use_percent > chk_percent :
	    error_code += 1	
	    error_msg  += disk_name + ':' + str(disk_use_percent) + "%"

    if error_code > 0:
        error_code = 1
    else:
        error_msg = 'DSK OK'        
    
    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    return error_code,error_msg


def check_cpu(chk_percent):
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """

    error_code = 0
    error_msg = "CPU OK"

    # 查看cpu的信息
    # CPU numbers 
    cpu_num = psutil.cpu_count(logical=False)
    
    # CPU use percent
    cpu_use = psutil.cpu_percent(None)
    if is_debug:
        log_info =   "CPU num:%d use:%d%%" % (cpu_num,cpu_use)
        print log_info

    if  cpu_use > chk_percent :
        error_code = 1
	error_msg = "CPU:" + str(cpu_use) + '%'

    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info	
    return error_code,error_msg

def check_memory(chk_percent):
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """

    error_code = 0
    error_msg = 'Mem OK' 

    total = str(round(psutil.virtual_memory().total/(1024.0*1024.0*1024.0), 2))
    mem_use_percent = psutil.virtual_memory().percent
    if is_debug:
        log_info =  "MEM tot:%sG  use:%d%%"  % (total,mem_use_percent)
        print log_info
    if  (mem_use_percent > chk_percent) :
        error_code = 1
	error_msg  = 'Mem:' + str(mem_use_percent) + '%'

    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    return error_code,error_msg	

def check_process_linux(): 
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """	
    error_code = 0
    error_msg =''


    #print '[进程信息]:'

    
    check_code = 0

    for data_section in config.sections():
        if data_section[0:4]=='PROC':
            check_enable = config.getint(data_section,"enable")
            check_command = config.get(data_section,"ps_command")
            restart_command = config.get(data_section,"restart_command") 
	    if (check_enable == 1 ):
                check_command += '|grep -v grep'
                (status,s) = commands.getstatusoutput(check_command)
		if is_debug:
                    PROC_SEC = data_section + '                    '
                    PROC_SEC = PROC_SEC[0:20]   
                    log_info =  "[Check: %s][Stat: %d][Proc: %s]" % (PROC_SEC,status,check_command)
                    print log_info
                if status !=0 :	             		
		    error_code += 1
		    error_msg += data_section + ': ERROR '
		    if (len(restart_command) >0) :
 	                print "\t Restart:",restart_command 
                        (status,s) = commands.getstatusoutput(restart_command)
			print "\t Restart %s,status:%d,%s" % (restart_command,status,s)
		        if status == 0 :
			    print "    ok!"
			    error_msg += ' restart ok '
		        else:
	                    print "    error!"
                            error_msg += ' restart ERROR '
      
    if error_code >0:
        error_code = 1 
    else:
	error_msg = 'PRO OK'

    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    return error_code,error_msg	              


def check_file_linux(): 
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """	
    error_code = 0
    error_msg =''


    #print '[文件信息]:'

    
    check_code = 0

    for data_section in config.sections():
        if data_section[0:4]=='FILE':
            check_enable = config.getint(data_section,"enable")
	    file_path =  config.get(data_section,"file_path")
	    dir_date_format = config.get(data_section,"dir_date_format")
            file_date = time.strftime(dir_date_format, time.localtime(time.time()))
            file_pattern = config.get(data_section,"file_pattern")
            time_min = config.get(data_section,"time_min") 
	    if (check_enable == 1 ):
		    
                check_command = 'find ' + file_path+'/'+file_date+ " -name '"+file_pattern+"' -mmin -"+ time_min +' |wc -l'
		print check_command
                (status,s) = commands.getstatusoutput(check_command)
		if is_debug:
                    PROC_SEC = data_section + '                    '
                    PROC_SEC = PROC_SEC[0:20]   
                    log_info =  "[Check: %s][Stat: %d][: %s]" % (PROC_SEC,status,s)
                    print log_info

		if not (status == 0 and int(s) >0) :
		    error_code += 1
		    error_msg += data_section + ': ERROR '
		    
      
    if error_code >0:
        error_code = 1 
    else:
	error_msg = 'FILE OK'

    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    return error_code,error_msg	           

def check_db(db_string,chk_percent):
    """
    return:
        error_code : 0 ok ,1 error
        error_msg:
    """
    error_code = 0
    error_msg = ''

    # 1/ check  database connect 
    try:
        db = cx_Oracle.connect(db_string)       
    except  Exception as e: 
        #print  e
        error_msg = 'DB ERR'
        db = ''
        print  "DB CONNECT ERROR !!!" + str(e)
	error_code = 1
        error_msg = 'DB ERR'
        return  error_code,error_msg
    
    print "DB CONNECT OK"
    
    # 2/ check table space
    print "----------- CHECK TABLESPACE -------------"
    try:
        
        #检查 百分比 大于80% 的表空间
        my_sql = ''' 
             SELECT a.tablespace_name, round((total - free) / total, 2) * 100 pp
               FROM (SELECT tablespace_name, SUM(bytes) free
                      FROM dba_free_space
                      GROUP BY tablespace_name) a,
                   (SELECT tablespace_name, SUM(bytes) total
                    FROM dba_data_files
                     GROUP BY tablespace_name) b
             WHERE a.tablespace_name = b.tablespace_name
             and round((total - free) / total, 2) * 100 >
             '''
        my_sql += str(chk_percent)

        cursor = db.cursor()             
        cursor.execute(my_sql)
        result = cursor.fetchall()         
        count = cursor.rowcount 
        cursor.close()         
        for row in result:             
            tbs_name = row[0]
            tbs_pct  = int(row[1]) 	
            if is_debug :
		log_info =  "%s %d%%" % (tbs_name,tbs_pct)
                print log_info

            if (tbs_pct > int(chk_percent)) :
	        #chg_tablespace(db,tbs_name,max_percent,max_file_size)
                error_code += 1
	        error_msg += tbs_name + ':' + str(tbs_pct) + '%'

    except  Exception as e:  
        error_code += 1 
        error_msg = 'DB TBS ERR'
        print error_msg + str(e)
        return error_code,error_msg

    if error_code > 0 :
        error_code = 1
    else:
        error_msg = 'TBS OK' 
     
    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    # there may be sth wrong !  
    

    # 3/ check data
    print "--------- CHECK DATA --------" 
    (data_code,data_msg) = check_data(db)

    error_code += data_code
    error_msg += ',' + data_msg

    	    

    return error_code,error_msg	 

     


def check_data(db):

    error_code = 0
    error_msg = ''
    
    dd = db.cursor()
    
    for data_section in config.sections():
        if data_section[0:5]=='TABLE':
            check_enable = config.getint(data_section,"enable")
            check_sql = config.get(data_section,"check_sql")
            rows_limit = config.getint(data_section,"rows_limit")
            if (check_enable == 1 ):
                try:
                    dd.execute(check_sql)
                    chk_rownum = dd.fetchone()[0]
                except:
                    error_code = 1
                    error_msg = 'DB QUERY ERROR' 
    
                if is_debug:
                    DATA_SEC = data_section + '                    '
                    DATA_SEC = DATA_SEC[0:20]
		    log_info =  "[DATA: %s][Rows: %d][SQL: %s]" % (DATA_SEC,chk_rownum,check_sql)
		    print log_info
                if (chk_rownum < rows_limit) :
                    error_code += 1
                    error_msg += data_section + ': ERROR '        

    dd.close()        
                
    if error_code >0:
        error_code = 1
    else:
        error_msg = 'DATA OK'


    log_info =  "error_code:%d error_msg:%s" % (error_code,error_msg)
    print log_info
    return error_code,error_msg		


def  send_msg(check_msg):
	
    # --------  SEND  MSG----------------------
    #        It should send a msg at  8:10 whether there has error or not
    #        and other time ,just send error msg
    #------------------------------------------
    msg_phone = config.get("CHK_CONFIG", "msg_phone")        
    phone_list = msg_phone.split(',')
    my_url = config.get("CHK_CONFIG","url")

    my_time = time.strftime("%H:%M:%S", time.localtime())
    my_hour = int(my_time[0:2])
    my_min  = int(my_time[3:5])
    if ((my_hour == 8 and my_min > 1 and my_min <15 ) or (check_code > 0 )):  

    #url_wx = 'http://58.213.29.51:18098/TranRunsTX/servlet/WeiXiServlet'
    #url_sms = 'http://58.213.29.51:18098/TranRunsTX/servlet/SmsServlet'
    #s = {'phone': '13952008822', 'message': 'Hello','source':'1'}
    #r = requests.post(url, data=s)
    #print r
        #print "encoding_orig:",chardet.detect(check_msg)
        #.encode('utf-8')
        #encoding =  chardet.detect(check_msg)['encoding']
        #check_msg.decode(encoding).encode('utf-8')
        #print "encoding:",chardet.detect(check_msg)

        try:
            for my_phone in  phone_list:		    
	        s = {'phone':my_phone,'message':check_msg,'source':'1'}
	        r = requests.post(my_url,data=s)
	        print r
        except  Exception as e:  
	    print  'Send msg  error:'
	    print e

  

if __name__ == "__main__":
    global is_debug   

    #log_file  = sys.argv[0]+".log"
    #logger = logging.getLogger()
    #fun_set_logger(log_file)

    config_file = "config.ini" 

    if ( not os.path.isfile(config_file)):    
        print " I need the config file to run : config.ini !\n"
        exit()

    config = ConfigParser.ConfigParser()
    config.read(config_file)  

    now_time = time.strftime('%Y%m%d %H:%M:%S', time.localtime(time.time()))
    print ""
    print "================="
    print now_time
    print "================="

    is_debug =  config.getint("CHK_CONFIG", "debug")    
    check_host = config.get("CHK_CONFIG", "check_host")    
    check_msg = check_host + " "
    check_code = 0
    
    print "----- CHECK DISK -----" 
    disk_chk_pnt = config.getint("CHK_CONFIG", "disk_chk_percent")
    (error_code,error_msg) = check_disk_space(disk_chk_pnt)
    check_msg += error_msg
    check_code += error_code
 
    print "----- CHECK CPU -----" 
    cpu_chk_pnt  = config.getint("CHK_CONFIG", "cpu_chk_percent")
    (error_code,error_msg) = check_cpu(cpu_chk_pnt)
    check_msg +=',' + error_msg
    check_code += error_code
    
    print "----- CHECK MEMORY -----" 
    mem_chk_pnt  = config.getint("CHK_CONFIG", "mem_chk_percent")
    (error_code,error_msg) = check_memory(mem_chk_pnt)
    check_msg +=',' + error_msg
    check_code += error_code
   

    print "----- CHECK PROCESS -----" 
    (error_code,error_msg) = check_process_linux()
    check_msg +=',' + error_msg
    check_code += error_code

    #print error_msg,chardet.detect(error_msg)    
    if ( config.getint("DB_CONFIG", "enable") == 1 ) :
        print "----- CHECK DATABASE -----" 
        db_string = config.get("DB_CONFIG", "db_string")
        tbs_chk_pnt = config.get("DB_CONFIG","tbs_chk_percent") 
        (error_code,error_msg) = check_db(db_string,tbs_chk_pnt)
    
        check_msg += ',' + error_msg 
        check_code += error_code
    
    print "----- CHECK FILE UPLOAD -----" 
    
    (error_code,error_msg) =check_file_linux()
    check_msg +=',' + error_msg
    check_code += error_code


    print "check_code:" + str(check_code) + "check_msg:" + check_msg

    send_msg(check_msg)

    
   
