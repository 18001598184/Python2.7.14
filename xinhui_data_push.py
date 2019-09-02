# -*- coding: utf-8 -*-
from __future__ import division
import re, os, time, logging
import ConfigParser
from ftplib import FTP
import ftplib
import socket
import math
import shutil

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

# 产生侦码文件
def gen_detect_file(bfile, localpath):

    fp1 = open(bfile, 'rb+')
    in_file_list = fp1.readlines()
    fp1.close()
    fileno = math.ceil(len(in_file_list) / 5000)

    for i in range(int(fileno)):
        loctime = time.localtime()
        str_time = str(int(round(time.time()*1000)))
        timestrf = time.strftime('%Y%m%d%H%M%S', loctime)
        datafilename = localpath + "/" + "THIRD_790413984_" + timestrf + "_" + str_time + ".dat"
        fp2 = open(datafilename, "ab+")
        n = 1

        for line in in_file_list:
            if n <= 5000:
                if len(line.split(",")) == 17:
                    line = line.strip("\n").strip("\r")
                    info, devicemodel, start_time, imsi, imei, longitude, latitude, record_time, device_type, tmsi, \
                rssi, band, mac, device_code, vendor_name, plmn, tel_number = line.split(",")
                else:
                    logger.info("[gen_detect_file]field count in this line is less than 20, ignore the line [%s]" % line)
                    continue
                one_file = []
                if record_time:
                    one_file.append(str(int(time.mktime(time.strptime(record_time, '%Y-%m-%d %H:%M:%S')))))
                else:
                    logger.warning("[gen_detect_file]cap time can't find in this line, ignore the line [%s]" % line)
                    continue
                one_file.append("27")
                one_file.extend([''] * 4)
                if imsi:
                    one_file.append(imsi)
                else:
                    logger.warning("[gen_detect_file]imsi can't find in this line, ignore the line [%s]" % line)
                    continue
                if imei:
                    one_file.append(imei[0:-1])
                else:
                    one_file.append('')
                one_file.extend([''] * 3)

                # update by limuxia 2019.4.17 start 
                if not device_code:
                    logger.warning("[gen_detect_file]device code can't find in this line, ignore the line [%s]" % line)
                    continue
                if len(device_code) > 50:
                    logger.warning("[gen_detect_file]the length of device code in this line is more than 50, ignore the line [%s]" % device_code)
                    continue
                if code != '':
                    if device_code not in code.split(":"):
                        continue
                one_file.append(device_code)
                # update by limuxia 2019.4.17 end
                if longitude:
                    longitude = "%.6f" % float(longitude)
                    one_file.append(str(longitude))
                else:
                    one_file.append('')
                if latitude:
                    latitude = "%.6f" % float(latitude)
                    one_file.append(str(latitude))
                else:
                    one_file.append('')
                one_file.extend([''] * 4)
                if tel_number:
                    msisdn = tel_number + "0000"
                    one_file.append(msisdn)
                else:
                    one_file.append('')
                one_file.extend([''] * 2)
                if len(one_file) == 21:
                    fp2.write(','.join(one_file) + "\n")
                else:
                    logger.warning("[gen_detect_file]length of final detect data is error.")
                    continue
                n = n + 1
            else:
                in_file_list = in_file_list[5000:]
                number = str(n - 1)
                logger.info("[gen_detect_file][" + number + "/" + "5000" + "]" + " detect info has created ok!")
                break
        fp2.close()
    number = str(n - 1)
    logger.info("[gen_detect_file]in tatal [" + number + "/" + str(len(in_file_list)) + "]" + " detect info has created ok!")

    
# Added by limuxia 2019.4.16 start
# 生产指定格式的心跳文件
def gen_heart_file(bfile, localpath):
    fp1 = open(bfile, 'rb+')
    in_file_list = fp1.readlines()
    fp1.close()
    fileno = math.ceil(len(in_file_list) / 5000)

    for i in range(int(fileno)):
        loctime = time.localtime()
        str_time = str(int(round(time.time()*1000)))#时间戳
        datafilename = localpath + "/" + "1001" +"_HEART_BEAT_" + str_time + ".dat"
        fp2 = open(datafilename, "ab+")
        n = 1

        for line in in_file_list:
            if n <= 5000:
                # 心跳文件格式验证
                line_split = line.split(",")
                if len(line_split) == 20:
                    device_code = line_split[3]
                else:
                    logger.warning("[gen_heart_file]field count in this line is less than 20, ignore the line [%s]" % line)
                    continue
                one_file = []
                one_file.append("0008")
                one_file.append("8")
                one_file.append("")
                # update by limuxia 2019.4.17 start 
                if not device_code:
                    logger.warning("[gen_heart_file]device code can't find in this line, ignore the line [%s]" % line)
                    continue
                if len(device_code) > 50:
                    logger.warning("[gen_heart_file]the length of device code in this line is more than 50, ignore the line [%s]" % device_code)
                    continue
                if code != '':
                    if device_code not in code.split(":"):
                        continue
                one_file.append(device_code)
                # update by limuxia 2019.4.17 end
                one_file.append("01")
                fp2.write("\t".join(one_file) + "\n")
                n = n + 1
            else:
                in_file_list = in_file_list[5000:]
                number = str(n - 1)
                logger.info("[gen_heart_file][" + number + "/" + "5000" + "] " + "heart beat has created ok!")
                break
        fp2.close()
    number = str(n - 1)
    logger.info("[gen_heart_file]in tatal [" + number + "/" + str(len(in_file_list)) + "]" + " heart beat has created ok!")
    
# 读取配置:[HEART BEAT]配置项为空时，默认会读取[DETECT INFO]中相应的配置项
def read_config(config):
    config_dict = {}
    config_dict['fun_switch'] = [config.get("DETECT INFO", "fun_switch"), config.get("HEART BEAT", "fun_switch")]
    # 启用项fun_switch为必填项：0/1
    for fun_switch in config_dict['fun_switch']:    
        if fun_switch not in ('0', '1'):
            logger.error("[read_config]fun_switch must be 0 or 1.")
            exit()
    ftp_switch = config.get("DETECT INFO", "ftp_switch")
    if config.get("HEART BEAT", "ftp_switch") == '':
        config_dict['ftp_switch'] = [ftp_switch, ftp_switch]
    else:
        config_dict['ftp_switch'] = [ftp_switch, config.get("HEART BEAT", "ftp_switch")]
    ftp_ip = config.get("DETECT INFO", "ftp_ip")
    if config.get("HEART BEAT", "ftp_ip") == '':
        config_dict['ftp_ip'] = [ftp_ip, ftp_ip]
    else:
        config_dict['ftp_ip'] = [ftp_ip, config.get("HEART BEAT", "ftp_ip")]    
    ftp_port = config.get("DETECT INFO", "ftp_port")        
    if config.get("HEART BEAT", "ftp_port") == '':
        config_dict['ftp_port'] = [ftp_port, ftp_port]
    else:
        config_dict['ftp_port'] = [ftp_port, config.get("HEART BEAT", "ftp_port")]    
    ftp_username = config.get("DETECT INFO", "ftp_username")        
    if config.get("HEART BEAT", "ftp_username") == '':
        config_dict['ftp_username'] = [ftp_username, ftp_username]
    else:
        config_dict['ftp_username'] = [ftp_username, config.get("HEART BEAT", "ftp_username")]    
    ftp_password = config.get("DETECT INFO", "ftp_password")        
    if config.get("HEART BEAT", "ftp_password") == '':
        config_dict['ftp_password'] = [ftp_password, ftp_password]
    else:
        config_dict['ftp_password'] = [ftp_password, config.get("HEART BEAT", "ftp_password")]    
    remotepath = config.get("DETECT INFO", "remotepath")        
    if config.get("HEART BEAT", "remotepath") == '':
        config_dict['remotepath'] = [remotepath, remotepath]
    else:
        config_dict['remotepath'] = [remotepath, config.get("HEART BEAT", "remotepath")]   
    localpath = config.get("DETECT INFO", "localpath")
    if config.get("HEART BEAT", "localpath") == '':
        config_dict['localpath'] = [localpath, localpath]
    else:
        config_dict['localpath'] = [localpath, config.get("HEART BEAT", "localpath")]  
    newdir = config.get("DETECT INFO", "newdir")
    if config.get("HEART BEAT", "newdir") == '':
        config_dict['newdir'] = [newdir, newdir]
    else:
        config_dict['newdir'] = [newdir, config.get("HEART BEAT", "newdir")]
    backup = config.get("DETECT INFO", "backup")
    if config.get("HEART BEAT", "backup") == '':
        config_dict['backup'] = [backup, backup]
    else:
        config_dict['backup'] = [backup, config.get("HEART BEAT", "backup")]
    config_dict['code'] = config.get("DEVICE", "code")
    config_dict['filter'] = [config.get("DETECT INFO", "filter"), config.get("HEART BEAT", "filter")]
    config_dict['period'] = config.get("REPORT", "period")
    for i in range(2):
        if config_dict['newdir'][i] == '':
            config_dict['newdir'][i] = './newback'
        if config_dict['localpath'][i] == '':
            config_dict['localpath'][i] = './data'
    return config_dict

    
def check_config(config_dict, type, desb):
    if config_dict['ftp_switch'][type] not in ('0', '1'):
        logger.error("[check_config]%sftp_switch must be 0 or 1." % desb)
        exit()
    if config_dict['backup'][type] == '':
        logger.error("[check_config]:%sbackup can't be null." % desb)
        exit()
    if config_dict['filter'][type] == '':
        logger.error("[check_config]:%sfilter can't be null." % desb)
        exit()
    # 上报周期默认为1分钟
    period = config_dict['period']
    if period == '':
        config_dict['period'] = 60
    else:
        try:
            config_dict['period'] = float(period)
        except ValueError:
            logger.error("[check_config][REPORT].period must be a number")
            exit()
    # FTP功能启用时，ip,port,uname,upwd不允许为空
    if config_dict['ftp_switch'][type] == '0':
        return
    if '' in (config_dict['ftp_ip'][type], config_dict['ftp_port'][type], 
              config_dict['ftp_username'][type], config_dict['ftp_password'][type]):
        logger.error("[check_config]ftp_ip & ftp_port & ftp_username & ftp_password cat't be null when ftp_switch=1.")
        exit() 
# Added by limuxia 2019.4.16 end


def fun_set_logger(log_file):
    logger.setLevel(logging.DEBUG)
    LOG_FORMAT = "[%(asctime)s]-[%(levelname)s] %(message)s"

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # output log to file
    logger_FileHandler = logging.FileHandler(log_file)
    logger_FileHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger_FileHandler.setLevel(logging.INFO)
    logger.addHandler(logger_FileHandler)
    # output log to command-line console
    logger_StreamHandler = logging.StreamHandler()
    logger_StreamHandler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger_StreamHandler.setLevel(logging.INFO)
    logger.addHandler(logger_StreamHandler)


def ftpconnect(ftp_ip, ftp_port, ftp_username, ftp_password):
    ftp = FTP()
    try:
        ftp.connect(ftp_ip, ftp_port)
    except (socket.error, socket.gaierror):
        logger.warning("[ftpconnect]Can't connect to the ftp server.")
        return False
    try:
        ftp.login(ftp_username, ftp_password)
    except ftplib.error_perm:
        logger.warning("[ftpconnect]User %s can't login the ftp server." % ftp_username)
        return False
    return ftp


def uploadfile(ftp, remotepathfile, localpathfile):
    #ftp传输开启主动模式
    ftp.set_pasv(0)
    bufsize = 2048
    #判断本地文件是否为空，如果空不上传，直接删除
    if not os.stat(localpathfile).st_size:
        logger.info("[uploadfile]file %s is empty,drop it." % localpathfile)
        os.remove(localpathfile)
        ftp.quit()
        return
    try:
        filepath, filename = os.path.split(remotepathfile)
        ftp.cwd(filepath.lstrip("."))
        #检查远程ftp是否有同名文件,如果有则丢弃本地文件
        if filename in ftp.nlst():
            logger.info("[uploadfile]file %s has exist in ftp server,drop local file." % filename)
            os.remove(localpathfile)
            ftp.quit()
            return
        else:
            ftptmpfile = filename + '.tmp'
            fp = open(localpathfile, 'rb')
            ftp.storbinary('STOR ' + ftptmpfile, fp, bufsize)
            logger.debug("[uploadfile]upload .tmp file:%s ok!" % ftptmpfile)
            fp.close()
            ftp.rename(ftptmpfile.lstrip("."), remotepathfile.lstrip("."))
            logger.info("[uploadfile]upload file %s ok." % remotepathfile)
            tfile = filename + ".tmp"
            os.remove(localpathfile)
            if tfile in ftp.nlst():
                ftp.delete(tfile.lstrip("."))
                logger.info("")
    except:
            logger.warning("[uploadfile]upload file %s error." % filename)
            ftp.quit()
            return
    ftp.quit()


def main_process(type, file_re, gen_file):
    switch = cfg_dict['ftp_switch'][type]
    ftp_ip = cfg_dict['ftp_ip'][type]
    ftp_port = cfg_dict['ftp_port'][type]
    ftp_username = cfg_dict['ftp_username'][type]
    ftp_password = cfg_dict['ftp_password'][type]
    remotepath = cfg_dict['remotepath'][type]
    backup = cfg_dict['backup'][type]
    newdir = cfg_dict['newdir'][type]
    localpath = cfg_dict['localpath'][type]
    filter = cfg_dict['filter'][type]
    
    # Added by Limuxia 2019.4.17 start
    # backup目录检测
    if not os.path.isdir(backup):
        logger.error("[main_process]Can't find the bakup dir:%s" % backup)
        exit()
    # Added by Limuxia 2019.4.17 end
    
    for bfile in os.listdir(backup):
        myfile = backup + "/" + bfile
        # 目标文件名称检测
        if not re.match('^'+filter+'.*\.log$', bfile, re.M | re.I):
            continue
        # 重复文件检测
        if os.path.split(myfile)[1] in os.listdir(newdir):
            logger.warning("[main_process]%s has exist in newdir,drop it." % myfile)
            os.remove(myfile)
            continue
        # Added by limuxia 2019.4.17 start
        # 空文件不做处理
        if not os.stat(myfile).st_size:
            continue
        # Added by limuxia 2019.4.17 end
        # 生成指定格式的文件
        gen_file(myfile, localpath)
        
        # 将已处理的文件从bakup移动到newdir        
        path, name = os.path.split(myfile)
        newname = newdir + "/" + name
        shutil.move(myfile, newname)
    
    if (switch == "0"):
        return    
    file_list = []
    file_list = os.listdir(localpath)
    if not file_list:
        return
    # 搜索数据文件目录，上传FTP    
    for ffile in file_list:
        if not re.match(file_re, ffile, re.M | re.I):
            continue
        myftp = ftpconnect(ftp_ip, ftp_port, ftp_username, ftp_password)
        if not myftp:
            return
        remotefile = remotepath + "/" + ffile
        localfile = localpath + "/" + ffile
        uploadfile(myftp, remotefile, localfile)
    
    
if __name__ == "__main__":
    config_file = "xinhui_data_push.ini"
    log_file = "xinhui_data_push.log"
    logger = logging.getLogger()
    fun_set_logger(log_file)
    if (not os.path.isfile(config_file)):
        logger.error("[main]config file %s is missing." % config_file)
        exit()
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    cfg_dict = read_config(config)
    #配置文件检查
    if cfg_dict['fun_switch'][0] == '1':
        check_config(cfg_dict, 0, "[DETECT INFO]")
    if cfg_dict['fun_switch'][1] == '1':
        check_config(cfg_dict, 1, "[HEART BEAT]")
        
    # 设备编码
    code = cfg_dict['code']
    
    while True:
        if cfg_dict['fun_switch'][0] == '1':
            if not os.path.isdir(cfg_dict['localpath'][0]):
                os.makedirs(cfg_dict['localpath'][0])
            if not os.path.isdir(cfg_dict['newdir'][0]):
                os.makedirs(cfg_dict['newdir'][0])
            main_process(type=0, file_re=r'^(THIRD.*\.dat)$', gen_file=gen_detect_file)
            
        if cfg_dict['fun_switch'][1] == '1':
            if not os.path.isdir(cfg_dict['localpath'][1]):
                os.makedirs(cfg_dict['localpath'][1])
            if not os.path.isdir(cfg_dict['newdir'][1]):
                os.makedirs(cfg_dict['newdir'][1])
            main_process(type=1, file_re=r'^(1001_HEART_BEAT.*\.dat)$', gen_file=gen_heart_file)
        logger.info("[main]sleep %s s" % cfg_dict['period'])
        time.sleep(cfg_dict['period'])

