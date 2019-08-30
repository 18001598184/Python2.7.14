#!/bin/sh
. /etc/profile
export mydate=`date +%Y%m%d`
export myfile=mon$mydate.log
echo $myfile
cd /home/oracle/pro_monitor

/usr/local/bin/python ./pro_mon_linux.py >> ./$myfile
