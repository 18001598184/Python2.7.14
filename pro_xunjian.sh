#!/bin/sh
. /etc/profile
export mydate=`date +%Y%m%d`
export deldate=`date -d "90 days ago" +%Y%m%d`
export myfile=xunjian$mydate.log
echo $myfile
cd /home/oracle/pro_xunjian

/usr/local/bin/python ./pro_xunjian_linux.py >> ./$myfile

rm -rf mon${deldate}.log
