#change execute permission
#chmod +x rd_share_data.sh

#add crontab
#* * * * * /home/oracle/rd_share_data/rd_share_data_zhanjiang/rd_share_data_zhanjiang.sh

export LD_LIBRARY_PATH=/home/oracle/app/product/11.2.0/dbhome_1/lib:/usr/lib:/usr/lib64
export PATH=/home/oracle/app/product/11.2.0/dbhome_1/bin:$PATH
export ORACLE_HOME=/home/oracle/app/product/11.2.0/dbhome_1
export NLS_LANG=.AL32UTF8

mysql_ip=219.132.57.36
mysql_port=3306
mysql_dbname=sengen
mysql_username=nanjingsengen
mysql_password="1Q2we3as#$%^&a123"

install_dir=/home/oracle/rd_share_data

cd ${install_dir}
/usr/local/bin/python ./rd_share_data_zhanjiang.py
