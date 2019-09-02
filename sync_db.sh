#!/bin/sh

echo "
*/5 * * * * /home/oracle/sync_db/sync_db.sh
">/dev/null


/usr/bin/python /home/oracle/sync_db/sync_db.py

