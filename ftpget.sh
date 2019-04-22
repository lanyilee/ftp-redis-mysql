#!/bin/sh
lftp << EOF
open $1
user $2 $3
echo "success connect to ftp">>ftpout.txt 
cd ./pe
lcd ./files
get $4
echo "success download file from ftp" >>../ftpout.txt 
bye
EOF
echo "success"
exit 0
