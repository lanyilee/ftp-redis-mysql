#!/bin/sh
rm $1 
time=$(date +%Y-%m-%d\ %H:%M:%S)
echo $time >> ./log/rmout.txt  
echo "success delete files" >>./log/rmout.txt  
exit 0
