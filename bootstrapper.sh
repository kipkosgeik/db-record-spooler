#!/bin/sh

#executes record-spooler.py
#If we want to run in a specific python version/environment we can swith environment using virtualenv


cd /home/kenny/bulk-reports
#Write to log file
echo "$(date) - $(python records-spooler.py)" >> out.log
