#!/bin/sh

#executes record-spooler.py
#If we want to run in a specific python version/environment we can swith environment using virtualenv

#Write to log file
echo "$(date) Starting.."
echo "$(date) - $(python records-spooler.py)" >> out.log
echo "$(date) Completed"
