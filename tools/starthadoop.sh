#!/bin/bash

echo "Starting Hadoop Stuff ..."

for service in /etc/init.d/hadoop-*
do
sudo $service start
done

echo "Done"
