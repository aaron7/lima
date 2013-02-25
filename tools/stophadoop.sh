#!/bin/bash

echo "Stopping Hadoop Stuff ..."

for service in /etc/init.d/hadoop-*
do
sudo $service stop
done

echo "Done"
