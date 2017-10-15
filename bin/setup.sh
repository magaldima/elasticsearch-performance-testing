#!/usr/bin/env bash

yum -y update -y;
yum -y remove java-1.7.0;
yum -y install java-1.8.0;

rpm -i 'https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.3.rpm'

chkconfig --add elasticsearch

/usr/share/elasticsearch/bin/elasticsearch-plugin install discovery-ec2 -b

echo '
discovery:
    zen.hosts_provider: ec2
network.host: _ec2_
' >> /etc/elasticsearch/elasticsearch.yml

echo '
-Xmx15g
-Xms15g
' >> /etc/elasticsearch/jvm.options
