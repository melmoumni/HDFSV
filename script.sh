#!/bin/bash

green='\e[0;32m'
GREEN='\e[0;32m'
red='\e[0;31m'
RED='\e[1;31m'
blue='\e[0;34m'
BLUE='\e[1;34m'
cyan='\e[0;36m'
CYAN='\e[1;36m'
NC='\e[0m'

if [ $# -eq 0 ]; then 
		echo -e "${RED}Usage: ${NC}./script.sh ${CYAN}port $NC"
		exit
fi

if [ -z "$HADOOP_CONF" ]; then
    echo -e "${RED}You need to set the HADOOP_CONF environnement variable to the absolute path of your hadoop core-site.xml in your ${cyan}~/.bashrc${NC} ${RED}and ${cyan}source ~/.bashrc.\n${NC}Example: ${green}echo \"export HADOOP_CONF=/opt/hadoop-2.6.4/etc/hadoop/core-site.xml\" >> ~/.bashrc \n${NC}Refer to the documentation for additional informations."
    exit 1
fi

if [ ! -f $HADOOP_CONF ]; then
    echo -e "${red}The file $HADOOP_CONF does not exists $NC"
fi

echo -e "${GREEN}Your HADOOP_CONF is $HADOOP_CONF $NC"



if [ -z "$HIVE_CONF" ]; then
    echo -e "${BLUE}You may want to set the HIVE_CONF environnement variable to the absolute path of your hive hive-site.xml in your ${cyan}~/.bashrc${NC} ${BLUE}and ${cyan}source ~/.bashrc.\n${NC}Refer to the documentation for additional informations."
fi

if [ -z "$HBASE_CONF" ]; then
    echo -e "${BLUE}You may want to set the HBASE_CONF environnement variable to the absolute path of your hbase hbase-site.xml in your ${cyan}~/.bashrc${NC} ${BLUE}and ${cyan}source ~/.bashrc.\n${NC}Refer to the documentation for additional informations."
fi

if [ -f $HIVE_CONF ]; then
		echo -e "${GREEN}Your HIVE_CONF is $HIVE_CONF $NC"
else
		echo -e "${blue}The file $HIVE_CONF does not exists. The hive functionnalities won't be available.${NC}"
fi

if [ -f $HBASE_CONF ]; then
		echo -e "${GREEN}Your HBASE_CONF is $HBASE_CONF $NC"
else
		echo -e "${blue}The file $HBASE_CONF does not exists. The hbase functionnalities won't be available.${NC}"
fi

./wildfly-10.0.0.Final/bin/standalone.sh -Djboss.http.port=$1
