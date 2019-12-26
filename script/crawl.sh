#!/bin/sh
export PATH=$PATH:/usr/local/bin
echo -n "开始执行爬虫"
source /root/virtlen/py3/bin/activate
cd  /root/ip/
python crawl_ip.py
