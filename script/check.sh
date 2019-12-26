#!/bin/sh
export PATH=$PATH:/usr/local/bin
source /root/virtlen/py3/bin/activate
cd  /root/ip/
python check_ip.py
