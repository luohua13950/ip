__author__ = 'luohua139'
import requests
import random
import agent
import re
from lxml import etree
def req():
    url = "https://www.66ip.cn/{}.html"
    resp = requests.get(url.format(2))

    html_xpath = etree.HTML(resp.text)
    x_tr = html_xpath.xpath("//table//tr")
    for tr in x_tr[2:]:
        ip = tr.xpath("td[1]/text()")
        port = tr.xpath("td[2]/text()")
        tt = tr.xpath("td[5]/text()")[0]
        aa = re.findall("\d+",tt)
        bb = "-".join(aa[:-1])
        str_times =  bb+ " 00:00:00"
        print(ip,port,str_times)

def tts(*args):
    if "need_handle_time" in args:
        print("need_handle_time" in args)

if __name__ == '__main__':
    import re
    from crawl_ip import Crawl
    msg = "need_handle_time"
    aa = Crawl().check_invalid_day("2019-12-26 00:00:00")
    req()

