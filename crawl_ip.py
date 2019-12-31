__author__ = 'luohua139'
import requests
import redis
import logging
import configparser
import datetime
import time
import re
from lxml import etree
from threading import Thread
import threading
from multiprocessing import Process


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s ： %(message)s',
filename="crawl_ip.log", filemode="a")
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s ： %(message)s', )
logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
}


class Config():
    def __init__(self):
        self.cg = configparser.ConfigParser()
        self.cg.read("config.ini")

    @property
    def db_config(self):
        sec = self.cg._sections
        db_info = dict(sec["db_config"])
        return db_info


class RedisClient():
    MAX_SCORE = 100
    MIN_SCORE = 0
    DECR_SCORE = -10
    INIT_SCORE = 100
    LIMITED = 10000

    def __init__(self, host, port, password, db, max_connections = 5):
        pool = redis.ConnectionPool(host=host, port=port, password=password, db=db,max_connections = max_connections,decode_responses=True)
        self.redis = redis.StrictRedis(connection_pool= pool)

    def add(self, proxies, score=INIT_SCORE, name="proxies"):
        ret = self.redis.zadd(name=name, mapping={proxies: score})
        if ret == 1:
            logger.info("{} 代理添加成功".format(proxies))
        else:
            if score != self.INIT_SCORE:
                logger.info("{} 分值更新为{}".format(proxies, score))
            else:
                logger.info("{} 代理已存在！".format(proxies))
        return ret

    def decr(self, proxies, score=DECR_SCORE, name="proxies"):
        """
        ret 为当前代理的分数
        :param proxies:
        :param score:
        :param name:
        :return:
        """
        ret = self.redis.zincrby(name, score, proxies, )
        if ret > 0:
            logger.info("{} 当前分数为{},大于0".format(proxies, ret))
        else:
            logger.info("{} 当前分数{},小于0，移除此ip".format(proxies, ret))
            self.remove(proxies=proxies)
        return ret

    def remove(self, proxies, name="proxies"):
        ret = self.redis.zrem(name, proxies)
        if ret > 0:
            logger.info("{}移除成功".format(proxies))
        else:
            logger.error("{}移除失败".format(proxies))
        return ret

    def score(self, proxies, name="proxies"):
        ret = self.redis.zscore(name=name, value=proxies)
        return ret

    def count(self, name="proxies"):
        _count = self.redis.zcard(name=name)
        return _count

    def countByScore(self, name="proxies", mins=MIN_SCORE, maxs=MAX_SCORE):
        ret = self.redis.zcount(name, mins, maxs)
        return ret

    def removeByScore(self, name="proxies", mins=MIN_SCORE, maxs=MAX_SCORE):
        ret = self.redis.zremrangebyscore(name, mins, maxs)
        if ret > 0:
            logger.info("移除成功{}个".format(ret))
        return ret

    def batch(self, name="proxies", start=0, end=10, desc=False, withscores=False):
        ret = self.redis.zrange(name, start, end, desc=desc, withscores=withscores)
        return ret


class BaseClass(type):
    def __new__(cls, name, base, attrs):
        attrs["cw_func"] = []
        count = 0
        for k, v in attrs.items():
            if k.startswith("crawl_"):
                attrs["cw_func"].append(k)
                count += 1
        attrs["func_count"] = count
        return type.__new__(cls, name, base, attrs)


class Crawl(metaclass=BaseClass):
    register_website_map = {
        "crawl_kuaidaili": "快代理",
        "crawl_xici": "西刺代理",
        "crawl_89": "89代理",
        "crawl_66": "66代理",
    }
    dest_url_map = {
        "快代理": "https://www.kuaidaili.com/free/inha/{}/",
        "西刺代理": "https://www.xicidaili.com/nn/{}",
        "89代理": "http://www.89ip.cn/index_{}.html",
        "66代理": "http://www.66ip.cn/{}.html",
    }

    def __init__(self, valid_period=10):
        db_info = Config().db_config
        self.rc = RedisClient(**db_info)
        self.valid_period = valid_period
        self.base = datetime.timedelta(days=valid_period)
        self.sleep_sec = 30

    def check_invalid_day(self, str_time):
        try:
            day = datetime.datetime.now()
            dt = datetime.datetime.strptime(str_time.split()[0], "%Y-%m-%d")
        except Exception as e:
            logger.error("转换时间发生错误:{}".format(e))
            return False
        logger.info("此ip上次验证时间为{}前".format(day.date() - dt.date()))
        return day.date() - dt.date() < self.base

    @staticmethod
    def get_page(url, page):
        resp = requests.get(url=url.format(page), headers=headers)
        logger.info("{}:当前爬取第{}页".format(threading.current_thread().name, page))
        x_page = etree.HTML(resp.text)
        return x_page

    def add_proxies(self, ip, port, http, str_time):
        try:
            http = http.lower()
            if self.check_invalid_day(str_time):
                proxies = "{}://{}:{}".format(http, ip, port)
                self.rc.add(proxies)
                return True
            else:
                return False
        except Exception as e:

            logger.error("存储过程发生异常:{}".format(e))

    def scheduler(self, crawler, url, name):
        page = 1
        while True:
            try:
                html = self.get_page(url, page)
                if not eval("self.{}".format(crawler))(html, name):
                    logger.info(
                        "{}:当前ip校验时间已超过{}天,不再继续爬取后面！".format(threading.current_thread().name, self.valid_period))
                    break
                page += 1
                logger.info("{}睡眠{}秒".format(name, self.sleep_sec))
                time.sleep(self.sleep_sec)
            except Exception as e:
                logger.error("%s爬取过程发生错误：%s" % (name, e))
                break

    def crawl_kuaidaili(self, x_page, name):
        x_tr = x_page.xpath("//tbody//tr")
        if len(x_tr) < 1:
            x_tr = x_page.xpath("//table//tr")
        ip_xpath = "td[1]/text()"
        port_xpath = "td[2]/text()"
        http_xpath = "td[4]/text()"
        str_time_xpath = "td[7]/text()"
        return self.crawler_template(x_tr, ip_xpath, port_xpath, http_xpath, str_time_xpath, name)

    def crawl_xici(self, x_page, name):
        x_tr = x_page.xpath("//tbody//tr")
        if len(x_tr) < 1:
            x_tr = x_page.xpath("//table//tr")
        ip_xpath = "td[2]/text()"
        port_xpath = "td[3]/text()"
        http_xpath = "td[6]/text()"
        str_time_xpath = "td[10]/text()"
        msg = "need_handle_time"
        return self.crawler_template(x_tr, ip_xpath, port_xpath, http_xpath, str_time_xpath, name, msg)

    def crawl_89(self, x_page, name):
        x_tr = x_page.xpath("//table[@class='layui-table']/tbody//tr")
        ip_xpath = "td[1]/text()"
        port_xpath = "td[2]/text()"
        http_xpath = "http"
        str_time_xpath = "td[5]/text()"
        msg = "need_handle_time"
        return self.crawler_template(x_tr, ip_xpath, port_xpath, http_xpath, str_time_xpath, name, msg)

    def crawl_66(self, x_page, name):
        _x_tr = x_page.xpath("//table//tr")
        x_tr = _x_tr[:]
        if len(_x_tr)>1:
            x_tr = _x_tr[2:]
        ip_xpath = "td[1]/text()"
        port_xpath = "td[2]/text()"
        http_xpath = "http"
        str_time_xpath = "td[5]/text()"
        msg = "need_handle_time"
        return self.crawler_template(x_tr, ip_xpath, port_xpath, http_xpath, str_time_xpath, name, msg)

    def crawler_template(self, x_tr, ip_xpath, port_xpath, http_xpath, str_time_xpath, name, *args):
        for tr in x_tr:
            try:
                _ip = tr.xpath(ip_xpath)
                ip = _ip[0].strip() if _ip else ""
                _port = tr.xpath(port_xpath)
                port = _port[0].strip() if _port else ""
                if http_xpath not in ["http","https"]:
                    _http = tr.xpath(http_xpath)
                    http = _http[0].strip() if _http else ""
                else:
                    http = http_xpath
                _str_time = tr.xpath(str_time_xpath)
                str_time = _str_time[0].strip() if _str_time else ""

                if not all([ip, port, http]):
                    continue
                if "need_handle_time" in args:
                    str_time = self.handle_time(name, str_time)
                if not self.add_proxies(ip, port, http, str_time):
                    logger.info("当前ip验证时间为{}".format(str_time))
                    return False
            except Exception as e:
                logger.error("%s代理解析页面发生错误：%s" % (name, e))
                continue
        return True

    def handle_time(self, name, str_times):
        if name == "西刺代理":
            str_times = "20" + str_times
        if name == "66代理":
            _dt = re.findall("\d+",str_times)
            dt = "-".join(_dt[:-1])
            str_times =  dt+ " 00:00:00"
        return str_times

    def run(self):
        th_list = []
        for cw in self.cw_func:
            proxy_website_name = self.register_website_map.get(cw, "")
            url = self.dest_url_map.get(proxy_website_name, "")
            if not url:
                continue
            th_list.append(
                Thread(target=self.scheduler, args=(cw, url, proxy_website_name), name="thread-{}".format(cw)))
            logger.info("创建线程...")
        for th in th_list:
            logger.info("启动线程:{}...".format(th.name))
            th.start()


class Scheduler():
    def __init__(self):
        self.test_url = "http://www.baidu.com"


if __name__ == '__main__':
    cw = Crawl()
    cw.run()

