#! /usr/bin/python
# coding=utf-8
# require install aiohttp, aiodns

import urllib.request, urllib.error, urllib.parse
from time import sleep
from datetime import datetime
import re
import os
import asyncio, aiohttp
import logging
import json

webui_api = R"http://127.0.0.1:8088/api/v2/torrents/"
# trackers保存路径
trackers_file_path = os.path.join(os.environ["HOME"], R".local/share/qBittorrent/trackers_list")
# 获取trackers的url
trackers_list_url = R"https://cf.trackerslist.com/all.txt"

# 检查tracker url重试次数
CHECK_RETRY = 3
# 检查tracker url连接超时时间，单位秒
CHECK_TIMEOUT = 15
# 每秒发送请求数量
REQUESTS_PER_SECOND = 10
SLEEP_TIME = 1 / REQUESTS_PER_SECOND
# 最大并发连接数量
CONCURRENCY = 100

LOG_LEVEL = logging.INFO
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

transaction_id = bytes(4) + os.urandom(4)
TRACKER_UDP_CONNECTION_REQUEST = bytes.fromhex("0000041727101980") + transaction_id

def read_trackers_file() -> list[str]:
    trackers_list = list()
    try:
        file_dt = datetime.fromtimestamp(os.path.getmtime(trackers_file_path))
        interval = datetime.now() - file_dt
        if interval.days == 0 and interval.seconds < 43200:
            with open(trackers_file_path, "r") as f:
                trackers_list = [line for line in f.read().split(os.linesep) if len(line) > 0]
    except Exception:
        pass

    return trackers_list


def write_trackers_file(trackers_list:list[str]):
    if trackers_list:
        with open(trackers_file_path, "w") as f:
            f.write(os.linesep.join(trackers_list))


def get_trackers_list() -> (list[str], list[str]):
    logger.info("get trackers list...")
    trackers_list = read_trackers_file()
    if not trackers_list:
        html_content = ""
        for _ in range(0, 5):
            try:
                with urllib.request.urlopen(trackers_list_url) as response:
                    html_content = response.read().decode("utf8")
                    break
            except urllib.error.HTTPError as e:
                logger.warning(f"发生HTTP错误: {e.code} {e.reason}")
            except urllib.error.URLError as e:
                logger.warning(f"发生URL错误: {e.reason}")
            sleep(5)
        else:
            logger.error("无法访问 %s" % trackers_list_url)

        trackers_list = [line.strip() for line in html_content.split("\n\n") if len(line.strip()) > 0]
        write_trackers_file(trackers_list)

    return filter_protocol(trackers_list)


def filter_protocol(trackers_list:list[str]) -> (list[str], list[str]):
    http_trackers, udp_trackers = list(), list()

    reg_http = re.compile(R"^https?://[^/].*", re.I)
    reg_udp = re.compile(R"^udp://[^/].*", re.I)
    for url in trackers_list:
        if reg_http.match(url):
            http_trackers.append(url)
        elif reg_udp.match(url):
            udp_trackers.append(url)

    return http_trackers, udp_trackers


async def check_trackers(http_trackers:list[str], udp_trackers:list[str]) -> list[str]:
    logger.info("check trackers...")
    semaphore = asyncio.Semaphore(CONCURRENCY)
    http_timeout = aiohttp.ClientTimeout(sock_connect=CHECK_TIMEOUT, total=CHECK_TIMEOUT*2)

    tasks = list()
    for url in http_trackers:
        tasks.append(asyncio.create_task(check_http_tracker(url, semaphore, http_timeout)))
        await asyncio.sleep(SLEEP_TIME)

    for url in udp_trackers:
        tasks.append(asyncio.create_task(check_udp_tracker(url, semaphore)))
        await asyncio.sleep(SLEEP_TIME)

    results = await asyncio.gather(*tasks)

    return [url for url in results if url]


async def check_http_tracker(url:str, semaphore:asyncio.Semaphore, http_timeout:aiohttp.ClientTimeout) -> str:
    ret = ""
    async with semaphore:
        for _ in range(0, CHECK_RETRY):
            try:
                logger.debug(f"HTTP GET: {url}\n")
                async with aiohttp.request("GET", url, timeout=http_timeout) as resp:
                    if resp.status == 200:
                        ret = url
                    logger.debug(f"{resp.status}: {url}\n")
                    break
            except TimeoutError:
                logger.debug(f"connection timeout: {url}\n")
            except Exception:
                logger.debug(f"connect failed: {url}\n")
                break
    return ret


async def check_udp_tracker(url:str, semaphore:asyncio.Semaphore) -> str:
    class UDPTrackerRequest(asyncio.DatagramProtocol):
        def __init__(self, future):
            self.future = future
            self.transport = None

        def connection_made(self, transport):
            self.transport = transport
            self.transport.sendto(TRACKER_UDP_CONNECTION_REQUEST)

        def datagram_received(self, data, addr):
            if not self.future.done():
                self.future.set_result(data)
            self.transport.close()

        def error_received(self, exc):
            pass

        def connection_lost(self, exc):
            if not self.future.done():
                self.future.set_result()

    ret = ""
    p = urllib.parse.urlparse(url)
    host, port = p.hostname, p.port
    loop = asyncio.get_running_loop()

    async with semaphore:
        for _ in range(0, CHECK_RETRY):
            future = loop.create_future()
            try:
                udp_transport, _ = await loop.create_datagram_endpoint(
                    lambda: UDPTrackerRequest(future),
                    remote_addr=(host, port)
                )

                logger.debug(f"udp request: {url}\n")
                tracker_resp = await asyncio.wait_for(future, CHECK_TIMEOUT)
                if len(tracker_resp) == 16 and tracker_resp[:8] == transaction_id:
                    ret = url
                    logger.debug(f"response ok: {url}\n")
                    break

                logger.debug(f"response error: {url}\n{tracker_resp}\n")
                await asyncio.sleep(5)
            except TimeoutError:
                logger.debug(f"connection timeout: {url}\n")
            except Exception:
                logger.debug(f"connect failed: {url}\n")
                break
            finally:
                if "udp_transport" in locals():
                    udp_transport.close()

    return ret


def get_running_torrents_hash_list() -> list[str]:
    logger.info("get running torrents hash list...")
    url = webui_api + R"info?filter=running"
    hash_list, torrents_list = list(), list()
    try:
        with urllib.request.urlopen(url) as response:
            torrents_list = json.load(response)
    except urllib.error.HTTPError as e:
        logger.warning(f"发生HTTP错误: {url} {e.code} {e.reason}")
    except urllib.error.URLError as e:
        logger.warning(f"发生URL错误: {url} {e.reason}")

    for t in torrents_list:
        hash_list.append(t["hash"])

    return hash_list


def get_torrents_trackers(hash_list:list[str]) -> dict[str:list]:
    logger.info("get torrents trackers...")
    torrents_trackers = dict()
    for hash in hash_list:
        url = webui_api + fR"trackers?hash={hash}"
        try:
            with urllib.request.urlopen(url) as response:
                torrents_trackers[hash] = json.load(response)
        except urllib.error.HTTPError as e:
            logger.warning(f"发生HTTP错误: {url} {e.code} {e.reason}")
        except urllib.error.URLError as e:
            logger.warning(f"发生URL错误: {url} {e.reason}")
        sleep(SLEEP_TIME)

    return torrents_trackers


def filter_torrents_trackers(torrents_trackers:dict[str:list], new_trackers:set[str]) -> dict[str:dict]:
    trackers_filter = dict()
    working_all, not_working_all = set(), set()

    reg_url = re.compile(R"^(udp|https?)://[^/].*", re.I)
    for key, trackers_list in torrents_trackers.items():
        not_working, working = set(), set()
        trackers_filter[key] = {
            "working": working,
            "not_working": not_working
        }
        for t in trackers_list:
            if reg_url.match(t["url"]):
                if t["status"] == 4:
                    not_working.add(t["url"])
                    not_working_all.add(t["url"])
                elif t["status"] == 2:
                    working.add(t["url"])
                    working_all.add(t["url"])
                else:
                    working.add(t["url"])

    not_working_all -= working_all
    new_trackers |= working_all
    new_trackers -= not_working_all

    for v in trackers_filter.values():
        v["new"] = new_trackers - v["working"]
        del v["working"]

    return trackers_filter


def update_torrents_trackers(trackers_update:dict[str:dict]):
    logger.info("update torrents trackers...")
    add_trackers_url = webui_api + R"addTrackers"
    remove_trackers_url = webui_api + R"removeTrackers"

    for hash, v in trackers_update.items():
        if v["not_working"]:
            payload = {
                "hash": hash,
                "urls": "|".join(v["not_working"])
            }
            encoded_data = urllib.parse.urlencode(payload).encode("utf-8")
            req = urllib.request.Request(remove_trackers_url, data=encoded_data, method="POST")
            try:
                with urllib.request.urlopen(req) as response:
                    if response.status == 200:
                        logging.debug(f"{hash}: remove trackers success")
            except urllib.error.HTTPError as e:
                logger.warning(f"{hash}: remove trackers 发生HTTP错误: {e.code} {e.reason}")
            except urllib.error.URLError as e:
                logger.warning(f"{hash}: remove trackers 发生URL错误: {e.reason}")
            sleep(SLEEP_TIME)

        if v["new"]:
            payload = {
                "hash": hash,
                "urls": "\n".join(v["new"])
            }
            encoded_data = urllib.parse.urlencode(payload).encode("utf-8")
            req = urllib.request.Request(add_trackers_url, data=encoded_data, method="POST")
            try:
                with urllib.request.urlopen(req) as response:
                    if response.status == 200:
                        logging.debug(f"{hash}: add trackers success")
            except urllib.error.HTTPError as e:
                logger.warning(f"{hash}: add trackers 发生HTTP错误: {e.code} {e.reason}")
            except urllib.error.URLError as e:
                logger.warning(f"{hash}: add trackers 发生URL错误: {e.reason}")
            sleep(SLEEP_TIME)


if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=LOG_LEVEL)

    http_trackers, udp_trackers = get_trackers_list()
    trackers = asyncio.run(check_trackers(http_trackers, udp_trackers))

    hash_list = get_running_torrents_hash_list()
    torrents_trackers = get_torrents_trackers(hash_list)
    trackers_update = filter_torrents_trackers(torrents_trackers, set(trackers))
    update_torrents_trackers(trackers_update)

    logger.info("All done.")
