from flask import Flask, request, render_template_string
import threading
import requests
import time
import random
import re
import socket
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from urllib.parse import urlparse

app = Flask(__name__)

# ====================== 内存优化 ======================
socket.setdefaulttimeout(10)

OPTIMIZED_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

class LimitedDict(dict):
    def __init__(self, maxlen=500):
        super().__init__()
        self._maxlen = maxlen
    def __setitem__(self, key, value):
        if len(self) >= self._maxlen and key not in self:
            try: del self[next(iter(self))]
            except StopIteration: pass
        super().__setitem__(key, value)

# ====================== 全局配置 ======================
running = False
base_thread = 20
night_thread = 800
day_start_hour = 7
night_start_hour = 0
chunk_size = 262144
req_delay_ms = 10
timeout_s = 15
stall_timeout_s = 8
max_fail_times = 2
single_url_daily_max_gb = 0

weight_speed = 50
weight_video = 30
weight_live = 20

source_default = "default"
fast_link_ratio = 50
bibi_ratio = 0
import_ratio = 0
active_list_limit = 20
url_delay_s = 10

daily_limit_gb = 0
speed_limit_mbps = 0
province_filter = "all"   # all / same / other
isp_filter = "all"        # all / dianxin / liantong / yidong / jiaoyu

schedule_segments = [
    {"time_range": "", "threads": 0, "limit_gb_min": 0, "limit_gb_max": 0, "speed_limit": 0},
    {"time_range": "", "threads": 0, "limit_gb_min": 0, "limit_gb_max": 0, "speed_limit": 0},
    {"time_range": "", "threads": 0, "limit_gb_min": 0, "limit_gb_max": 0, "speed_limit": 0},
]

total_download_bytes = 0
current_speed_bps = 0
start_time = None
active_threads = 0
active_connections = LimitedDict(maxlen=500)

# ====================== 本机IP信息（启动时自动检测）======================
local_province = "未知"
local_isp = "未知"
local_ip = "未知"

def detect_local_ip_info():
    """启动时检测本机公网IP的省份和运营商"""
    global local_province, local_isp, local_ip
    apis = [
        "https://ip.useragentinfo.com/json",
        "http://ip-api.com/json/?lang=zh-CN",
    ]
    for api in apis:
        try:
            resp = requests.get(api, headers=OPTIMIZED_HEADERS, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if "ip" in data:
                    local_ip = data.get("ip", "未知")
                    local_province = data.get("province", data.get("regionName", "未知"))
                    local_isp = normalize_isp(data.get("isp", data.get("org", "未知")))
                elif data.get("status") == "success":
                    local_ip = data.get("query", "未知")
                    local_province = data.get("regionName", "未知")
                    local_isp = normalize_isp(data.get("isp", "未知"))
                print(f"[检测] 本机IP: {local_ip} | 省份: {local_province} | 运营商: {local_isp}")
                return
        except Exception: pass
    print("[检测] 无法检测本机IP信息")

def normalize_isp(isp_str):
    """标准化运营商名称"""
    s = isp_str.lower()
    if "电信" in s or "telecom" in s or "chinanet" in s: return "电信"
    if "联通" in s or "unicom" in s: return "联通"
    if "移动" in s or "mobile" in s or "cmnet" in s: return "移动"
    if "教育" in s or "edu" in s or "cernet" in s: return "教育网"
    return "其他"

# ====================== 全国各省CDN/镜像站数据库 ======================
# 格式: (url, 省份, 运营商类型, 速度权重)
PROVINCIAL_MIRRORS = {
    # ===== 北京 =====
    "北京": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 600),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 500),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 100),
    ],
    # ===== 上海 =====
    "上海": [
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.sjtug.sjtu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
    ],
    # ===== 江苏（南京）=====
    "江苏": [
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 400),
        ("https://mirrors6.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
    ],
    # ===== 安徽（合肥）=====
    "安徽": [
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
    ],
    # ===== 浙江（杭州）=====
    "浙江": [
        ("https://mirrors.zju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
        ("https://mirrors.zju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
    ],
    # ===== 广东（广州/深圳）=====
    "广东": [
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 500),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 100),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 100),
    ],
    # ===== 重庆 =====
    "重庆": [
        ("https://mirrors.cqu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 100),
    ],
    # ===== 四川（成都）=====
    "四川": [
        ("https://mirrors.njupt.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
        ("https://mirror.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
    ],
    # ===== 辽宁（大连）=====
    "辽宁": [
        ("https://mirrors.dlut.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 100),
        ("https://mirror.neu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
    ],
    # ===== 黑龙江（哈尔滨）=====
    "黑龙江": [
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
    ],
    # ===== 陕西（西安）=====
    "陕西": [
        ("https://mirrors.xjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
    ],
    # ===== 湖北（武汉）=====
    "湖北": [
        ("https://mirrors.hust.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 300),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 150),
    ],
    # ===== 山东 =====
    "山东": [
        ("https://mirrors.qdu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
        ("https://mirrors.sdwu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 150),
    ],
    # ===== 福建（厦门）=====
    "福建": [
        ("https://mirrors.xmu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
    ],
    # ===== 甘肃（兰州）=====
    "甘肃": [
        ("https://mirror.lzu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
    ],
    # ===== 四川（电子科大）=====
    "贵州": [
        ("https://mirrors.uestc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 200),
    ],
    # ===== 全国通用（不限省份）=====
    "全国": [
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 50),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 50),
    ],
}

# 所有省份列表
ALL_PROVINCES = list(PROVINCIAL_MIRRORS.keys())

# ====================== 链接池 ======================
speed_url_list = []
video_url_list = []
live_url_list = []
crawled_urls = []

# ====================== 视频CDN链接 ======================
# 各大视频平台CDN域名（浏览器F12抓包获取）
VIDEO_CDN_SOURCES = [
    # B站视频CDN
    "https://upos-sz-mirrorcdn.bilivideo.com/",
    "https://cn-hbxy-cmcc-live-01.bilivideo.com/",
    "https://upos-sz-mirrorcos.bilivideo.com/",
    # 优酷CDN
    "https://pl-ali.youku.com/",
    "https://vali-mcp.vip.youku.com/",
    # 腾讯视频CDN
    "https://vd.l.qq.com/",
    "https://video.dispatch.tc.qq.com/",
    # 爱奇艺CDN
    "https://cache.m.iqiyi.com/",
    "https://vip.video.iqiyi.com/",
    # 大文件下载（游戏/软件）
    "https://cdn.akamai.steamstatic.com/",
    "https://cdn1.epicgames.com/",
    "https://autopatchcn.yuanshen.com/",
    "https://software.download.prss.microsoft.com/",
    # 国内CDN
    "https://dldir1.qq.com/",
    "https://dldir1v6.qq.com/",
    "https://dlsw.baidu.com/",
]

# ====================== 直播源（m3u8）======================
# IPTV直播源（央视+卫视+地方台）
LIVE_STREAM_SOURCES = [
    # 央视直播
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221226016/index.m3u8",  # CCTV-1
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225588/index.m3u8",  # CCTV-2
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221227166/index.m3u8",  # CCTV-5
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225548/index.m3u8",  # CCTV-6
    # 卫视直播
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225800/index.m3u8",  # 湖南卫视
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225802/index.m3u8",  # 浙江卫视
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225804/index.m3u8",  # 江苏卫视
    "http://ottrrs.hl.chinamobile.com/PLTV/88888888/224/3221225806/index.m3u8",  # 东方卫视
    # IPTV公开源
    "https://live.kilvn.com/iptv.m3u",
]
def generate_speed_urls(count=5000):
    """从全国各省镜像+视频CDN生成链接池"""
    urls = []
    for province, mirrors in PROVINCIAL_MIRRORS.items():
        for url, isp, weight in mirrors:
            n = max(1, weight // 10)
            for _ in range(n):
                urls.append(f"{url}?t={random.randint(1000000, 9999999)}")
    for u in crawled_urls:
        for _ in range(3):
            urls.append(f"{u}?t={random.randint(1000000, 9999999)}")
    # 补充视频CDN链接
    for base in VIDEO_CDN_SOURCES:
        for _ in range(10):
            urls.append(f"{base}test?t={random.randint(1000000, 9999999)}")
    random.shuffle(urls)
    return urls[:count]

def generate_video_urls(count=2000):
    """视频链接：B站/优酷/腾讯/爱奇艺CDN + 大文件"""
    urls = []
    video_sources = VIDEO_CDN_SOURCES.copy()
    for province, mirrors in PROVINCIAL_MIRRORS.items():
        for url, isp, weight in mirrors:
            if ".iso" in url:
                video_sources.append(url)
    per_src = max(1, count // len(video_sources))
    for u in video_sources:
        for _ in range(min(per_src, 5)):
            urls.append(f"{u}?t={random.randint(1000000, 9999999)}")
    random.shuffle(urls)
    return urls[:count]

def generate_live_urls(count=500):
    """直播链接：IPTV直播源 + 各省镜像"""
    urls = []
    for u in LIVE_STREAM_SOURCES:
        for _ in range(20):
            urls.append(f"{u}?t={random.randint(1000000, 9999999)}")
    sources = []
    for province, mirrors in PROVINCIAL_MIRRORS.items():
        for url, isp, weight in mirrors:
            if "Packages" in url or "Release" in url:
                sources.append(url)
    per_src = max(1, (count - len(urls)) // max(1, len(sources)))
    for u in sources:
        for _ in range(min(per_src, 3)):
            urls.append(f"{u}?t={random.randint(1000000, 9999999)}")
    random.shuffle(urls)
    return urls[:count]

# ====================== 初始化 ======================
print("[初始化] 生成链接池...")
speed_url_list = generate_speed_urls(5000)
video_url_list = generate_video_urls(2000)
live_url_list = generate_live_urls(500)
total = len(speed_url_list) + len(video_url_list) + len(live_url_list)
print(f"[初始化] 完成: 测速{len(speed_url_list)} | 视频{len(video_url_list)} | 直播{len(live_url_list)} | 总计{total}")

url_fail_count = LimitedDict(maxlen=10000)
url_daily_traffic = LimitedDict(maxlen=50000)
url_session_traffic = LimitedDict(maxlen=50000)
blacklist = set()

executor = None
stats_timer = None
cleanup_timer = None
crawl_timer = None

# ====================== 工具函数 ======================
def get_current_thread_count():
    hour = datetime.now().hour
    if night_start_hour <= hour < day_start_hour:
        return night_thread
    return base_thread

def format_bytes(bytes_val):
    if bytes_val < 1024: return f"{bytes_val} B"
    elif bytes_val < 1024**2: return f"{bytes_val/1024:.2f} KB"
    elif bytes_val < 1024**3: return f"{bytes_val/1024**2:.2f} MB"
    else: return f"{bytes_val/1024**3:.2f} GB"

def format_speed(bps):
    if bps < 1024: return f"{bps:.0f} bps"
    elif bps < 1024**2: return f"{bps/1024:.2f} Kbps"
    elif bps < 1024**3: return f"{bps/1024**2:.2f} Mbps"
    else: return f"{bps/1024**3:.2f} Gbps"

def format_duration(seconds):
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0: return f"{hours}时{minutes:02d}分{secs:02d}秒"
    elif minutes > 0: return f"{minutes}分{secs:02d}秒"
    return f"{secs}秒"

def extract_host_from_url(url):
    try: return urlparse(url).hostname or url[:50]
    except Exception: return url[:50]

# ====================== IP归属地查询（带缓存）======================
ip_cache = LimitedDict(maxlen=5000)

def get_ip_location(host):
    """查询IP/域名的省份和运营商"""
    # 先查镜像站映射
    for domain, info in PROVINCE_ISP_MAP.items():
        if domain in host or host.endswith("." + domain):
            return info
    # 查缓存
    if host in ip_cache:
        return ip_cache[host]
    # 尝试DNS解析后查API
    try:
        ip = socket.gethostbyname(host)
        if ip in ip_cache:
            return ip_cache[ip]
        # 调用免费API
        try:
            resp = requests.get(f"https://ip.useragentinfo.com/json?ip={ip}", headers=OPTIMIZED_HEADERS, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                province = data.get("province", "国内")
                isp = normalize_isp(data.get("isp", "其他"))
                result = (province, isp)
                ip_cache[ip] = result
                ip_cache[host] = result
                return result
        except Exception: pass
        try:
            resp = requests.get(f"http://ip-api.com/json/{ip}?lang=zh-CN", headers=OPTIMIZED_HEADERS, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    province = data.get("regionName", "国内")
                    isp = normalize_isp(data.get("isp", "其他"))
                    result = (province, isp)
                    ip_cache[ip] = result
                    ip_cache[host] = result
                    return result
        except Exception: pass
    except Exception: pass
    return ("国内", "其他")

# 镜像站域名 → (省份, 运营商) 快速映射
PROVINCE_ISP_MAP = {
    "mirrors.tuna.tsinghua.edu.cn": ("北京", "教育网"),
    "mirrors6.tuna.tsinghua.edu.cn": ("北京", "教育网"),
    "mirrors.aliyun.com": ("杭州", "电信"),
    "mirrors.huaweicloud.com": ("东莞", "联通"),
    "repo.huaweicloud.com": ("东莞", "联通"),
    "mirrors.cloud.tencent.com": ("广州", "联通"),
    "mirrors.nju.edu.cn": ("南京", "教育网"),
    "mirrors6.nju.edu.cn": ("南京", "教育网"),
    "mirrors.ustc.edu.cn": ("合肥", "教育网"),
    "mirrors6.ustc.edu.cn": ("合肥", "教育网"),
    "mirrors.sjtug.sjtu.edu.cn": ("上海", "教育网"),
    "mirrors6.sjtug.sjtu.edu.cn": ("上海", "教育网"),
    "mirrors.hit.edu.cn": ("哈尔滨", "教育网"),
    "mirrors.dlut.edu.cn": ("大连", "教育网"),
    "mirrors.cqu.edu.cn": ("重庆", "教育网"),
    "mirrors.zju.edu.cn": ("杭州", "教育网"),
    "mirrors.hust.edu.cn": ("武汉", "教育网"),
    "mirrors.xjtu.edu.cn": ("西安", "教育网"),
    "mirrors.njupt.edu.cn": ("南京", "教育网"),
    "mirror.neu.edu.cn": ("沈阳", "教育网"),
    "mirrors.qdu.edu.cn": ("青岛", "教育网"),
    "mirrors.sdwu.edu.cn": ("济南", "教育网"),
    "mirrors.xmu.edu.cn": ("厦门", "教育网"),
    "mirror.lzu.edu.cn": ("兰州", "教育网"),
    "mirrors.uestc.edu.cn": ("成都", "教育网"),
    "dlsw.baidu.com": ("北京", "电信"),
    "gdown.baidu.com": ("北京", "电信"),
    "dldir1.qq.com": ("深圳", "联通"),
    "dldir1v6.qq.com": ("深圳", "联通"),
    "registry.npmmirror.com": ("杭州", "联通"),
    "cdn.mysql.com": ("全球", "电信"),
    "download.visualstudio.microsoft.com": ("全球", "电信"),
    "releases.ubuntu.com": ("全球", "电信"),
}

def get_province_for_url(url):
    """获取URL对应的省份"""
    host = extract_host_from_url(url)
    for domain, (province, _) in PROVINCE_ISP_MAP.items():
        if domain in host or host.endswith("." + domain):
            return province
    return "全国"

def get_isp_for_url(url):
    """获取URL对应的运营商"""
    host = extract_host_from_url(url)
    for domain, (_, isp) in PROVINCE_ISP_MAP.items():
        if domain in host or host.endswith("." + domain):
            return isp
    return "其他"

# ====================== 后台任务 ======================
def stats_updater():
    global current_speed_bps, total_download_bytes
    last_total = 0
    while running:
        current_total = total_download_bytes
        current_speed_bps = (current_total - last_total) * 8
        last_total = current_total
        time.sleep(1)

def daily_cleanup():
    global url_daily_traffic, blacklist
    while True:
        now = datetime.now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time.sleep((next_midnight - now).total_seconds())
        url_daily_traffic.clear()
        blacklist.clear()
        url_fail_count.clear()
        print("[清理] 每日统计已清空")

def thread_pool_manager():
    global active_threads, executor
    while running:
        target_threads = get_current_thread_count()
        if active_threads != target_threads:
            if executor: executor.shutdown(wait=False)
            executor = ThreadPoolExecutor(max_workers=target_threads)
            active_threads = target_threads
            for _ in range(target_threads): executor.submit(download_worker)
            print(f"[线程] 调整为 {target_threads} 个线程")
        time.sleep(60)

# ====================== 核心下载循环 ======================
def pick_url_distributed():
    """根据省份/运营商设置，智能选择URL"""
    total_w = weight_speed + weight_video + weight_live
    rand_val = random.randint(1, total_w)
    if rand_val <= weight_speed:
        pool = speed_url_list
    elif rand_val <= weight_speed + weight_video:
        pool = video_url_list
    else:
        pool = live_url_list

    # 省份过滤
    for _ in range(30):
        u = random.choice(pool)
        if u in blacklist: continue
        if url_session_traffic.get(u, -1) == 0: continue

        # 省份筛选
        if province_filter != "all":
            url_province = get_province_for_url(u)
            if province_filter == "same" and url_province != local_province and url_province != "全国":
                continue
            if province_filter == "other" and url_province == local_province:
                continue

        # 运营商筛选
        if isp_filter != "all":
            url_isp = get_isp_for_url(u)
            if isp_filter == "dianxin" and url_isp != "电信": continue
            if isp_filter == "liantong" and url_isp != "联通": continue
            if isp_filter == "yidong" and url_isp != "移动": continue
            if isp_filter == "jiaoyu" and url_isp != "教育网": continue

        return u

    # 放宽条件
    for _ in range(10):
        u = random.choice(pool)
        if u not in blacklist:
            return u
    return None

def download_worker():
    global total_download_bytes
    tid = threading.current_thread().name
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=1)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    while running:
        target_url = pick_url_distributed()
        if not target_url:
            time.sleep(0.5); continue

        conn_start = datetime.now(); conn_bytes = 0
        host_display = extract_host_from_url(target_url)
        ip_loc, ip_cat = get_ip_location(host_display)
        try:
            ip_addr = socket.gethostbyname(host_display)
            host_display = ip_addr
        except Exception: pass

        now_ts = time.time()
        active_connections[tid] = {
            "url": target_url, "start_time": conn_start.strftime("%H:%M:%S"),
            "bytes": 0, "host": host_display, "location": ip_loc, "category": ip_cat,
            "speed_bps": 0, "last_bytes": 0, "speed_ts": now_ts
        }

        try:
            resp = session.get(target_url, stream=True, timeout=timeout_s, headers=OPTIMIZED_HEADERS)
            resp.raise_for_status()
            last_recv_time = time.time(); got_data = False
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not running: active_connections.pop(tid, None); return
                if chunk:
                    got_data = True; chunk_len = len(chunk)
                    conn_bytes += chunk_len; total_download_bytes += chunk_len
                    url_session_traffic[target_url] = url_session_traffic.get(target_url, 0) + chunk_len
                    url_daily_traffic[target_url] = url_daily_traffic.get(target_url, 0) + chunk_len
                    active_connections[tid]["bytes"] = conn_bytes
                    _now = time.time()
                    _dt = _now - active_connections[tid].get("speed_ts", _now)
                    if _dt >= 1.0:
                        _delta = conn_bytes - active_connections[tid].get("last_bytes", 0)
                        active_connections[tid]["speed_bps"] = _delta * 8 / _dt
                        active_connections[tid]["last_bytes"] = conn_bytes
                        active_connections[tid]["speed_ts"] = _now
                    last_recv_time = _now
                elif got_data:
                    if time.time() - last_recv_time >= stall_timeout_s: break
            url_fail_count[target_url] = 0
        except Exception:
            cnt = url_fail_count.get(target_url, 0) + 1
            url_fail_count[target_url] = cnt
            if cnt >= max_fail_times:
                blacklist.add(target_url)
        finally:
            active_connections.pop(tid, None)
            time.sleep(req_delay_ms / 1000)

# ====================== 自动清理 ======================
def auto_cleanup_dead_links():
    """每5分钟：剔除无流量链接，随机补充新链接"""
    global speed_url_list, video_url_list, live_url_list
    while True:
        time.sleep(300)
        if not running: continue
        before = len(speed_url_list) + len(video_url_list) + len(live_url_list)

        speed_url_list[:] = [u for u in speed_url_list if u not in blacklist]
        video_url_list[:] = [u for u in video_url_list if u not in blacklist]
        live_url_list[:] = [u for u in live_url_list if u not in blacklist]

        dead_urls = set()
        for u in list(url_session_traffic.keys()):
            if url_session_traffic[u] == 0 and u not in blacklist:
                dead_urls.add(u)
        if dead_urls:
            speed_url_list[:] = [u for u in speed_url_list if u not in dead_urls]
            video_url_list[:] = [u for u in video_url_list if u not in dead_urls]
            live_url_list[:] = [u for u in live_url_list if u not in dead_urls]
            for u in dead_urls:
                url_session_traffic.pop(u, None)
                url_fail_count.pop(u, None)
            print(f"[清理] 剔除 {len(dead_urls)} 个无流量链接")

        after = len(speed_url_list) + len(video_url_list) + len(live_url_list)
        removed = before - after
        if removed > 0:
            print(f"[清理] 总移除 {removed} 个，剩余 {after}")

        if after < 4000:
            need_s = max(0, 3300 - len(speed_url_list))
            if need_s > 0: speed_url_list.extend(generate_speed_urls(need_s))
            need_v = max(0, 2000 - len(video_url_list))
            if need_v > 0: video_url_list.extend(generate_video_urls(need_v))
            total = len(speed_url_list) + len(video_url_list) + len(live_url_list)
            print(f"[清理] 随机补充完成: 总计{total}条")

        if len(url_fail_count) > 5000: url_fail_count.clear()
        if len(url_session_traffic) > 30000:
            kept = {k: v for k, v in url_session_traffic.items() if v > 0}
            url_session_traffic.clear()
            url_session_traffic.update(kept)

# ====================== 爬虫（全国各省）======================
def auto_crawl_and_update():
    """爬虫：自动爬取全国各省镜像站目录"""
    global speed_url_list, video_url_list, live_url_list, crawled_urls
    while True:
        time.sleep(7200)
        print("[爬虫] 开始抓取全国各省镜像...")
        new_urls = []
        crawl_bases = []
        # 收集所有省份的镜像站目录
        for province, mirrors in PROVINCIAL_MIRRORS.items():
            seen_hosts = set()
            for url, isp, _ in mirrors:
                host = extract_host_from_url(url)
                if host in seen_hosts: continue
                seen_hosts.add(host)
                # 构造目录URL
                p = urlparse(url)
                dir_path = "/".join(p.path.split("/")[:-1]) + "/"
                crawl_bases.append(f"{p.scheme}://{p.netloc}{dir_path}")
        # 爬取
        for base in crawl_bases[:30]:  # 限制30个避免太慢
            try:
                resp = requests.get(base, headers=OPTIMIZED_HEADERS, timeout=8)
                if resp.status_code == 200:
                    found = re.findall(r'href="([^"]+\.(deb|iso|xz|gz|tar|zip|exe)[^"]*)"', resp.text, re.I)
                    for match in found[:30]:
                        link = match[0]
                        if link.startswith("/"):
                            p = urlparse(base)
                            link = f"{p.scheme}://{p.netloc}{link}"
                        elif not link.startswith("http"):
                            link = base.rstrip("/") + "/" + link
                        if link not in new_urls:
                            new_urls.append(link)
            except Exception: pass
        if new_urls:
            crawled_urls = new_urls[:1000]
            speed_url_list = generate_speed_urls(5000)
            video_url_list = generate_video_urls(2000)
            live_url_list = generate_live_urls(500)
            print(f"[爬虫] 更新完成: 爬取{len(new_urls)}条，池总计{len(speed_url_list)+len(video_url_list)+len(live_url_list)}条")

# ====================== 省份/运营商统计 ======================
def get_province_stats():
    """统计正在运行的连接按省份分布"""
    stats = defaultdict(lambda: {"count": 0, "bytes": 0, "speed": 0})
    for tid, info in list(active_connections.items()):
        loc = info.get("location", "未知")
        stats[loc]["count"] += 1
        stats[loc]["bytes"] += info.get("bytes", 0)
        stats[loc]["speed"] += info.get("speed_bps", 0)
    return dict(stats)

def get_isp_stats():
    """统计正在运行的连接按运营商分布"""
    stats = defaultdict(lambda: {"count": 0, "bytes": 0, "speed": 0})
    for tid, info in list(active_connections.items()):
        cat = info.get("category", "其他")
        stats[cat]["count"] += 1
        stats[cat]["bytes"] += info.get("bytes", 0)
        stats[cat]["speed"] += info.get("speed_bps", 0)
    return dict(stats)

# ====================== Web面板 ======================
html_tpl = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>流量冲刷器 v4.0</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;font-size:14px;background:#f5f6fa;display:flex;height:100vh;overflow:hidden}
.sidebar{width:220px;background:#2c3e50;color:#ecf0f1;flex-shrink:0;display:flex;flex-direction:column;height:100vh;overflow-y:auto}
.sidebar-header{padding:18px 16px 14px;border-bottom:1px solid rgba(255,255,255,.08);font-size:16px;font-weight:600}
.sidebar-nav{flex:1;padding:8px 0}
.nav-item{display:flex;align-items:center;padding:11px 18px;color:#bdc3c7;cursor:pointer;border-left:3px solid transparent;font-size:13.5px}
.nav-item:hover{background:rgba(255,255,255,.06);color:#fff}
.nav-item.active{background:rgba(52,152,219,.15);color:#3498db;border-left-color:#3498db;font-weight:500}
.nav-item .nav-icon{width:22px;margin-right:10px;text-align:center;font-size:15px}
.sidebar-footer{padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);font-size:11px;color:#7f8c8d}
.main{flex:1;overflow-y:auto;background:#f5f6fa}
.page{display:none;padding:24px 32px;max-width:1200px}.page.active{display:block}
.page-title{font-size:20px;font-weight:600;color:#2c3e50;margin-bottom:16px;padding-bottom:10px;border-bottom:2px solid #3498db;display:inline-block}
.card{background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.card-title{font-size:15px;font-weight:600;color:#2c3e50;margin-bottom:14px;display:flex;align-items:center}
.card-title .dot{width:8px;height:8px;border-radius:50%;margin-right:8px}
.dot-green{background:#27ae60}.dot-blue{background:#3498db}
.status-bar{background:#fffaf0;border-left:4px solid #f39c12;padding:14px 18px;margin-bottom:16px;border-radius:0 6px 6px 0;font-size:13.5px;color:#666}
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:16px}
.stat-card{background:#fff;border-radius:8px;padding:18px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.stat-value{font-size:26px;font-weight:700;color:#2c3e50;line-height:1.3}
.stat-label{font-size:12px;color:#95a5a6;margin-top:4px}
.stat-value.running{color:#27ae60}.stat-value.stopped{color:#e74c3c}
.data-table{width:100%;border-collapse:collapse;font-size:13px}
.data-table th{background:#f8f9fa;padding:10px 12px;text-align:left;font-weight:600;color:#555;border-bottom:2px solid #e8e8e8}
.data-table td{padding:9px 12px;border-bottom:1px solid #f0f0f0}
.data-table tr:hover{background:#fafbfd}
.form-section{background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.form-label{display:block;font-size:13.5px;color:#333;margin-bottom:6px;font-weight:500}
.form-input{width:100%;padding:9px 12px;border:1px solid #ddd;border-radius:4px;font-size:14px}
.form-input:focus{outline:none;border-color:#3498db}
.radio-group{display:flex;flex-wrap:wrap;gap:18px;margin-top:4px}
.radio-group label{display:flex;align-items:center;gap:5px;cursor:pointer;font-size:13.5px;color:#444}
.radio-group input[type=radio]{accent-color:#3498db;width:16px;height:16px}
.collapsible{cursor:pointer;display:flex;justify-content:space-between;align-items:center;padding:12px 0;font-size:14.5px;font-weight:600;color:#333;border-bottom:1px solid #eee}
.collapsible::after{content:'▶';font-size:11px;color:#999;transition:transform .2s}
.collapsible.open::after{transform:rotate(90deg)}
.collapse-content{display:none;padding:14px 0;border-bottom:1px solid #eee}
.collapse-content.show{display:block}
.btn{display:inline-block;padding:10px 28px;border:none;border-radius:4px;cursor:pointer;font-size:14px;font-weight:500;text-decoration:none;text-align:center}
.btn-primary{background:#3498db;color:#fff}.btn-primary:hover{background:#2980b9}
.btn-success{background:#27ae60;color:#fff}.btn-success:hover{background:#219a52}
.btn-danger{background:#e74c3c;color:#fff}.btn-danger:hover{background:#c0392b}
.btn-block{display:block;width:100%}.btn-lg{padding:12px 32px;font-size:15px}
.btn-group{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
.alert{padding:12px 16px;border-radius:6px;margin-bottom:14px;font-size:13px}
.alert-warn{background:#fef9e7;border-left:4px solid #f39c12;color:#d68910}
.info-row{display:flex;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0;font-size:13px}
.info-row:last-child{border-bottom:none}
.info-label{color:#7f8c8d;width:160px;flex-shrink:0}
.info-value{color:#2c3e50;font-weight:500}
.link-stats{background:#fafbfd;padding:14px 18px;border-radius:6px;margin-bottom:16px;font-size:13px;line-height:2;color:#555}
.link-stats b{color:#2c3e50}
.notes{background:#f8f9fa;padding:14px 18px;border-radius:6px;margin-top:16px;font-size:12.5px;color:#777;line-height:1.9}
.notes ol{margin:6px 0 0 18px}
.setting-field{margin-bottom:16px}
.setting-label{font-size:13.5px;color:#2980b9;font-weight:600;margin-bottom:6px;display:block;border-left:3px solid #3498db;padding-left:8px}
.setting-input{width:100%;padding:9px 12px;border:1px solid #ddd;border-radius:4px;font-size:14px}
.setting-input:focus{outline:none;border-color:#3498db}
.segment-card{background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.segment-header{font-size:15px;font-weight:600;color:#3498db;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #ecf0f1}
.segment-row{display:flex;align-items:center;margin-bottom:12px;gap:10px}
.segment-row label{min-width:70px;font-size:13.5px;color:#555;font-weight:500}
.segment-row input{flex:1;padding:8px 10px;border:1px solid #ddd;border-radius:4px;font-size:13.5px}
.segment-row .input-sm{max-width:120px}.segment-row .input-range{max-width:140px}
.input-suffix{font-size:13px;color:#888;margin-left:4px}
.desc-area{background:#fafbfd;padding:14px 18px;border-radius:6px;margin-bottom:16px;font-size:12.5px;color:#666;line-height:1.8}
.desc-area ol{margin:4px 0 0 18px}
.isp-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
.isp-card{background:#fff;border-radius:8px;padding:14px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.06);border-left:4px solid #3498db}
.isp-card.telecom{border-left-color:#e74c3c}.isp-card.unicom{border-left-color:#3498db}
.isp-card.mobile{border-left-color:#27ae60}.isp-card.edu{border-left-color:#f39c12}
.isp-count{font-size:22px;font-weight:700;color:#2c3e50}
.isp-label{font-size:11px;color:#95a5a6;margin-top:2px}
.province-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:8px;margin-top:10px}
.province-tag{background:#f0f4ff;border:1px solid #d4deff;border-radius:6px;padding:8px 10px;text-align:center;font-size:12px}
.province-tag .p-name{font-weight:600;color:#2c3e50}.province-tag .p-count{color:#3498db;font-size:11px}
@media(max-width:768px){.sidebar{width:180px}.stats-grid,.isp-grid{grid-template-columns:repeat(2,1fr)}.page{padding:16px}}
</style></head><body>
<div class="sidebar">
  <div class="sidebar-header">🌊 流量冲刷器 v4.0</div>
  <nav class="sidebar-nav">
    <div class="nav-item active" data-page="dashboard"><span class="nav-icon">📊</span>每日任务</div>
    <div class="nav-item" data-page="schedule"><span class="nav-icon">⏰</span>分时设置</div>
    <div class="nav-item" data-page="active-list"><span class="nav-icon">🔄</span>正在运行列表</div>
    <div class="nav-item" data-page="custom-urls"><span class="nav-icon">🔗</span>自定义链接</div>
    <div class="nav-item" data-page="settings"><span class="nav-icon">⚙️</span>设置</div>
    <div class="nav-item" data-page="about"><span class="nav-icon">ℹ️</span>版本说明</div>
  </nav>
  <div class="sidebar-footer">本机: {{ local_ip }} ({{ local_province }}/{{ local_isp }})</div>
</div>
<div class="main">
  <div class="page active" id="page-dashboard">
    <div style="text-align:center;margin-bottom:18px"><div class="page-title" style="border:none;padding:0">每日任务</div></div>
    <div class="stats-grid">
      <div class="stat-card"><div class="stat-value {{ 'running' if running else 'stopped' }}" id="dash-speed">{{ current_speed }}</div><div class="stat-label">实时速率</div></div>
      <div class="stat-card"><div class="stat-value" id="dash-total">{{ total_download }}</div><div class="stat-label">累计消耗</div></div>
      <div class="stat-card"><div class="stat-value" id="dash-threads">{{ active_threads }}</div><div class="stat-label">活跃线程</div></div>
      <div class="stat-card"><div class="stat-value" id="dash-duration">{{ run_duration }}</div><div class="stat-label">运行时长</div></div>
    </div>
    <div class="status-bar">🔔 当前状态：<strong id="sb-status">{{ "正在冲刷" if running else "已停止" }}</strong> | 本机: {{ local_ip }} ({{ local_province }}/{{ local_isp }})</div>

    <!-- 运营商统计 -->
    <div class="card">
      <div class="card-title"><span class="dot dot-blue"></span>运营商分布</div>
      <div class="isp-grid">
        <div class="isp-card telecom"><div class="isp-count" id="isp-telecom">0</div><div class="isp-label">电信</div></div>
        <div class="isp-card unicom"><div class="isp-count" id="isp-unicom">0</div><div class="isp-label">联通</div></div>
        <div class="isp-card mobile"><div class="isp-count" id="isp-mobile">0</div><div class="isp-label">移动</div></div>
        <div class="isp-card edu"><div class="isp-count" id="isp-edu">0</div><div class="isp-label">教育网</div></div>
      </div>
    </div>

    <!-- 省份统计 -->
    <div class="card">
      <div class="card-title"><span class="dot dot-green"></span>地区分布</div>
      <div class="province-grid" id="province-grid">
        <div class="province-tag"><div class="p-name">加载中...</div></div>
      </div>
    </div>

    <!-- 链接池信息 -->
    <div class="card">
      <div class="card-title"><span class="dot dot-blue"></span>链接池信息</div>
      <div class="link-stats">
        链接总数：<b id="link-total">{{ all_len }}</b> &nbsp;
        测速链接：<b id="link-speed">{{ len_speed }}</b> 条 &nbsp;
        视频链接：<b id="link-video">{{ len_video }}</b> 条 &nbsp;
        直播链接：<b id="link-live">{{ len_live }}</b> 条 &nbsp;
        黑名单：<b id="link-blacklist" style="color:#e74c3c">{{ blacklist_count }}</b> 条
      </div>
    </div>

    <form method="POST" action="/setconfig">
      <div class="form-section">
        <label class="form-label">线程数</label>
        <input type="number" name="base_thread" value="{{ base_thread }}" min="1" max="2000" class="form-input" style="max-width:300px">
      </div>
      <div class="form-section">
        <label class="form-label">每日消耗流量（GB，0为无限）</label>
        <input type="number" name="daily_limit" value="{{ daily_limit_gb }}" min="0" step="0.1" class="form-input" style="max-width:300px">
      </div>
      <div class="form-section">
        <label class="form-label">限速（MB/s，0为不限制）</label>
        <input type="number" name="speed_limit" value="{{ speed_limit_mbps }}" min="0" step="0.1" class="form-input" style="max-width:300px">
      </div>
      <div class="form-section">
        <label class="form-label">省份筛选</label>
        <div class="radio-group">
          <label><input type="radio" name="province" value="all" {{ 'checked' if province_filter=='all' }}> 所有</label>
          <label><input type="radio" name="province" value="same" {{ 'checked' if province_filter=='same' }}> 同省（{{ local_province }}）</label>
          <label><input type="radio" name="province" value="other" {{ 'checked' if province_filter=='other' }}> 省外</label>
        </div>
      </div>
      <div class="form-section">
        <label class="form-label">运营商筛选</label>
        <div class="radio-group">
          <label><input type="radio" name="isp" value="all" {{ 'checked' if isp_filter=='all' }}> 所有</label>
          <label><input type="radio" name="isp" value="dianxin" {{ 'checked' if isp_filter=='dianxin' }}> 电信</label>
          <label><input type="radio" name="isp" value="liantong" {{ 'checked' if isp_filter=='liantong' }}> 联通</label>
          <label><input type="radio" name="isp" value="yidong" {{ 'checked' if isp_filter=='yidong' }}> 移动</label>
          <label><input type="radio" name="isp" value="jiaoyu" {{ 'checked' if isp_filter=='jiaoyu' }}> 教育网</label>
        </div>
      </div>
      <div class="form-section">
        <div class="collapsible" onclick="this.classList.toggle('open');this.nextElementSibling.classList.toggle('show')">高级设置</div>
        <div class="collapse-content">
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:8px">
            <div><label class="form-label" style="font-size:12.5px;color:#666">夜间线程数</label><input type="number" name="night_thread" value="{{ night_thread }}" min="1" max="2000" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">夜间开始(时)</label><input type="number" name="day_start" value="{{ day_start_hour }}" min="0" max="23" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">测速占比(%)</label><input type="number" name="ws" value="{{ weight_speed }}" min="0" max="100" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">视频占比(%)</label><input type="number" name="wv" value="{{ weight_video }}" min="0" max="100" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">直播占比(%)</label><input type="number" name="wl" value="{{ weight_live }}" min="0" max="100" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">超时(秒)</label><input type="number" name="timeout_s" value="{{ timeout_s }}" min="3" max="60" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">无流量超时(秒)</label><input type="number" name="stall_timeout" value="{{ stall_timeout_s }}" min="3" max="60" class="form-input"></div>
            <div><label class="form-label" style="font-size:12.5px;color:#666">请求间隔(ms)</label><input type="number" name="req_delay" value="{{ req_delay_ms }}" min="0" max="5000" class="form-input"></div>
          </div>
        </div>
      </div>
      {% if not running %}
      <a href="/start" class="btn btn-success btn-block btn-lg" style="margin-bottom:12px;font-size:16px">▶ 开始冲刷</a>
      {% else %}
      <div class="btn-group" style="margin-bottom:12px">
        <a href="/stop" class="btn btn-danger btn-lg" style="flex:1;font-size:15px">⏹ 暂停冲刷</a>
        <a href="/resetstats" class="btn btn-primary btn-lg" style="flex:1;font-size:15px">🔄 重置统计</a>
      </div>
      {% endif %}
      <button type="submit" class="btn btn-primary btn-block btn-lg" style="margin-bottom:12px">提交</button>
      <a href="/stopandclear" class="btn btn-danger btn-block" onclick="return confirm('确定停止并清空？')">暂停并清空配置</a>
    </form>
    <div class="notes"><strong>注意事项：</strong><ol>
      <li>链接池覆盖全国{{ all_province_count }}个省市地区CDN/镜像源</li>
      <li>自动检测本机省份和运营商，支持同省/外省/运营商筛选</li>
      <li>自动清理无流量链接，每5分钟检测并随机补充</li>
      <li>爬虫每2小时自动更新全国各省镜像站链接</li>
    </ol></div>
  </div>

  <div class="page" id="page-schedule">
    <div style="text-align:center;margin-bottom:18px"><div class="page-title" style="border:none;padding:0">分时设置</div></div>
    <div class="desc-area"><strong>说明</strong><ol>
      <li>时间段：空为不运行</li><li>线程数：0为不运行</li><li>消耗：0为无限</li><li>限速：0为不限制</li>
    </ol></div>
    <form method="POST" action="/setschedule">
      {% for i in range(3) %}
      <div class="segment-card">
        <div class="segment-header">📍 第{{ i+1 }}段</div>
        <div class="segment-row"><label>时间段</label><input type="text" name="seg{{i}}_time" value="{{ schedule[i].time_range }}" placeholder="如 07:00-23:00" class="form-input"></div>
        <div class="segment-row"><label>线程数</label><input type="number" name="seg{{i}}_threads" value="{{ schedule[i].threads }}" min="0" max="2000" class="form-input input-sm"></div>
        <div class="segment-row"><label>消耗(GB)</label><input type="number" name="seg{{i}}_limit_min" value="{{ schedule[i].limit_gb_min }}" min="0" step="0.1" class="form-input input-range" placeholder="最小"><span class="input-suffix">~</span><input type="number" name="seg{{i}}_limit_max" value="{{ schedule[i].limit_gb_max }}" min="0" step="0.1" class="form-input input-range" placeholder="最大"></div>
        <div class="segment-row"><label>限速(MB/s)</label><input type="number" name="seg{{i}}_speed" value="{{ schedule[i].speed_limit }}" min="0" step="0.1" class="form-input input-sm"></div>
      </div>
      {% endfor %}
      <button type="submit" class="btn btn-primary btn-block btn-lg" style="margin-bottom:12px">保存</button>
      <a href="/clearschedule" class="btn btn-danger btn-block" onclick="return confirm('确定清空？')">清空</a>
    </form>
  </div>

  <div class="page" id="page-active-list">
    <div class="page-title">正在运行列表</div>
    <div class="card">
      <table class="data-table">
        <thead><tr><th>开始时间</th><th>运行时长</th><th>消耗流量</th><th>实时速度</th><th>连接位置(IP/省份)</th><th>运营商</th></tr></thead>
        <tbody id="active-tbody"><tr><td colspan="6" style="padding:20px;text-align:center;color:#999">加载中...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="page" id="page-custom-urls">
    <div class="page-title">自定义链接</div>
    <div class="card">
      <div class="info-row"><span class="info-label">链接总数</span><span class="info-value" id="link-total2">{{ all_len }} 条</span></div>
      <div class="info-row"><span class="info-label">测速链接</span><span class="info-value" id="link-speed2">{{ len_speed }} 条</span></div>
      <div class="info-row"><span class="info-label">视频链接</span><span class="info-value" id="link-video2">{{ len_video }} 条</span></div>
      <div class="info-row"><span class="info-label">直播链接</span><span class="info-value" id="link-live2">{{ len_live }} 条</span></div>
      <div class="info-row"><span class="info-label">黑名单</span><span class="info-value" id="link-blacklist2" style="color:#e74c3c">{{ blacklist_count }} 条</span></div>
      <div class="info-row"><span class="info-label">覆盖省份</span><span class="info-value">{{ all_province_count }} 个省市地区</span></div>
      <div class="btn-group" style="margin-top:16px">
        <a href="/refreshnow" class="btn btn-primary">🔄 立即刷新链接池</a>
        <a href="/crawlnow" class="btn btn-success">🕷️ 立即爬取更新</a>
      </div>
    </div>
  </div>

  <div class="page" id="page-settings">
    <div style="text-align:center;margin-bottom:18px"><div class="page-title" style="border:none;padding:0">设置</div></div>
    <div class="alert alert-warn">⚠ 增减来源设置建议重启后此软件，比例实时生效</div>
    <form method="POST" action="/setconfig">
      <div class="setting-field"><label class="setting-label">📡 来源</label><div class="radio-group" style="margin-top:8px"><label><input type="radio" name="source" value="default" {{ 'checked' if source_default=='default' }}> 默认</label><label><input type="radio" name="source" value="bibi" {{ 'checked' if source_default=='bibi' }}> bibi</label><label><input type="radio" name="source" value="import" {{ 'checked' if source_default=='import' }}> 导入</label></div></div>
      <div class="setting-field"><label class="setting-label">⚡ 较快链接比例(0-100)</label><input type="number" name="fast_link_ratio" value="{{ fast_link_ratio }}" min="0" max="100" class="setting-input"></div>
      <div class="setting-field"><label class="setting-label">📺 bibi使用比例(0-100)</label><input type="number" name="bibi_ratio" value="{{ bibi_ratio }}" min="0" max="100" class="setting-input"></div>
      <div class="setting-field"><label class="setting-label">📥 导入使用比例(0-100)</label><input type="number" name="import_ratio" value="{{ import_ratio }}" min="0" max="100" class="setting-input"></div>
      <div class="setting-field"><label class="setting-label">🔄 运行列表显示条数</label><input type="number" name="active_list_limit" value="{{ active_list_limit }}" min="1" max="500" class="setting-input"></div>
      <div class="setting-field"><label class="setting-label">⏱ 请求延迟(ms)</label><input type="number" name="url_delay_s" value="{{ url_delay_s }}" min="0" max="60000" class="setting-input"></div>
      <div class="setting-field"><label class="setting-label">💾 单URL每日最大(GB)</label><input type="number" name="single_url_max" value="{{ single_url_daily_max_gb }}" min="0" step="0.1" class="setting-input"></div>
      <button type="submit" class="btn btn-primary btn-block btn-lg" style="margin-top:20px">保存</button>
    </form>
  </div>

  <div class="page" id="page-about">
    <div class="page-title">版本说明</div>
    <div class="card">
      <div class="info-row"><span class="info-label">版本号</span><span class="info-value">v4.0</span></div>
      <div class="info-row"><span class="info-label">更新日期</span><span class="info-value">2026-06-20</span></div>
      <div class="info-row"><span class="info-label">新功能</span><span class="info-value" style="max-width:500px">全国{{ all_province_count }}省市CDN覆盖 · 省份+运营商精确识别 · 同省/外省智能分流 · 运营商筛选 · 自动爬取各省镜像 · 无流量自动剔除</span></div>
      <div class="info-row"><span class="info-label">覆盖地区</span><span class="info-value" style="max-width:500px">{{ province_list_str }}</span></div>
    </div>
  </div>
</div>

<script>
document.querySelectorAll('.nav-item').forEach(function(item){
  item.addEventListener('click', function(){
    document.querySelectorAll('.nav-item').forEach(function(n){ n.classList.remove('active') });
    document.querySelectorAll('.page').forEach(function(p){ p.classList.remove('active') });
    this.classList.add('active');
    document.getElementById('page-' + this.getAttribute('data-page')).classList.add('active');
  });
});
function refreshStats(){
  fetch('/api/stats').then(function(r){ return r.json(); }).then(function(d){
    var el;
    el=document.getElementById('dash-speed');if(el)el.textContent=d.speed;
    el=document.getElementById('dash-total');if(el)el.textContent=d.total;
    el=document.getElementById('dash-threads');if(el)el.textContent=d.threads;
    el=document.getElementById('dash-duration');if(el)el.textContent=d.duration;
    el=document.getElementById('sb-status');if(el)el.textContent=d.running?'正在冲刷':'已停止';
    el=document.getElementById('link-total');if(el)el.textContent=d.total_links;
    el=document.getElementById('link-speed');if(el)el.textContent=d.speed_count;
    el=document.getElementById('link-video');if(el)el.textContent=d.video_count;
    el=document.getElementById('link-live');if(el)el.textContent=d.live_count;
    el=document.getElementById('link-blacklist');if(el)el.textContent=d.blacklist;
    el=document.getElementById('link-total2');if(el)el.textContent=d.total_links+' 条';
    el=document.getElementById('link-speed2');if(el)el.textContent=d.speed_count+' 条';
    el=document.getElementById('link-video2');if(el)el.textContent=d.video_count+' 条';
    el=document.getElementById('link-live2');if(el)el.textContent=d.live_count+' 条';
    el=document.getElementById('link-blacklist2');if(el)el.textContent=d.blacklist+' 条';
    // ISP stats
    if(d.isp_stats){
      el=document.getElementById('isp-telecom');if(el)el.textContent=d.isp_stats['电信']||0;
      el=document.getElementById('isp-unicom');if(el)el.textContent=d.isp_stats['联通']||0;
      el=document.getElementById('isp-mobile');if(el)el.textContent=d.isp_stats['移动']||0;
      el=document.getElementById('isp-edu');if(el)el.textContent=d.isp_stats['教育网']||0;
    }
    // Province stats
    if(d.province_stats){
      var grid=document.getElementById('province-grid');
      if(grid){
        var h='';
        var sorted=Object.entries(d.province_stats).sort(function(a,b){return b[1].count-a[1].count});
        for(var i=0;i<sorted.length;i++){
          h+='<div class="province-tag"><div class="p-name">'+sorted[i][0]+'</div><div class="p-count">'+sorted[i][1].count+'个 | '+sorted[i][1].speed_str+'</div></div>';
        }
        if(!h)h='<div class="province-tag"><div class="p-name">暂无连接</div></div>';
        grid.innerHTML=h;
      }
    }
  }).catch(function(){});
}
function refreshActive(){
  fetch('/api/active').then(function(r){ return r.json(); }).then(function(data){
    var rows=data.data;var tbody=document.getElementById('active-tbody');if(!tbody)return;
    if(!rows.length){tbody.innerHTML='<tr><td colspan="6" style="padding:20px;text-align:center;color:#999">暂无活跃连接</td></tr>';return;}
    var h='';
    for(var i=0;i<rows.length;i++){var r=rows[i];
      var ispClass='';
      if(r.category==='电信')ispClass='color:#e74c3c';
      else if(r.category==='联通')ispClass='color:#3498db';
      else if(r.category==='移动')ispClass='color:#27ae60';
      else if(r.category==='教育网')ispClass='color:#f39c12';
      h+='<tr><td>'+r.start+'</td><td>'+r.duration+'</td><td>'+r.bytes+'</td><td>'+r.speed+'</td><td><div style="font-size:12px">'+r.host+'</div><div style="font-size:11.5px;color:#27ae60">'+r.location+'</div></td><td style="'+ispClass+';font-weight:600">'+r.category+'</td></tr>';}
    tbody.innerHTML=h;
  }).catch(function(){});
}
setInterval(function(){refreshStats();refreshActive();},5000);
refreshStats();refreshActive();
</script></body></html>"""

# ====================== Flask路由 ======================
@app.route("/")
def index():
    all_len = len(speed_url_list) + len(video_url_list) + len(live_url_list)
    run_dur = format_duration((datetime.now() - start_time).total_seconds()) if start_time else "0秒"
    return render_template_string(html_tpl,
        running=running, current_speed=format_speed(current_speed_bps),
        total_download=format_bytes(total_download_bytes), active_threads=active_threads,
        run_duration=run_dur, len_speed=len(speed_url_list), len_video=len(video_url_list),
        len_live=len(live_url_list), all_len=all_len, blacklist_count=len(blacklist),
        weight_speed=weight_speed, weight_video=weight_video, weight_live=weight_live,
        base_thread=base_thread, night_thread=night_thread, day_start_hour=day_start_hour,
        night_start_hour=night_start_hour, timeout_s=timeout_s, stall_timeout_s=stall_timeout_s,
        req_delay_ms=req_delay_ms, daily_limit_gb=daily_limit_gb, speed_limit_mbps=speed_limit_mbps,
        province_filter=province_filter, isp_filter=isp_filter, schedule=schedule_segments,
        source_default=source_default, fast_link_ratio=fast_link_ratio,
        bibi_ratio=bibi_ratio, import_ratio=import_ratio,
        active_list_limit=active_list_limit, url_delay_s=url_delay_s,
        single_url_daily_max_gb=single_url_daily_max_gb,
        local_ip=local_ip, local_province=local_province, local_isp=local_isp,
        all_province_count=len([p for p in PROVINCIAL_MIRRORS if p != "全国"]),
        province_list_str="、".join([p for p in PROVINCIAL_MIRRORS if p != "全国"]))

@app.route("/api/stats")
def api_stats():
    run_dur = format_duration((datetime.now() - start_time).total_seconds()) if start_time else "0秒"
    isp_stats = {}
    province_stats = {}
    for tid, info in list(active_connections.items()):
        cat = info.get("category", "其他")
        loc = info.get("location", "未知")
        isp_stats[cat] = isp_stats.get(cat, 0) + 1
        if loc not in province_stats:
            province_stats[loc] = {"count": 0, "speed": 0}
        province_stats[loc]["count"] += 1
        province_stats[loc]["speed"] += info.get("speed_bps", 0)
    # 格式化省份速度
    for p in province_stats:
        province_stats[p]["speed_str"] = format_speed(province_stats[p]["speed"])
    return {"running": running, "speed": format_speed(current_speed_bps), "total": format_bytes(total_download_bytes),
            "duration": run_dur, "threads": str(active_threads), "blacklist": str(len(blacklist)),
            "speed_count": str(len(speed_url_list)), "video_count": str(len(video_url_list)),
            "live_count": str(len(live_url_list)), "total_links": str(len(speed_url_list)+len(video_url_list)+len(live_url_list)),
            "isp_stats": isp_stats, "province_stats": province_stats}

@app.route("/api/active")
def api_active():
    rows = []; now = datetime.now()
    for tid, info in list(active_connections.items()):
        try:
            st = datetime.strptime(info["start_time"], "%H:%M:%S").replace(year=now.year, month=now.month, day=now.day)
            if st > now: st -= timedelta(days=1)
            elapsed = max(0, int((now - st).total_seconds()))
            dur_str = format_duration(elapsed)
        except Exception: dur_str = "运行中"
        rows.append({"start": info["start_time"], "duration": dur_str, "bytes": format_bytes(info["bytes"]),
                     "speed": format_speed(info.get("speed_bps", 0)),
                     "host": info.get("host","未知"), "location": info.get("location","未知"), "category": info.get("category","其他")})
    rows.sort(key=lambda x: x["start"], reverse=True)
    return {"data": rows}

@app.route("/setschedule", methods=["POST"])
def set_schedule():
    global schedule_segments
    for i in range(3):
        p = f"seg{i}_"; schedule_segments[i]["time_range"] = request.form.get(p+"time", "")
        t = request.form.get(p+"threads"); schedule_segments[i]["threads"] = int(t) if t and t.strip() else 0
        lm = request.form.get(p+"limit_min"); schedule_segments[i]["limit_gb_min"] = float(lm) if lm and lm.strip() else 0
        lx = request.form.get(p+"limit_max"); schedule_segments[i]["limit_gb_max"] = float(lx) if lx and lx.strip() else 0
        sp = request.form.get(p+"speed"); schedule_segments[i]["speed_limit"] = float(sp) if sp and sp.strip() else 0
    return '<script>alert("分时设置已保存");window.location.href="/";</script>'

@app.route("/clearschedule")
def clear_schedule():
    global schedule_segments
    schedule_segments = [{"time_range":"","threads":0,"limit_gb_min":0,"limit_gb_max":0,"speed_limit":0} for _ in range(3)]
    return '<script>alert("已清空");window.location.href="/";</script>'

@app.route("/setconfig", methods=["POST"])
def set_config():
    global base_thread, night_thread, day_start_hour, night_start_hour
    global weight_speed, weight_video, weight_live, timeout_s, stall_timeout_s, req_delay_ms
    global daily_limit_gb, speed_limit_mbps, province_filter, isp_filter
    global source_default, fast_link_ratio, bibi_ratio, import_ratio
    global active_list_limit, url_delay_s, single_url_daily_max_gb
    def _g(name, cast=int, default=None):
        v = request.form.get(name)
        if v is not None and v.strip(): return cast(v)
        return default
    base_thread = _g('base_thread') or base_thread
    night_thread = _g('night_thread') or night_thread
    day_start_hour = _g('day_start') or day_start_hour
    weight_speed = _g('ws') or weight_speed
    weight_video = _g('wv') or weight_video
    weight_live = _g('wl') or weight_live
    timeout_s = _g('timeout_s') or timeout_s
    stall_timeout_s = _g('stall_timeout') or stall_timeout_s
    req_delay_ms = _g('req_delay') or req_delay_ms
    daily_limit_gb = _g('daily_limit', float, 0.0) if request.form.get('daily_limit') is not None else daily_limit_gb
    speed_limit_mbps = _g('speed_limit', float, 0.0) if request.form.get('speed_limit') is not None else speed_limit_mbps
    province_filter = request.form.get('province') or province_filter
    isp_filter = request.form.get('isp') or isp_filter
    source_default = request.form.get('source') or source_default
    fast_link_ratio = _g('fast_link_ratio') if request.form.get('fast_link_ratio') else fast_link_ratio
    bibi_ratio = _g('bibi_ratio') if request.form.get('bibi_ratio') else bibi_ratio
    import_ratio = _g('import_ratio') if request.form.get('import_ratio') else import_ratio
    active_list_limit = _g('active_list_limit') if request.form.get('active_list_limit') else active_list_limit
    url_delay_s = _g('url_delay_s') if request.form.get('url_delay_s') else url_delay_s
    single_url_daily_max_gb = _g('single_url_max', float, 0.0) if request.form.get('single_url_max') else single_url_daily_max_gb
    return '<script>alert("配置保存成功");window.location.href="/";</script>'

@app.route("/stopandclear")
def stop_and_clear():
    global running, executor, daily_limit_gb, speed_limit_mbps, province_filter, isp_filter
    global base_thread, total_download_bytes, current_speed_bps, start_time
    running = False
    if executor: executor.shutdown(wait=False); executor = None
    daily_limit_gb=0; speed_limit_mbps=0; province_filter="all"; isp_filter="all"
    base_thread=20; total_download_bytes=0; current_speed_bps=0; start_time=None
    return '<script>alert("已停止并清空");window.location.href="/";</script>'

@app.route("/start")
def start_task():
    global running, start_time, executor, stats_timer, cleanup_timer, crawl_timer
    if not running:
        running = True; start_time = datetime.now()
        stats_timer = threading.Thread(target=stats_updater, daemon=True); stats_timer.start()
        if not cleanup_timer: cleanup_timer = threading.Thread(target=daily_cleanup, daemon=True); cleanup_timer.start()
        if not crawl_timer: crawl_timer = threading.Thread(target=auto_crawl_and_update, daemon=True); crawl_timer.start()
        threading.Thread(target=auto_cleanup_dead_links, daemon=True).start()
        threading.Thread(target=thread_pool_manager, daemon=True).start()
        print("[启动] 冲刷任务已启动")
    return '<script>window.location.href="/";</script>'

@app.route("/stop")
def stop_task():
    global running, executor
    running = False
    if executor: executor.shutdown(wait=False); executor = None
    print("[停止] 冲刷任务已停止")
    return '<script>window.location.href="/";</script>'

@app.route("/refreshnow")
def refresh_now():
    global speed_url_list, video_url_list, live_url_list
    speed_url_list = generate_speed_urls(5000)
    video_url_list = generate_video_urls(2000)
    live_url_list = generate_live_urls(500)
    all_len = len(speed_url_list)+len(video_url_list)+len(live_url_list)
    print(f"[手动刷新] 链接池: {all_len}条")
    return f'<script>alert("链接池已刷新: {all_len}条");window.location.href="/";</script>'

@app.route("/crawlnow")
def crawl_now():
    global speed_url_list, video_url_list, live_url_list, crawled_urls
    new_urls = []
    crawl_bases = []
    for province, mirrors in PROVINCIAL_MIRRORS.items():
        seen_hosts = set()
        for url, isp, _ in mirrors:
            host = extract_host_from_url(url)
            if host in seen_hosts: continue
            seen_hosts.add(host)
            p = urlparse(url)
            dir_path = "/".join(p.path.split("/")[:-1]) + "/"
            crawl_bases.append(f"{p.scheme}://{p.netloc}{dir_path}")
    for base in crawl_bases[:20]:
        try:
            resp = requests.get(base, headers=OPTIMIZED_HEADERS, timeout=8)
            if resp.status_code == 200:
                found = re.findall(r'href="([^"]+\.(deb|iso|xz|gz|tar|zip|exe)[^"]*)"', resp.text, re.I)
                for match in found[:30]:
                    link = match[0]
                    if link.startswith("/"):
                        p = urlparse(base); link = f"{p.scheme}://{p.netloc}{link}"
                    elif not link.startswith("http"):
                        link = base.rstrip("/") + "/" + link
                    if link not in new_urls: new_urls.append(link)
        except Exception: pass
    if new_urls:
        crawled_urls = new_urls[:1000]
        speed_url_list = generate_speed_urls(5000)
        video_url_list = generate_video_urls(2000)
        live_url_list = generate_live_urls(500)
    total = len(speed_url_list)+len(video_url_list)+len(live_url_list)
    return f'<script>alert("爬取{len(new_urls)}条，池总计{total}条");window.location.href="/";</script>'

@app.route("/resetstats")
def reset_stats():
    global total_download_bytes, current_speed_bps, start_time
    total_download_bytes=0; current_speed_bps=0; start_time=datetime.now() if running else None
    return '<script>alert("统计已重置");window.location.href="/";</script>'

if __name__ == "__main__":
    import logging; log = logging.getLogger('werkzeug'); log.setLevel(logging.ERROR)
    # 启动时检测本机IP信息
    detect_local_ip_info()
    app.run(host="0.0.0.0", port=9999, debug=False)
