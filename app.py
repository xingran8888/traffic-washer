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
import gc

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
base_thread = 120
speed_test_on_start = True
min_speed_mbps = 10
night_thread = 200
day_start_hour = 7
night_start_hour = 0
chunk_size = 1048576  # v5.3: 1MB，高速下载优化
req_delay_ms = 1
timeout_s = 8
stall_timeout_s = 3
max_fail_times = 3
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
thread_config_changed = False
active_connections = LimitedDict(maxlen=200)

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
                    local_province = normalize_province(data.get("province", data.get("regionName", "未知")))
                    local_isp = normalize_isp(data.get("isp", data.get("org", "未知")))
                elif data.get("status") == "success":
                    local_ip = data.get("query", "未知")
                    local_province = normalize_province(data.get("regionName", "未知"))
                    local_isp = normalize_isp(data.get("isp", "未知"))
                print(f"[检测] 本机IP: {local_ip} | 省份: {local_province} | 运营商: {local_isp}")
                return
        except Exception: pass
    print("[检测] 无法检测本机IP信息")

import url_pool
from url_pool import (
    PROVINCIAL_MIRRORS, VIDEO_CDN_SOURCES, LIVE_STREAM_SOURCES, PROVINCE_ISP_MAP,
    normalize_province, normalize_isp,
    generate_speed_urls, generate_video_urls, generate_live_urls
)

# ====================== 全局变量 ======================
ALL_PROVINCES = list(PROVINCIAL_MIRRORS.keys())
speed_url_list = []
video_url_list = []
live_url_list = []
crawled_urls = []
pool_lock = threading.Lock()
blacklist_lock = threading.Lock()
fail_count_lock = threading.Lock()
traffic_lock = threading.Lock()
connections_lock = threading.Lock()
domain_last_hit = {}
DOMAIN_COOLDOWN_S = 0.2
url_alive_fail_count = {}
alive_fail_lock = threading.Lock()

# ====================== 初始化 ======================
print("[初始化] 生成链接池...")
_speed = generate_speed_urls(5000)
_video = generate_video_urls(2000)
_live = generate_live_urls(500)
with pool_lock:
    speed_url_list = _speed
    video_url_list = _video
    live_url_list = _live
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
    Bps = bps / 8
    if Bps < 1024: return f"{Bps:.0f} B/s"
    elif Bps < 1048576: return f"{Bps/1024:.1f} KB/s"
    elif Bps < 1073741824: return f"{Bps/1048576:.1f} MB/s"
    else: return f"{Bps/1073741824:.2f} GB/s"

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
# URL→省份反向索引（从PROVINCIAL_MIRRORS构建）
url_province_map = {}

def build_url_province_map():
    """从PROVINCIAL_MIRRORS构建URL到省份的映射"""
    global url_province_map
    for province, mirrors in PROVINCIAL_MIRRORS.items():
        for entry in mirrors:
            url = entry[0]
            if url not in url_province_map:
                url_province_map[url] = province
    print(f"[初始化] URL省份索引: {len(url_province_map)}条")

def get_province_for_url(url):
    """获取URL对应的省份（先查URL索引，再查域名映射）"""
    # 优先从PROVINCIAL_MIRRORS反向索引获取
    if url in url_province_map:
        return url_province_map[url]
    # 回退到域名映射
    host = extract_host_from_url(url)
    for domain, (province, _) in PROVINCE_ISP_MAP.items():
        if domain in host or host.endswith("." + domain):
            return normalize_province(province)
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
        with traffic_lock:
            url_daily_traffic.clear()
        with blacklist_lock:
            blacklist.clear()
        with fail_count_lock:
            url_fail_count.clear()
        with alive_fail_lock:
            url_alive_fail_count.clear()
        print("[清理] 每日统计已清空")


# ====================== v5.3 链接预验证与筛选 ======================
def validate_url(url, timeout=3):
    """HEAD请求验证URL可达性，返回(可达, 速度bytes/s)"""
    try:
        import requests as req
        t0 = time.time()
        r = req.head(url, timeout=timeout, allow_redirects=True, headers={'User-Agent': 'Mozilla/5.0'})
        elapsed = time.time() - t0
        if r.status_code in (200, 301, 302, 303, 307, 308):
            size = int(r.headers.get('content-length', 0))
            speed = size / elapsed if elapsed > 0 and size > 0 else 1000000  # 默认1MB/s
            return True, speed
        return False, 0
    except Exception:
        return False, 0

def warmup_filter_urls(url_list, max_workers=100, sample_size=99999):
    """预热筛选：抽样验证URL可达性，剔除不可达的链接"""
    if not url_list:
        return url_list
    import concurrent.futures
    sample = url_list[:sample_size] if len(url_list) > sample_size else url_list
    valid = []
    invalid = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(validate_url, u): u for u in sample}
        for fut in concurrent.futures.as_completed(futures):
            u = futures[fut]
            try:
                ok, spd, sz = fut.result()
                if ok and sz >= 52428800:
                    valid.append(u)
                else:
                    invalid.append(u)
            except Exception:
                invalid.append(u)
    # 从原始列表中移除验证失败的URL
    invalid_set = set(invalid)
    filtered = [u for u in url_list if u not in invalid_set]
    removed = len(url_list) - len(filtered)
    if removed > 0:
        print(f"[筛选] 预验证 {len(sample)} 条URL: {len(valid)}可达, {len(invalid)}不可达, 从池中移除 {removed} 条")
    else:
        print(f"[筛选] 预验证 {len(sample)} 条URL: 全部可达")
    return filtered


def zero_traffic_cleanup():
    """v5.3: 定期清理0流量URL"""
    while running:
        time.sleep(30)
        with traffic_lock:
            dead = [u for u, t in url_session_traffic.items() if t == 0]
        if dead:
            bl_add = 0
            with fail_count_lock:
                for u in dead:
                    cnt = url_fail_count.get(u, 0) + 1
                    url_fail_count[u] = cnt
                    if cnt >= 3:
                        with blacklist_lock:
                            blacklist.add(u)
                        bl_add += 1
            if bl_add > 0:
                print(f"[清理] 拉黑 {bl_add} 条0流量URL, 黑名单共 {len(blacklist)} 条")

def track_domain_speed(domain, bytes_recv):
    """追踪每个域名的下载速度"""
    with domain_speed_lock:
        if domain not in domain_speed_stats:
            domain_speed_stats[domain] = {'bytes': 0, 'start': time.time(), 'avg': 0}
        s = domain_speed_stats[domain]
        s['bytes'] += bytes_recv
        elapsed = time.time() - s['start']
        if elapsed > 5:
            s['avg'] = s['bytes'] / elapsed
            s['bytes'] = 0
            s['start'] = time.time()

def get_slow_domains(threshold_mbps=5):
    """返回平均速度低于阈值的域名列表"""
    with domain_speed_lock:
        slow = []
        for d, s in domain_speed_stats.items():
            if s['avg'] > 0 and s['avg'] < threshold_mbps * 1000000 / 8:
                slow.append(d)
        return slow

def periodic_speed_test():
    """v5.3: 每5分钟自动测速一次"""
    while running:
        time.sleep(300)  # 5 minutes
        if running:
            try:
                speed_test_and_switch()
            except Exception as e:
                print(f"[测速] 异常: {e}")


# ====================== v5.4 启动测速 + 域名级拉黑 ======================
def speed_test_and_switch():
    """v5.4: 从链接池随机测速8条，不可达域名整批拉黑"""
    global speed_url_list, video_url_list, live_url_list
    import requests as req
    test_count = 8
    print("[测速] 开始随机测速...")
    with pool_lock:
        all_urls = list(speed_url_list) + list(video_url_list) + list(live_url_list)
    if not all_urls:
        return
    sample = random.sample(all_urls, min(test_count, len(all_urls)))
    results = []
    for url in sample:
        test_url = url.split("?")[0] + "?t=" + str(random.randint(1000000, 9999999))
        try:
            t0 = time.time()
            resp = req.get(test_url, stream=True, timeout=5, headers=OPTIMIZED_HEADERS)
            total = 0
            for chunk in resp.iter_content(chunk_size=262144):
                total += len(chunk)
                if total > 524288:
                    break
            elapsed = time.time() - t0
            speed_mbps = (total * 8) / (elapsed * 1000000) if elapsed > 0 else 0
            domain = get_domain_from_url(url)
            results.append((url, speed_mbps, domain))
            resp.close()
        except Exception:
            domain = get_domain_from_url(url)
            results.append((url, 0, domain))
    if not results:
        return
    avg_speed = sum(s for _, s, _ in results) / len(results)
    fast = [(u, s, d) for u, s, d in results if s >= min_speed_mbps]
    dead = [(u, s, d) for u, s, d in results if s == 0]
    print(f"[测速] 平均: {avg_speed:.1f} Mbps | 达标: {len(fast)} | 不可达: {len(dead)}")
    dead_domains = set(d for _, _, d in dead if d)
    if dead_domains:
        with blacklist_lock:
            for url, _, _ in dead:
                blacklist.add(url)
            for pool in [speed_url_list, video_url_list, live_url_list]:
                for u in pool:
                    if get_domain_from_url(u) in dead_domains:
                        blacklist.add(u)
        print(f"[测速] 拉黑死域名 {len(dead_domains)} 个")

def periodic_speed_test():
    """v5.4: 每5分钟自动测速"""
    while running:
        time.sleep(300)
        if running:
            try:
                speed_test_and_switch()
            except Exception as e:
                print(f"[测速] 异常: {e}")

def zero_traffic_cleanup():
    """v5.4: 域名级0流量清理 - 同域名3条以上0流量则整批拉黑"""
    while running:
        # Wait up to 10s, but wake immediately on config change
        for _ in range(10):
            if thread_config_changed: break
            time.sleep(1)
        with traffic_lock:
            dead = [u for u, t in url_session_traffic.items() if t == 0]
        if not dead:
            continue
        dom_count = {}
        for u in dead:
            d = get_domain_from_url(u)
            if d:
                dom_count[d] = dom_count.get(d, 0) + 1
        bl_domains = [d for d, cnt in dom_count.items() if cnt >= 3]
        if bl_domains:
            with blacklist_lock:
                for pool in [speed_url_list, video_url_list, live_url_list]:
                    for u in pool:
                        if get_domain_from_url(u) in bl_domains:
                            blacklist.add(u)
            print(f"[清理] 拉黑死域名 {len(bl_domains)} 个, 黑名单共 {len(blacklist)} 条")

def thread_pool_manager():
    global active_threads, executor, thread_config_changed
    while running:
        target_threads = get_current_thread_count()
        if active_threads != target_threads or thread_config_changed:
            thread_config_changed = False
            if executor: executor.shutdown(wait=False)
            executor = ThreadPoolExecutor(max_workers=target_threads)
            active_threads = target_threads
            for _ in range(target_threads): executor.submit(download_worker)
            print(f"[线程] 调整为 {target_threads} 个线程")
        # Wait up to 10s, but wake immediately on config change
        for _ in range(10):
            if thread_config_changed: break
            time.sleep(1)

# ====================== 核心下载循环 ======================
def get_domain_from_url(url):
    """提取URL的域名(含端口)用于冷却"""
    try:
        parsed = urlparse(url)
        return parsed.hostname or ""
    except Exception:
        return ""


def pick_url_distributed():
    """v5.3 URL selection: blacklist + cooldown + province/ISP filter + city-level CDN distribution"""
    total_w = weight_speed + weight_video + weight_live
    rand_val = random.randint(1, total_w)
    if rand_val <= weight_speed:
        pool_name = "speed"
    elif rand_val <= weight_speed + weight_video:
        pool_name = "video"
    else:
        pool_name = "live"
    now_ts = time.time()
    with blacklist_lock:
        bl = blacklist.copy()
    if province_filter != "all" or isp_filter != "all":
        candidates = []
        _pf = province_filter
        _lp = local_province
        _matched = 0
        for province, mirrors in PROVINCIAL_MIRRORS.items():
            if province_filter == "same" and province != local_province:
                continue
            _matched += len(mirrors)
            if province_filter == "other" and province == local_province:
                continue
            for url, isp, weight in mirrors:
                if isp_filter == "dianxin" and isp != "\xe7\x94\xb5\xe4\xbf\xa1":
                    continue
                if isp_filter == "liantong" and isp != "\xe8\x81\x94\xe9\x80\x9a":
                    continue
                if isp_filter == "yidong" and isp != "\xe7\xa7\xbb\xe5\x8a\xa8":
                    continue
                if isp_filter == "jiaoyu" and isp != "\xe6\x95\x99\xe8\x82\xb2\xe7\xbd\x91":
                    continue
                u = f"{url}?t={random.randint(1000000, 9999999)}"
                if u in bl:
                    continue
                candidates.append((u, weight))
        if not candidates:
            print(f"[DEBUG] pick_url: 0 candidates! pf={_pf} lp={_lp} matched={_matched} bl={len(bl)}")
        if candidates:
            # v5.3 city-level distribution: group by CDN domain, round-robin across domains
            domain_groups = {}
            for u, w in candidates:
                d = get_domain_from_url(u)
                if d not in domain_groups:
                    domain_groups[d] = []
                domain_groups[d].append((u, w))
            domains = list(domain_groups.keys())
            domains.sort(key=lambda d: domain_last_hit.get(d, 0))
            for d in domains:
                g = domain_groups[d]
                if g:
                    tw = sum(w for _, w in g)
                    r = random.uniform(0, tw)
                    cum = 0
                    for u, w in g:
                        cum += w
                        if r <= cum:
                            domain_last_hit[d] = time.time()
                            return u
                    domain_last_hit[d] = time.time()
                    return g[-1][0]
            total_weight = sum(w for _, w in candidates)
            r = random.uniform(0, total_weight)
            cumulative = 0
            for u, w in candidates:
                cumulative += w
                if r <= cumulative:
                    domain = get_domain_from_url(u)
                    if domain:
                        domain_last_hit[domain] = time.time()
                    return u
            u = candidates[-1][0]
            domain = get_domain_from_url(u)
            if domain:
                domain_last_hit[domain] = time.time()
            return u
    with pool_lock:
        if pool_name == "speed":
            pool = list(speed_url_list)
        elif pool_name == "video":
            pool = list(video_url_list)
        else:
            pool = list(live_url_list)
    source = list(pool)
    random.shuffle(source)
    for u in source:
        if u in bl:
            continue
        with traffic_lock:
            if url_session_traffic.get(u, -1) == 0:
                continue
        domain = get_domain_from_url(u)
        if domain:
            last_hit = domain_last_hit.get(domain, 0)
            if now_ts - last_hit < DOMAIN_COOLDOWN_S:
                continue
        if domain:
            domain_last_hit[domain] = time.time()
        return u
    for u in source:
        if u in bl:
            continue
        domain = get_domain_from_url(u)
        if domain:
            domain_last_hit[domain] = time.time()
        return u

def download_worker():
    global total_download_bytes
    tid = threading.current_thread().name
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=1)
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
        with connections_lock:
            active_connections[tid] = {
                "url": target_url, "start_time": conn_start.strftime("%H:%M:%S"),
                "bytes": 0, "host": host_display, "location": ip_loc, "category": ip_cat,
                "speed_bps": 0, "last_bytes": 0, "speed_ts": now_ts
            }

        try:
            with session.get(target_url, stream=True, timeout=timeout_s, headers=OPTIMIZED_HEADERS) as resp:
                resp.raise_for_status()
                last_recv_time = time.time(); got_data = False
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not running:
                        with connections_lock:
                            active_connections.pop(tid, None)
                        return
                    if chunk:
                        got_data = True; chunk_len = len(chunk)
                        conn_bytes += chunk_len
                        total_download_bytes += chunk_len
                        with traffic_lock:
                            url_session_traffic[target_url] = url_session_traffic.get(target_url, 0) + chunk_len
                            url_daily_traffic[target_url] = url_daily_traffic.get(target_url, 0) + chunk_len
                        _now = time.time()
                        _dt = _now - active_connections[tid].get("speed_ts", _now)
                        if _dt >= 1.0:
                            _delta = conn_bytes - active_connections[tid].get("last_bytes", 0)
                            speed_bps = _delta * 8 / _dt
                            with connections_lock:
                                active_connections[tid]["bytes"] = conn_bytes
                                active_connections[tid]["speed_bps"] = speed_bps
                                active_connections[tid]["last_bytes"] = conn_bytes
                                active_connections[tid]["speed_ts"] = _now
                        else:
                            with connections_lock:
                                active_connections[tid]["bytes"] = conn_bytes
                        last_recv_time = time.time()
                    elif got_data:
                        if time.time() - last_recv_time >= stall_timeout_s:
                            break
            with fail_count_lock:
                url_fail_count[target_url] = 0
        except Exception:
            with fail_count_lock:
                cnt = url_fail_count.get(target_url, 0) + 1
                url_fail_count[target_url] = cnt
                if cnt >= max_fail_times:
                    with blacklist_lock:
                        blacklist.add(target_url)
        finally:
            with connections_lock:
                active_connections.pop(tid, None)
            time.sleep(req_delay_ms / 1000)

# ====================== v5.5: 定时内存清理 ======================
def memory_cleanup():
    """每5分钟强制GC + 清理过期数据"""
    while True:
        time.sleep(300)
        gc.collect()
        with traffic_lock:
            expired = [k for k, v in url_session_traffic.items() if v == 0]
            for k in expired[:1000]:
                url_session_traffic.pop(k, None)
        with connections_lock:
            now = time.time()
            zombie = [k for k, v in active_connections.items()
                      if now - v.get("speed_ts", now) > 600]
            for k in zombie:
                active_connections.pop(k, None)
        print(f"[内存] GC完成 | traffic:{len(url_session_traffic)} conn:{len(active_connections)}")

# ====================== 自动清理 ======================
def auto_cleanup_dead_links():
    """每5分钟：剔除无流量链接，随机补充新链接（v5.4: 线程安全）"""
    global speed_url_list, video_url_list, live_url_list
    while True:
        time.sleep(300)
        if not running: continue

        with pool_lock:
            before = len(speed_url_list) + len(video_url_list) + len(live_url_list)

            with blacklist_lock:
                speed_url_list[:] = [u for u in speed_url_list if u not in blacklist]
                video_url_list[:] = [u for u in video_url_list if u not in blacklist]
                live_url_list[:] = [u for u in live_url_list if u not in blacklist]

            dead_urls = set()
            with traffic_lock:
                for u in list(url_session_traffic.keys()):
                    if url_session_traffic[u] == 0:
                        with blacklist_lock:
                            if u not in blacklist:
                                dead_urls.add(u)
            if dead_urls:
                speed_url_list[:] = [u for u in speed_url_list if u not in dead_urls]
                video_url_list[:] = [u for u in video_url_list if u not in dead_urls]
                live_url_list[:] = [u for u in live_url_list if u not in dead_urls]
                with traffic_lock:
                    for u in dead_urls:
                        url_session_traffic.pop(u, None)
                with fail_count_lock:
                    for u in dead_urls:
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

        with fail_count_lock:
            if len(url_fail_count) > 5000: url_fail_count.clear()
        with traffic_lock:
            if len(url_session_traffic) > 30000:
                # v5.5: 更激进的清理，只保留最近有流量的
                kept = {k: v for k, v in url_session_traffic.items() if v > 0}
                url_session_traffic.clear()
                url_session_traffic.update(kept)
                if len(url_session_traffic) > 5000:
                    items = list(url_session_traffic.items())
                    url_session_traffic.clear()
                    url_session_traffic.update(dict(items[len(items)//2:]))

# ====================== 爬虫（全国各省）======================
def auto_crawl_and_update():
    """爬虫：自动爬取全国各省镜像站目录（v5.4: HEAD验证）"""
    global speed_url_list, video_url_list, live_url_list, crawled_urls
    while True:
        time.sleep(7200)
        print("[爬虫] 开始抓取全国各省镜像...")
        raw_urls = []
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
                        if link not in raw_urls:
                            raw_urls.append(link)
            except Exception: pass

        # v5.4: HEAD验证，只保留 200/301/302 的URL
        new_urls = []
        for u in raw_urls:
            try:
                head_resp = requests.head(u, headers=OPTIMIZED_HEADERS, timeout=5, allow_redirects=True)
                if head_resp.status_code in (200, 301, 302):
                    new_urls.append(u)
            except Exception:
                pass  # HEAD失败的直接跳过

        if new_urls:
            with pool_lock:
                crawled_urls = new_urls[:1000]
                url_pool.crawled_urls = crawled_urls
                speed_url_list = generate_speed_urls(5000)
                video_url_list = generate_video_urls(2000)
                live_url_list = generate_live_urls(500)
            print(f"[爬虫] 更新完成: 爬取{len(raw_urls)}条，验证通过{len(new_urls)}条，池总计{len(speed_url_list)+len(video_url_list)+len(live_url_list)}条")


# ====================== v5.4: 后台存活校验线程 ======================
def alive_checker():
    """每60秒对链接池URL做HEAD探测，连续失败3次移入黑名单"""
    while True:
        time.sleep(60)
        if not running:
            continue

        with pool_lock:
            all_urls = list(set(speed_url_list + video_url_list + live_url_list))

        # 采样检测，每次最多100个URL
        sample = random.sample(all_urls, min(100, len(all_urls))) if all_urls else []

        for url in sample:
            try:
                resp = requests.head(url, headers=OPTIMIZED_HEADERS, timeout=5, allow_redirects=True)
                if resp.status_code in (200, 301, 302):
                    # 成功，重置失败计数
                    with alive_fail_lock:
                        url_alive_fail_count.pop(url, None)
                else:
                    with alive_fail_lock:
                        url_alive_fail_count[url] = url_alive_fail_count.get(url, 0) + 1
                        if url_alive_fail_count[url] >= 3:
                            with blacklist_lock:
                                blacklist.add(url)
                            url_alive_fail_count.pop(url, None)
                            print(f"[存活校验] 移入黑名单(状态码{resp.status_code}): {url[:80]}")
            except Exception:
                with alive_fail_lock:
                    url_alive_fail_count[url] = url_alive_fail_count.get(url, 0) + 1
                    if url_alive_fail_count[url] >= 3:
                        with blacklist_lock:
                            blacklist.add(url)
                        url_alive_fail_count.pop(url, None)
                        print(f"[存活校验] 移入黑名单(连接失败): {url[:80]}")


# ====================== v5.4: 黑名单恢复机制 ======================
def blacklist_recovery():
    """每10分钟从黑名单中随机恢复10%的链接重新测试"""
    while True:
        time.sleep(600)  # 10分钟
        if not running:
            continue

        with blacklist_lock:
            bl_list = list(blacklist)

        if not bl_list:
            continue

        # 恢复10%（至少1个）
        recover_count = max(1, len(bl_list) // 10)
        to_recover = random.sample(bl_list, min(recover_count, len(bl_list)))

        recovered = []
        for url in to_recover:
            try:
                resp = requests.head(url, headers=OPTIMIZED_HEADERS, timeout=5, allow_redirects=True)
                if resp.status_code in (200, 301, 302):
                    recovered.append(url)
            except Exception:
                pass  # 仍然不可用，保留在黑名单

        if recovered:
            with blacklist_lock:
                for url in recovered:
                    blacklist.discard(url)
            with fail_count_lock:
                for url in recovered:
                    url_fail_count.pop(url, None)
            with alive_fail_lock:
                for url in recovered:
                    url_alive_fail_count.pop(url, None)
            print(f"[黑名单恢复] 恢复 {len(recovered)}/{len(to_recover)} 个链接")

# ====================== 省份/运营商统计 ======================
def get_province_stats():
    """统计正在运行的连接按省份分布（v5.4: 线程安全）"""
    stats = defaultdict(lambda: {"count": 0, "bytes": 0, "speed": 0})
    with connections_lock:
        for tid, info in list(active_connections.items()):
            loc = info.get("location", "未知")
            stats[loc]["count"] += 1
            stats[loc]["bytes"] += info.get("bytes", 0)
            stats[loc]["speed"] += info.get("speed_bps", 0)
    return dict(stats)

def get_isp_stats():
    """统计正在运行的连接按运营商分布（v5.4: 线程安全）"""
    stats = defaultdict(lambda: {"count": 0, "bytes": 0, "speed": 0})
    with connections_lock:
        for tid, info in list(active_connections.items()):
            cat = info.get("category", "其他")
            stats[cat]["count"] += 1
            stats[cat]["bytes"] += info.get("bytes", 0)
            stats[cat]["speed"] += info.get("speed_bps", 0)
    return dict(stats)

# ====================== Web面板 ======================
html_tpl = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>流量冲刷器 v5.4</title>
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
  <div class="sidebar-header">🌊 流量冲刷器 v5.4</div>
  <nav class="sidebar-nav">
    <div class="nav-item active" data-page="dashboard"><span class="nav-icon">📊</span>每日任务</div>
    <div class="nav-item" data-page="schedule"><span class="nav-icon">⏰</span>分时设置</div>
    <div class="nav-item" data-page="active-list"><span class="nav-icon">🔄</span>正在运行列表</div>
    <div class="nav-item" data-page="custom-urls"><span class="nav-icon">🔗</span>自定义链接</div>
    <div class="nav-item" data-page="settings"><span class="nav-icon">⚙️</span>设置</div>
    <div class="nav-item" data-page="about"><span class="nav-icon">ℹ️</span>版本说明</div>
    <div class="nav-item" data-page="logs"><span class="nav-icon">📋</span>运行日志</div>
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
        </div>
      {% if not running %}
      <button onclick="doStart()" class="btn btn-success btn-block btn-lg" style="margin-bottom:12px;font-size:16px">▶ 开始冲刷</button>
      {% else %}
      <div class="btn-group" style="margin-bottom:12px">
        <button onclick="doStop()" class="btn btn-danger btn-lg" style="flex:1;font-size:15px">⏹ 暂停冲刷</button>
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
      <li>爬虫每2小时自动更新全国各省镜像站链接（含HEAD验证）</li>
      <li>v5.1: 线程安全锁保护 · 域名冷却2秒 · 每60秒存活校验 · 黑名单10分钟自动恢复10%</li>
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
      <div class="info-row"><span class="info-label">版本号</span><span class="info-value">v5.5</span></div>
  <div class="page" id="page-logs">
    <div class="page-title">运行日志</div>
    <div class="card">
      <div style="display:flex;gap:10px;margin-bottom:12px">
        <button onclick="loadLogs(200)" class="btn btn-primary">刷新日志</button>
        <button onclick="loadLogs(100)" class="btn btn-success">最近100行</button>
        <button onclick="loadLogs(500)" class="btn btn-success">最近500行</button>
      </div>
      <pre id="log-content" style="background:#1e1e1e;color:#d4d4d4;padding:16px;border-radius:8px;font-size:12px;max-height:600px;overflow-y:auto;white-space:pre-wrap;word-break:break-all">点击上方按钮加载日志...</pre>
    </div>
  </div>

      <div class="info-row"><span class="info-label">更新日期</span><span class="info-value">2026-06-20</span></div>
      <div class="info-row"><span class="info-label">新功能</span><span class="info-value" style="max-width:500px">全国{{ all_province_count }}省市CDN覆盖 · 省份+运营商精确识别 · 同省/外省智能分流 · 运营商筛选 · 自动爬取各省镜像 · 无流量自动剔除 · 线程安全锁 · 域名冷却 · 存活校验 · 黑名单恢复</span></div>
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
function loadLogs(n){
  fetch("/api/logs?lines="+(n||200)).then(function(r){return r.text()}).then(function(t){
    var el=document.getElementById("log-content");
    if(el)el.textContent=t;
    if(el)el.scrollTop=el.scrollHeight;
  });
}
function doStart(){
  var btn=document.getElementById("btn-start");
  if(btn){btn.textContent="启动中...";btn.disabled=true;}
  // Auto-save config first, then start
  var form=document.querySelector("form[action='/setconfig']");
  if(form){
    var fd=new FormData(form);
    fetch("/setconfig",{method:"POST",body:fd}).then(function(){
      return fetch("/start");
    }).then(function(){
      setTimeout(function(){
        var sb=document.getElementById("sb-status");if(sb)sb.textContent="正在冲刷";
        if(btn)btn.style.display="none";
        refreshStats();
      },2000);
    }).catch(function(){if(btn){btn.textContent="▶ 开始冲刷";btn.disabled=false;}});
  } else {
    fetch("/start").then(function(){
      setTimeout(function(){
        var sb=document.getElementById("sb-status");if(sb)sb.textContent="正在冲刷";
        if(btn)btn.style.display="none";
        refreshStats();
      },2000);
    }).catch(function(){if(btn){btn.textContent="▶ 开始冲刷";btn.disabled=false;}});
  }
}
function doStop(){
  var btn=document.getElementById("btn-stop");
  if(btn){btn.textContent="停止中...";btn.disabled=true;}
  fetch("/stop").then(function(){
    var sb=document.getElementById("sb-status");if(sb)sb.textContent="已停止";
    if(btn){btn.textContent="⏹ 暂停冲刷";btn.disabled=false;}
  }).catch(function(){if(btn){btn.textContent="⏹ 暂停冲刷";btn.disabled=false;}});
}
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
    with pool_lock:
        len_s = len(speed_url_list)
        len_v = len(video_url_list)
        len_l = len(live_url_list)
    with blacklist_lock:
        bl_count = len(blacklist)
    all_len = len_s + len_v + len_l
    run_dur = format_duration((datetime.now() - start_time).total_seconds()) if start_time else "0秒"
    return render_template_string(html_tpl,
        running=running, current_speed=format_speed(current_speed_bps),
        total_download=format_bytes(total_download_bytes), active_threads=active_threads,
        run_duration=run_dur, len_speed=len_s, len_video=len_v,
        len_live=len_l, all_len=all_len, blacklist_count=bl_count,
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

@app.route("/api/logs")
def api_logs():
    lines = request.args.get("lines", 200, type=int)
    return log_buffer.get(lines)


# ====================== v5.4 全量测速剔除低速 v2 ======================
def speed_test_pool():
    """测试所有URL: 4并发, 单线程15秒, 权重调整"""
    global speed_url_list, video_url_list, live_url_list
    import requests as req
    import concurrent.futures
    with pool_lock:
        all_urls = list(set(speed_url_list + video_url_list + live_url_list))
    total = len(all_urls)
    if total == 0:
        print("[测速] 链接池为空")
        return
    print(f"[测速] 开始全量测速 {total} 条URL (4并发, 单线程15秒)...")
    high = []
    mid = []
    dead = []
    tested = 0
    t_start = time.time()

    def test_speed(url):
        try:
            test_url = url.split("?")[0] + "?t=" + str(random.randint(1000000, 9999999))
            t0 = time.time()
            resp = req.get(test_url, stream=True, timeout=20, headers=OPTIMIZED_HEADERS)
            total_bytes = 0
            for chunk in resp.iter_content(chunk_size=65536):
                total_bytes += len(chunk)
                if time.time() - t0 >= 15:
                    break
            elapsed = max(time.time() - t0, 1)
            speed_mbs = total_bytes / elapsed / 1048576
            return (url, speed_mbs, total_bytes, True)
        except:
            return (url, 0, 0, False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(test_speed, u): u for u in all_urls}
        for fut in concurrent.futures.as_completed(futures):
            tested += 1
            url, speed_mbs, total_bytes, ok = fut.result()
            if ok and speed_mbs >= 40.0:
                high.append((url, speed_mbs))
            elif ok and speed_mbs >= 5.0:
                mid.append((url, speed_mbs))
            else:
                dead.append((url, speed_mbs))
            if tested % 20 == 0:
                elapsed = time.time() - t_start
                eta = (elapsed / tested) * (total - tested)
                print(f"[测速] {tested}/{total} 高:{len(high)} 中:{len(mid)} 低:{len(dead)} | ETA:{int(eta)}s")

    # Remove dead URLs
    dead_urls = [u for u, _ in dead]
    with pool_lock:
        speed_url_list[:] = [u for u in speed_url_list if u not in set(dead_urls)]
        video_url_list[:] = [u for u in video_url_list if u not in set(dead_urls)]
        live_url_list[:] = [u for u in live_url_list if u not in set(dead_urls)]
    with blacklist_lock:
        for u in dead_urls:
            blacklist.add(u)

    # Weight: high 3x, mid 1x
    with pool_lock:
        for u, _ in high:
            speed_url_list.extend([u] * 2)
        speed_url_list[:] = list(dict.fromkeys(speed_url_list))
        video_url_list[:] = list(dict.fromkeys(video_url_list))
        live_url_list[:] = list(dict.fromkeys(live_url_list))

    total_valid = len(speed_url_list) + len(video_url_list) + len(live_url_list)
    total_time = int(time.time() - t_start)
    print(f"[测速] ====== 完成 ======")
    print(f"[测速] 耗时: {total_time//60}分{total_time%60}秒")
    print(f"[测速] 测试: {total} | 高(>=40):{len(high)} | 中(5-40):{len(mid)} | 低(<5):{len(dead)}")
    print(f"[测速] 链接池: 测速{len(speed_url_list)} 视频{len(video_url_list)} 直播{len(live_url_list)} 总计{total_valid}")
    if high:
        top5 = sorted(high, key=lambda x: -x[1])[:5]
        print(f"[测速] TOP5高速: " + " | ".join(f"{s:.0f}MB/s" for _, s in top5))
@app.route("/speedtest")
def speed_test_route():
    if running:
        threading.Thread(target=speed_test_pool, daemon=True).start()
        return '<script>alert("全量测速已启动(50线程,<40MB/s剔除),请查看日志");window.location.href="/";</script>'
    return '<script>alert("请先启动任务");window.location.href="/";</script>'

@app.route("/api/stats")
def api_stats():
    run_dur = format_duration((datetime.now() - start_time).total_seconds()) if start_time else "0秒"
    isp_stats = {}
    province_stats = {}
    with connections_lock:
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
    with pool_lock:
        sp_c = len(speed_url_list); vi_c = len(video_url_list); li_c = len(live_url_list)
    with blacklist_lock:
        bl_c = len(blacklist)
    return {"running": running, "speed": format_speed(current_speed_bps), "total": format_bytes(total_download_bytes),
            "duration": run_dur, "threads": str(active_threads), "blacklist": str(bl_c),
            "speed_count": str(sp_c), "video_count": str(vi_c),
            "live_count": str(li_c), "total_links": str(sp_c+vi_c+li_c),
            "isp_stats": isp_stats, "province_stats": province_stats}

@app.route("/api/active")
def api_active():
    rows = []; now = datetime.now()
    with connections_lock:
        items = list(active_connections.items())
    for tid, info in items:
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
    _bt = _g('base_thread')
    if _bt is not None and _bt > 0: base_thread = _bt
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
    # Signal thread pool to refresh
    global thread_config_changed
    thread_config_changed = True
    return '<script>alert("配置保存成功");window.location.href="/";</script>'

@app.route("/stopandclear")
def stop_and_clear():
    global running, executor, daily_limit_gb, speed_limit_mbps, province_filter, isp_filter
    global base_thread, total_download_bytes, current_speed_bps, start_time
    running = False
    if executor: executor.shutdown(wait=False); executor = None
    daily_limit_gb=0; speed_limit_mbps=0; province_filter="all"; isp_filter="all"
    total_download_bytes=0; current_speed_bps=0; start_time=None
    # v5.4: 清空新增数据结构
    with blacklist_lock:
        blacklist.clear()
    with fail_count_lock:
        url_fail_count.clear()
    with alive_fail_lock:
        url_alive_fail_count.clear()
    domain_last_hit.clear()
    return '<script>alert("已停止并清空");window.location.href="/";</script>'


@app.route("/start")
def start_task():
    global running, start_time
    if not running:
        running = True; start_time = datetime.now()
        threading.Thread(target=_start_background, daemon=True).start()
    return "OK"

def _start_background():
    global executor, stats_timer, cleanup_timer, crawl_timer
    stats_timer = threading.Thread(target=stats_updater, daemon=True); stats_timer.start()
    if not cleanup_timer: cleanup_timer = threading.Thread(target=daily_cleanup, daemon=True); cleanup_timer.start()
    if not crawl_timer: crawl_timer = threading.Thread(target=auto_crawl_and_update, daemon=True); crawl_timer.start()
    threading.Thread(target=auto_cleanup_dead_links, daemon=True).start()
    threading.Thread(target=zero_traffic_cleanup, daemon=True).start()
    threading.Thread(target=thread_pool_manager, daemon=True).start()
    threading.Thread(target=alive_checker, daemon=True).start()
    threading.Thread(target=blacklist_recovery, daemon=True).start()
    threading.Thread(target=memory_cleanup, daemon=True).start()
    print("[启动] 冲刷任务已启动")

@app.route("/stop")
def stop_task():
    global running, executor
    running = False
    if executor: executor.shutdown(wait=False); executor = None
    print("[停止] 冲刷任务已停止")
    return "OK"

@app.route("/refreshnow")
def refresh_now():
    global speed_url_list, video_url_list, live_url_list
    _s = generate_speed_urls(5000)
    _v = generate_video_urls(2000)
    _l = generate_live_urls(500)
    with pool_lock:
        speed_url_list = _s
        video_url_list = _v
        live_url_list = _l
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
        url_pool.crawled_urls = crawled_urls
        _s = generate_speed_urls(5000)
        _v = generate_video_urls(2000)
        _l = generate_live_urls(500)
        with pool_lock:
            speed_url_list = _s
            video_url_list = _v
            live_url_list = _l
    with pool_lock:
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
    # 构建URL→省份反向索引
    build_url_province_map()
    app.run(host="0.0.0.0", port=9999, debug=False, threaded=True)
