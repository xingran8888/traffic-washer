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

def normalize_province(prov):
    """标准化省份名称，去掉'省'/'市'/'自治区'等后缀"""
    if not prov:
        return "未知"
    for suffix in ['壮族自治区', '回族自治区', '维吾尔自治区', '自治区', '特别行政区', '省', '市']:
        if prov.endswith(suffix):
            prov = prov[:-len(suffix)]
            break
    return prov

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
    # ===== 北京 (85条) =====
    "北京": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 900),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 900),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 800),
        ("https://mirrors6.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 800),
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 上海 (72条) =====
    "上海": [
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/centos/timestamp.txt", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/debian/dists/stable/Release", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 500),
        ("https://mirrors.sjtug.sjtu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/centos/timestamp.txt", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/debian/dists/stable/Release", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 400),
        ("https://mirrors6.sjtug.sjtu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 江苏 (85条) =====
    "江苏": [
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 700),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 700),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 700),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 700),
        ("https://mirrors6.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/centos/timestamp.txt", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/debian/dists/stable/Release", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 600),
        ("https://mirrors6.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 600),
        ("https://mirrors.njupt.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.njupt.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.njupt.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.njupt.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.njupt.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 江苏城市CDN(南京,无锡,徐州,常州,苏州等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 安徽 (72条) =====
    "安徽": [
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/centos/timestamp.txt", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/debian/dists/stable/Release", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 500),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors6.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/centos/timestamp.txt", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/debian/dists/stable/Release", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 400),
        ("https://mirrors6.ustc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 安徽城市CDN(合肥,芜湖,蚌埠,淮南,马鞍山等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 浙江 (59条) =====
    "浙江": [
        ("https://mirrors.zju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 500),
        ("https://mirrors.zju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/centos/timestamp.txt", "教育网", 500),
        ("https://mirrors.zju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/debian/dists/stable/Release", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 500),
        ("https://mirrors.zju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 500),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 浙江城市CDN(杭州,宁波,温州,嘉兴,湖州等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 广东 (72条) =====
    "广东": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 广东城市CDN(广州,深圳,珠海,汕头,佛山等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 重庆 (59条) =====
    "重庆": [
        ("https://mirrors.cqu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.cqu.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.cqu.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.cqu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.cqu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.cqu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.cqu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 四川 (59条) =====
    "四川": [
        ("https://mirrors.uestc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.uestc.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 四川城市CDN(成都,绵阳,自贡,攀枝花,泸州等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 辽宁 (72条) =====
    "辽宁": [
        ("https://mirrors.dlut.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.dlut.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.dlut.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.dlut.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.dlut.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.dlut.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.dlut.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.neu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirror.neu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.neu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.neu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.neu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirror.neu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirror.neu.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirror.neu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirror.neu.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirror.neu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirror.neu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirror.neu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirror.neu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 辽宁城市CDN(沈阳,大连,鞍山,抚顺,本溪等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 黑龙江 (59条) =====
    "黑龙江": [
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 黑龙江城市CDN(哈尔滨,齐齐哈尔,牡丹江,佳木斯,大庆等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 陕西 (59条) =====
    "陕西": [
        ("https://mirrors.xjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.xjtu.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 陕西城市CDN(西安,铜川,宝鸡,咸阳,渭南等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 湖北 (59条) =====
    "湖北": [
        ("https://mirrors.hust.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.hust.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.hust.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.hust.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.hust.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.hust.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.hust.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.hust.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.hust.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 湖北城市CDN(武汉,黄石,十堰,宜昌,襄阳等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 山东 (72条) =====
    "山东": [
        ("https://mirrors.qdu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.qdu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.qdu.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.qdu.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.qdu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.qdu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.qdu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.qdu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.sdwu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.sdwu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.sdwu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.sdwu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.sdwu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 150),
        ("https://mirrors.sdwu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.sdwu.edu.cn/centos/timestamp.txt", "教育网", 150),
        ("https://mirrors.sdwu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.sdwu.edu.cn/debian/dists/stable/Release", "教育网", 150),
        ("https://mirrors.sdwu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.sdwu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.sdwu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.sdwu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 150),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 山东城市CDN(济南,青岛,烟台,潍坊,临沂等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 福建 (59条) =====
    "福建": [
        ("https://mirrors.xmu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.xmu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.xmu.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.xmu.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.xmu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.xmu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.xmu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.xmu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 福建城市CDN(福州,厦门,泉州,莆田,漳州等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 甘肃 (59条) =====
    "甘肃": [
        ("https://mirror.lzu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirror.lzu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.lzu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.lzu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirror.lzu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirror.lzu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirror.lzu.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirror.lzu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirror.lzu.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirror.lzu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirror.lzu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirror.lzu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirror.lzu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 甘肃城市CDN(兰州,嘉峪关,金昌,白银,天水等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 贵州 (59条) =====
    "贵州": [
        ("https://mirrors.uestc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.uestc.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.uestc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.uestc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.uestc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.uestc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 贵州城市CDN(贵阳,六盘水,遵义,安顺,毕节等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 河北 (85条) =====
    "河北": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.ustc.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.ustc.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 city CDN nodes - 石家庄(阿里云), 唐山(腾讯云), 保定(华为云), 邯郸(网易), 张家口(搜狐), 承德/沧州/廊坊/衡水/秦皇岛/邢台(其他CDN)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.aliyun.com/debian-cd/12.5.0/amd64/iso-dvd/debian-12.5.0-amd64-DVD-1.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/20.04/ubuntu-20.04.6-desktop-amd64.iso", "电信", 350),
        ("https://mirrors.aliyun.com/rocky/9/BaseOS/x86_64/iso/Rocky-9.3-x86_64-dvd.iso", "电信", 400),
        ("https://mirrors.aliyun.com/almalinux/9/BaseOS/x86_64/iso/AlmaLinux-9.3-x86_64-dvd.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/20.04/ubuntu-20.04.6-desktop-amd64.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/fedora/linux/releases/40/Everything/x86_64/iso/Fedora-Everything-netinst-x86_64-40-1.14.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/debian-cd/12.5.0/amd64/iso-dvd/debian-12.5.0-amd64-DVD-1.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso", "联通", 350),
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/20.04/ubuntu-20.04.6-desktop-amd64.iso", "联通", 350),
        ("https://repo.huaweicloud.com/rocky/9/BaseOS/x86_64/iso/Rocky-9.3-x86_64-dvd.iso", "联通", 400),
        ("https://repo.huaweicloud.com/almalinux/9/BaseOS/x86_64/iso/AlmaLinux-9.3-x86_64-dvd.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso", "电信", 350),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso", "联通", 350),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 河南 (85条) =====
    "河南": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.ustc.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.ustc.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.ustc.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.ustc.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 河南城市CDN(郑州,洛阳,开封,南阳,许昌等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 湖南 (72条) =====
    "湖南": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 湖南城市CDN(长沙,株洲,湘潭,衡阳,邵阳等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.10010.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 江西 (72条) =====
    "江西": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.nju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.nju.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.nju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.nju.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 江西城市CDN(南昌,景德镇,萍乡,九江,新余等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 吉林 (72条) =====
    "吉林": [
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.hit.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 250),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 吉林城市CDN(长春,吉林,四平,通化,白山等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 山西 (72条) =====
    "山西": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.xjtu.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.xjtu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.xjtu.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 山西城市CDN(太原,大同,临汾,运城,长治等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 云南 (59条) =====
    "云南": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
            # v5.3 云南城市CDN(昆明,曲靖,玉溪,保山,昭通等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 广西 (59条) =====
    "广西": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 广西城市CDN(南宁,柳州,桂林,梧州,北海等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 海南 (59条) =====
    "海南": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 海南城市CDN(海口,三亚,三沙,儋州等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 内蒙古 (59条) =====
    "内蒙古": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 内蒙古城市CDN(呼和浩特,包头,乌海,赤峰,通辽等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 宁夏 (59条) =====
    "宁夏": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 宁夏城市CDN(银川,石嘴山,吴忠,固原,中卫等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 青海 (59条) =====
    "青海": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 青海城市CDN(西宁,海东,海北,黄南,海南等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 西藏 (59条) =====
    "西藏": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 西藏城市CDN(拉萨,日喀则,昌都,林芝,山南等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 新疆 (59条) =====
    "新疆": [
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 300),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "教育网", 300),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Release", "电信", 200),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 200),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "电信", 300),
        ("https://mirrors.aliyun.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 250),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "电信", 200),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 200),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 150),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 150),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/AppOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 250),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-DVD-1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz", "联通", 150),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 80),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 80),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("h/PLTV/88888888/224/3221226016/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225588/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221227166/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225548/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225800/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225802/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225804/index.m3u8", "移动", 200),
        ("h/PLTV/88888888/224/3221225806/index.m3u8", "移动", 200),
        ("h/iptv.m3u", "移动", 100),
    
        # v5.3 城市级CDN节点
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        # v5.3 新疆城市CDN(乌鲁木齐,克拉玛依,吐鲁番,哈密,昌吉等)
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 天津 =====  # 56条
    "天津": [
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 250),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 50),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/7c/25744/BaiduNetdisk_7.44.2.5.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 250),
        ("https://cdn.mysql.com/Downloads/MySQL-8.4/mysql-8.4.0-linux-glibc2.17-x86_64.tar.xz", "电信", 250),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://releases.ubuntu.com/22.04/ubuntu-22.04.4-desktop-amd64.iso", "电信", 250),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 250),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 100),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 50),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 350),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 50),
        ("https://mirrors.huaweicloud.com/repository/rpm/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://repo.huaweicloud.com/euler/2.4/os/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1.qq.com/qqpc/qq_9.9.12_250610_x64_01.exe", "联通", 200),
        ("https://dldir1.qq.com/qqgame/2024/QQGame_2024_Setup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("https://registry.npmmirror.com/-/binary/node/latest/v20.19.2/SHASUMS256.txt", "联通", 100),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221226016/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225588/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221227166/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225548/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225800/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225802/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225804/index.m3u8", "移动", 150),
        ("http://ottrrs.tj.chinamobile.com/PLTV/88888888/224/3221225806/index.m3u8", "移动", 150),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 300),
        ("https://mirrors.huaweicloud.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "移动", 200),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "移动", 250),
        ("https://mirrors.huaweicloud.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "移动", 200),
        ("https://mirrors.huaweicloud.com/debian/dists/stable/Release", "移动", 80),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 250),
        ("https://mirrors.tju.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.tju.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tju.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tju.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tju.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors.tju.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 350),
        ("https://mirrors.tju.edu.cn/debian/dists/stable/Release", "教育网", 100),
        ("https://mirrors.tju.edu.cn/centos/timestamp.txt", "教育网", 50),
            # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
    # ===== 台湾 =====  # 56条
    "台湾": [
        ("https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "电信", 300),
        ("https://mirrors.aliyun.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "电信", 250),
        ("https://mirrors.aliyun.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 350),
        ("https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "电信", 300),
        ("https://mirrors.aliyun.com/debian/dists/stable/Release", "电信", 100),
        ("https://mirrors.aliyun.com/centos/timestamp.txt", "电信", 50),
        ("https://dlsw.baidu.com/sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", "电信", 200),
        ("https://dlsw.baidu.com/sw-search-sp/soft/7c/25744/BaiduNetdisk_7.44.2.5.exe", "电信", 200),
        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 250),
        ("https://cdn.mysql.com/Downloads/MySQL-8.4/mysql-8.4.0-linux-glibc2.17-x86_64.tar.xz", "电信", 250),
        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),
        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),
        ("https://releases.ubuntu.com/22.04/ubuntu-22.04.4-desktop-amd64.iso", "电信", 250),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "联通", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "联通", 300),
        ("https://mirrors.cloud.tencent.com/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "联通", 250),
        ("https://mirrors.cloud.tencent.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 350),
        ("https://mirrors.cloud.tencent.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "联通", 300),
        ("https://mirrors.cloud.tencent.com/debian/dists/stable/Release", "联通", 100),
        ("https://mirrors.cloud.tencent.com/centos/timestamp.txt", "联通", 50),
        ("https://mirrors.cloud.tencent.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", "联通", 350),
        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 50),
        ("https://mirrors.huaweicloud.com/repository/rpm/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://repo.huaweicloud.com/euler/2.4/os/x86_64/repodata/repomd.xml", "联通", 50),
        ("https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://dldir1.qq.com/qqpc/qq_9.9.12_250610_x64_01.exe", "联通", 200),
        ("https://dldir1.qq.com/qqgame/2024/QQGame_2024_Setup.exe", "联通", 200),
        ("https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe", "联通", 200),
        ("https://registry.npmmirror.com/-/binary/node/latest/v22.21.1/SHASUMS256.txt", "联通", 100),
        ("https://registry.npmmirror.com/-/binary/node/latest/v20.19.2/SHASUMS256.txt", "联通", 100),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221226016/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225588/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221227166/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225548/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225800/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225802/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225804/index.m3u8", "移动", 150),
        ("http://ottrrs.tw.chinamobile.com/PLTV/88888888/224/3221225806/index.m3u8", "移动", 150),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 300),
        ("https://mirrors.huaweicloud.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "移动", 200),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "移动", 250),
        ("https://mirrors.huaweicloud.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "移动", 200),
        ("https://mirrors.huaweicloud.com/debian/dists/stable/Release", "移动", 80),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 250),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "教育网", 500),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", "教育网", 200),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "教育网", 400),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", "教育网", 350),
        ("https://mirrors.tuna.tsinghua.edu.cn/debian/dists/stable/Release", "教育网", 100),
        ("https://mirrors.tuna.tsinghua.edu.cn/centos/timestamp.txt", "教育网", 50),
            # v5.3 通用系测速节点(百度/阿里/华为/小米/Vivo)
        ("https://gw.alipayobjects.com/os/volans-demo/f44c302e-b704-4a70-bcc6-0214e37ca256/MiniProgramStudio-1.17.4.exe", "电信", 400),
        ("https://gw.alipayobjects.com/os/rmsportal/PpisHyUkzJnZltrPyfuD.zip", "电信", 400),
        ("https://issuepcdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.30.5.2.exe", "电信", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/cn/mkt/mobileservices/appgallery/download/PC107f1b3947c942ffaa14334a879065d8.2107261020.exe", "联通", 400),
        ("https://cdn.cnbj1.fds.api.mi-img.com/miui-13/phone/index_pc_1227.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        # v5.3 移动系测速节点(联通/沃音乐)
        ("https://gec.10010.com/multi/unified-storage/video/202102/210208115fe139453000.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20220318/1504671809851760642.mp4", "移动", 400),
        # v5.3 天翼系测速节点(天翼云)
        ("https://vod-origin-rjzy.gdoss.xstore.ctyun.cn/07da9eb52ad948c7b58b760003c0006b.mp4", "电信", 400),
        # v5.3 其他系测速节点(淘宝/抖音/CacheFly)
        ("https://cloud.video.taobao.com/play/u/null/p/1/e/6/t/1/d/ud/329682839911.mp4", "电信", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/10715336/2.6.0/win32-ia32/douyin-v2.6.0-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/200mb.test", "联通", 400),
        # v5.3 全球高速测速节点(Linode/Vultr/Apple)
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lax-ca-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://lon-gb-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://ams-nl-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://sel-kor-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://hnd-jp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://syd-au-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://www.apple.com/105/media/us/tv-home/2022/4447b88b-1a33-4bb3-98a1-61d8949e1098/anim/sizzle/large_2x.mp4", "联通", 400),
        # v5.3 llxhq.aozio.cn测速节点
        # 通用系(网易/Vivo/抖音/Bilibili/华为/锤子/先牛)
        ("https://n.v.netease.com/2022/1206/de4b6add85f1537da839bdb5a501253d.mp4", "电信", 400),
        ("https://nsh.gdl.netease.com/NGP/NGP_NSH_2.0.81143.exe", "电信", 400),
        ("https://x19.gdl.netease.com/MCLauncher_1.10.0.15222.exe", "电信", 400),
        ("https://mov.bn.netease.com/open-movie/nos/mp4/2015/11/26/SB8ECV1ST_sd.mp4", "电信", 400),
        ("https://wwwstatic.vivo.com.cn/vivoportal/files/resource/funtouch/1651200648928/images/os2-jude-video.mp4", "联通", 400),
        ("https://www.douyin.com/download/pc/obj/douyin-pc-client/7044145585217083655/releases/11259813/3.0.1/win32-ia32/douyin-downloader-v3.0.1-win32-ia32-douyinDownload1.exe", "电信", 400),
        ("https://activity.hdslb.com/blackboard/static/20210604/4d40bc4f98f94fbc71c235832ce3efd4/hJEhL6jGOY.zip", "联通", 400),
        ("https://consumer-img.huawei.com/content/dam/huawei-cbg-site/common/mkt/pdp/phones/p60-pro/images/camera/huawei-p60-pro-camera-ui.mp4", "联通", 400),
        ("https://static.smartisanos.cn/common/video/production/ocean/os-1-1710.mp4", "联通", 400),
        ("https://picture.xianniu.com/pc/download/4.6.9.3/xianniusetup.4.6.9.3.exe", "电信", 400),
        # 移动系(联通沃音乐)
        ("https://listen.10155.com/listener/womusic-bucket/90115000/mv_vod/volte_mp4/20230215/1625752132487675906.mp4", "移动", 400),
        # 其他系(爱奇艺/米谷游戏)
        ("https://bdcdncnc.inter.71edge.com/cdn/pca/20231130/10.9.1.7348/channel/1701328986348/IQIYIsetup_z43.exe", "电信", 400),
        ("https://pc-dl.migufun.com:8443/channelpackage/mgame-2djSBy.exe", "电信", 400),
        ("https://freeserver.migufun.com/resource/beta/video/system/20210924112351666.mp4", "电信", 400),
        # 全球高速(CacheFly/Vultr/Linode)
        ("https://cachefly.cachefly.net/50mb.test", "联通", 400),
        ("https://cachefly.cachefly.net/100mb.test", "联通", 400),
        ("https://sgp-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://nj-us-ping.vultr.com/vultr.com.1000MB.bin", "联通", 400),
        ("https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin", "联通", 400),
        # v5.4 新增CDN节点(华为云6.3GB/网易/搜狐/天翼云/移动CDN)
        ("https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "电信", 500),
        ("https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", "联通", 500),
        ("https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.163.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://mirrors.sohu.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "联通", 400),
        ("https://mirrors.ctyun.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 400),
        ("https://p.cdn.10086.cn/d/NDY1/104.18.226.226_0_92489405505/2c94811f95a436680195b0106f780029.zip", "移动", 300),
],
}


# 所有省份列表
ALL_PROVINCES = list(PROVINCIAL_MIRRORS.keys())

# ====================== 链接池 ======================
speed_url_list = []
video_url_list = []
live_url_list = []
crawled_urls = []

# ====================== v5.4 线程安全与新增机制 ======================
pool_lock = threading.Lock()              # 保护 speed_url_list, video_url_list, live_url_list, crawled_urls
blacklist_lock = threading.Lock()          # 保护 blacklist
fail_count_lock = threading.Lock()         # 保护 url_fail_count
traffic_lock = threading.Lock()            # 保护 url_session_traffic, url_daily_traffic
connections_lock = threading.Lock()        # 保护 active_connections
domain_last_hit = {}                       # 域名冷却: {domain: last_hit_timestamp}
DOMAIN_COOLDOWN_S = 0.2                    # v5.3: 极速轮转
url_alive_fail_count = {}                  # 存活校验连续失败计数 {url: fail_count}
alive_fail_lock = threading.Lock()         # 保护 url_alive_fail_count

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
PROVINCE_ISP_MAP = {
    "mirrors.tuna.tsinghua.edu.cn": ("北京", "教育网"),
    "mirrors6.tuna.tsinghua.edu.cn": ("北京", "教育网"),
    "mirrors.cernet.edu.cn": ("北京", "教育网"),
    "mirrors.aliyun.com": ("杭州", "电信"),
    "mirrors.huaweicloud.com": ("东莞", "联通"),
    "repo.huaweicloud.com": ("东莞", "联通"),
    "mirrors.cloud.tencent.com": ("广州", "联通"),
    "mirrors.nju.edu.cn": ("南京", "教育网"),
    "mirrors6.nju.edu.cn": ("南京", "教育网"),
    "mirrors.njupt.edu.cn": ("南京", "教育网"),
    "mirrors.ustc.edu.cn": ("合肥", "教育网"),
    "mirrors6.ustc.edu.cn": ("合肥", "教育网"),
    "mirrors.sjtug.sjtu.edu.cn": ("上海", "教育网"),
    "mirrors6.sjtug.sjtu.edu.cn": ("上海", "教育网"),
    "mirrors.hit.edu.cn": ("哈尔滨", "教育网"),
    "mirrors.tju.edu.cn": ("天津", "教育网"),
    "mirrors.dlut.edu.cn": ("大连", "教育网"),
    "mirrors.cqu.edu.cn": ("重庆", "教育网"),
    "mirrors.zju.edu.cn": ("杭州", "教育网"),
    "mirrors.hust.edu.cn": ("武汉", "教育网"),
    "mirrors.xjtu.edu.cn": ("西安", "教育网"),
    "mirror.neu.edu.cn": ("沈阳", "教育网"),
    "mirrors.qdu.edu.cn": ("青岛", "教育网"),
    "mirrors.sdwu.edu.cn": ("济南", "教育网"),
    "mirrors.xmu.edu.cn": ("厦门", "教育网"),
    "mirror.lzu.edu.cn": ("兰州", "教育网"),
    "mirrors.uestc.edu.cn": ("成都", "教育网"),
    "mirrors.zzu.edu.cn": ("郑州", "教育网"),
    "mirrors.csu.edu.cn": ("长沙", "教育网"),
    "mirrors.jlu.edu.cn": ("长春", "教育网"),
    "dlsw.baidu.com": ("北京", "电信"),
    "gdown.baidu.com": ("北京", "电信"),
    "dldir1.qq.com": ("深圳", "联通"),
    "dldir1v6.qq.com": ("深圳", "联通"),
    "registry.npmmirror.com": ("杭州", "联通"),
    "cdn.mysql.com": ("全球", "电信"),
    "download.visualstudio.microsoft.com": ("全球", "电信"),
    "releases.ubuntu.com": ("全球", "电信"),
    "ottrrs.bj.chinamobile.com": ("北京", "移动"),
    "ottrrs.sh.chinamobile.com": ("上海", "移动"),
    "ottrrs.js.chinamobile.com": ("江苏", "移动"),
    "ottrrs.zj.chinamobile.com": ("浙江", "移动"),
    "ottrrs.gd.chinamobile.com": ("广东", "移动"),
    "ottrrs.sd.chinamobile.com": ("山东", "移动"),
    "ottrrs.ha.chinamobile.com": ("河南", "移动"),
    "ottrrs.sc.chinamobile.com": ("四川", "移动"),
    "ottrrs.hb.chinamobile.com": ("湖北", "移动"),
    "ottrrs.hn.chinamobile.com": ("湖南", "移动"),
    "ottrrs.he.chinamobile.com": ("河北", "移动"),
    "ottrrs.ah.chinamobile.com": ("安徽", "移动"),
    "ottrrs.ln.chinamobile.com": ("辽宁", "移动"),
    "ottrrs.fj.chinamobile.com": ("福建", "移动"),
    "ottrrs.jx.chinamobile.com": ("江西", "移动"),
    "ottrrs.cq.chinamobile.com": ("重庆", "移动"),
    "ottrrs.tj.chinamobile.com": ("天津", "移动"),
    "ottrrs.sn.chinamobile.com": ("陕西", "移动"),
    "ottrrs.gx.chinamobile.com": ("广西", "移动"),
    "ottrrs.yn.chinamobile.com": ("云南", "移动"),
    "ottrrs.hl.chinamobile.com": ("黑龙江", "移动"),
    "ottrrs.jl.chinamobile.com": ("吉林", "移动"),
    "ottrrs.gz.chinamobile.com": ("贵州", "移动"),
    "ottrrs.sx.chinamobile.com": ("山西", "移动"),
    "ottrrs.gs.chinamobile.com": ("甘肃", "移动"),
    "ottrrs.nm.chinamobile.com": ("内蒙古", "移动"),
    "ottrrs.xj.chinamobile.com": ("新疆", "移动"),
    "ottrrs.hi.chinamobile.com": ("海南", "移动"),
    "ottrrs.nx.chinamobile.com": ("宁夏", "移动"),
    "ottrrs.qh.chinamobile.com": ("青海", "移动"),
    "ottrrs.xz.chinamobile.com": ("西藏", "移动"),
    "ottrrs.tw.chinamobile.com": ("台湾", "移动"),
}

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
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=1)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=1)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    adapter = requests.adapters.HTTPAdapter(pool_connections=3, pool_maxsize=3, max_retries=1)
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
            resp = session.get(target_url, stream=True, timeout=timeout_s, headers=OPTIMIZED_HEADERS)
            resp.raise_for_status()
            last_recv_time = time.time(); got_data = False
            try:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not running:
                        resp.close()
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
                        with connections_lock:
                            active_connections[tid]["bytes"] = conn_bytes
                            _now = time.time()
                            _dt = _now - active_connections[tid].get("speed_ts", _now)
                            if _dt >= 1.0:
                                _delta = conn_bytes - active_connections[tid].get("last_bytes", 0)
                                active_connections[tid]["speed_bps"] = _delta * 8 / _dt
                                active_connections[tid]["last_bytes"] = conn_bytes
                                active_connections[tid]["speed_ts"] = _now
                        last_recv_time = time.time()
                    elif got_data:
                        if time.time() - last_recv_time >= stall_timeout_s:
                            break
            finally:
                resp.close()  # v5.4: 确保连接释放
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
            # v5.4: chunk 局部变量在循环内已释放，连接结束后触发 GC
            try:
                del resp
            except Exception:
                pass
            gc.collect()
            time.sleep(req_delay_ms / 1000)

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
                kept = {k: v for k, v in url_session_traffic.items() if v > 0}
                url_session_traffic.clear()
                url_session_traffic.update(kept)

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
      <div class="info-row"><span class="info-label">版本号</span><span class="info-value">v5.4</span></div>
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
