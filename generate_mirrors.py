#!/usr/bin/env python3
"""Generate expanded PROVINCIAL_MIRRORS for Traffic Washer v5.1"""

# File path variants (different sizes for varied traffic patterns)
TELECOM_PATHS = [
    ("ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", 400),          # ~5GB
    ("ubuntu/dists/noble/main/binary-amd64/Packages.xz", 300),                # ~30MB
    ("ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", 300),        # ~15MB
    ("ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", 250),       # ~10MB
    ("centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", 350),  # ~10GB
    ("debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", 300), # ~650MB
    ("debian/dists/stable/Release", 100),                                       # ~150KB
    ("centos/timestamp.txt", 50),                                               # tiny
]

UNICOM_PATHS = [
    ("ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", 400),
    ("ubuntu/dists/noble/main/binary-amd64/Packages.xz", 300),
    ("ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", 300),
    ("ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", 250),
    ("centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", 350),
    ("debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", 300),
    ("debian/dists/stable/Release", 100),
    ("centos/timestamp.txt", 50),
    # Unicom-specific extra paths
    ("ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso", 350),       # ~2.6GB server ISO
]

MOBILE_PATHS = [
    ("ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", 300),
    ("ubuntu/dists/noble/main/binary-amd64/Packages.xz", 200),
    ("centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", 250),
    ("debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", 200),
    ("debian/dists/stable/Release", 80),
]

EDU_PATHS = [
    ("ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", 500),
    ("ubuntu/dists/noble/main/binary-amd64/Packages.xz", 200),
    ("ubuntu/dists/noble-updates/main/binary-amd64/Packages.xz", 200),
    ("ubuntu/dists/noble-security/main/binary-amd64/Packages.xz", 200),
    ("centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso", 400),
    ("debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso", 350),
    ("debian/dists/stable/Release", 100),
    ("centos/timestamp.txt", 50),
]

# CDN hosts per ISP
TELECOM_HOSTS = [
    "https://mirrors.aliyun.com",
    "https://dlsw.baidu.com",
    "https://cdn.mysql.com",
    "https://download.visualstudio.microsoft.com",
    "https://releases.ubuntu.com",
]

UNICOM_HOSTS = [
    "https://mirrors.cloud.tencent.com",
    "https://mirrors.huaweicloud.com",
    "https://repo.huaweicloud.com",
    "https://dldir1.qq.com",
    "https://dldir1v6.qq.com",
    "https://registry.npmmirror.com",
]

# Baidu-specific paths (different from standard mirrors)
BAIDU_PATHS = [
    ("sw-search-sp/soft/9a/25744/WPSOffice_11.1.0.15220.exe", 200),
    ("sw-search-sp/soft/7c/25744/BaiduNetdisk_7.44.2.5.exe", 200),
]

# MySQL CDN paths
MYSQL_PATHS = [
    ("Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", 250),
    ("Downloads/MySQL-8.4/mysql-8.4.0-linux-glibc2.17-x86_64.tar.xz", 250),
]

# VS CDN paths
VS_PATHS = [
    ("download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", 200),
]

# Ubuntu releases (global)
UBUNTU_RELEASES_PATHS = [
    ("24.04/ubuntu-24.04.4-desktop-amd64.iso", 300),
    ("22.04/ubuntu-22.04.4-desktop-amd64.iso", 250),
]

# Huawei Cloud specific paths
HUAWEI_PATHS = [
    ("repository/conf/repomd.xml", 50),
    ("repository/rpm/x86_64/repodata/repomd.xml", 50),
]

# Huawei repo paths
HUAWEI_REPO_PATHS = [
    ("euler/2.3/os/x86_64/repodata/repomd.xml", 50),
    ("euler/2.4/os/x86_64/repodata/repomd.xml", 50),
]

# QQ download paths
QQ_PATHS = [
    ("weixin/Windows/WeChatSetup.exe", 200),
    ("qqpc/qq_9.9.12_250610_x64_01.exe", 200),
    ("qqgame/2024/QQGame_2024_Setup.exe", 200),
]

# npmmirror paths
NPMMIRROR_PATHS = [
    ("-/binary/node/latest/v22.21.1/SHASUMS256.txt", 100),
    ("-/binary/node/latest/v20.19.2/SHASUMS256.txt", 100),
]

# China Mobile IPTV CDN nodes by province
MOBILE_IPTV_NODES = {
    "北京": "ottrrs.bj.chinamobile.com",
    "上海": "ottrrs.sh.chinamobile.com",
    "江苏": "ottrrs.js.chinamobile.com",
    "浙江": "ottrrs.zj.chinamobile.com",
    "广东": "ottrrs.gd.chinamobile.com",
    "山东": "ottrrs.sd.chinamobile.com",
    "河南": "ottrrs.ha.chinamobile.com",
    "四川": "ottrrs.sc.chinamobile.com",
    "湖北": "ottrrs.hb.chinamobile.com",
    "湖南": "ottrrs.hn.chinamobile.com",
    "河北": "ottrrs.he.chinamobile.com",
    "安徽": "ottrrs.ah.chinamobile.com",
    "辽宁": "ottrrs.ln.chinamobile.com",
    "福建": "ottrrs.fj.chinamobile.com",
    "江西": "ottrrs.jx.chinamobile.com",
    "重庆": "ottrrs.cq.chinamobile.com",
    "天津": "ottrrs.tj.chinamobile.com",
    "陕西": "ottrrs.sn.chinamobile.com",
    "广西": "ottrrs.gx.chinamobile.com",
    "云南": "ottrrs.yn.chinamobile.com",
    "黑龙江": "ottrrs.hl.chinamobile.com",
    "吉林": "ottrrs.jl.chinamobile.com",
    "贵州": "ottrrs.gz.chinamobile.com",
    "山西": "ottrrs.sx.chinamobile.com",
    "甘肃": "ottrrs.gs.chinamobile.com",
    "内蒙古": "ottrrs.nm.chinamobile.com",
    "新疆": "ottrrs.xj.chinamobile.com",
    "海南": "ottrrs.hi.chinamobile.com",
    "宁夏": "ottrrs.nx.chinamobile.com",
    "青海": "ottrrs.qh.chinamobile.com",
    "西藏": "ottrrs.xz.chinamobile.com",
}

# IPTV channel IDs (each generates a streaming URL)
IPTV_CHANNELS = [
    ("3221226016", "CCTV1"),
    ("3221225588", "CCTV2"),
    ("3221227166", "CCTV5"),
    ("3221225548", "CCTV6"),
    ("3221225800", "Hunan"),
    ("3221225802", "Zhejiang"),
    ("3221225804", "Jiangsu"),
    ("3221225806", "Dongfang"),
]

# Educational mirror sites by region (shared for provinces without local ones)
EDU_MIRRORS = {
    "北京": ["mirrors.tuna.tsinghua.edu.cn", "mirrors6.tuna.tsinghua.edu.cn", "mirrors.cernet.edu.cn"],
    "上海": ["mirrors.sjtug.sjtu.edu.cn", "mirrors6.sjtug.sjtu.edu.cn"],
    "江苏": ["mirrors.nju.edu.cn", "mirrors6.nju.edu.cn", "mirrors.njupt.edu.cn"],
    "安徽": ["mirrors.ustc.edu.cn", "mirrors6.ustc.edu.cn"],
    "浙江": ["mirrors.zju.edu.cn"],
    "湖北": ["mirrors.hust.edu.cn"],
    "陕西": ["mirrors.xjtu.edu.cn"],
    "辽宁": ["mirrors.dlut.edu.cn", "mirror.neu.edu.cn"],
    "黑龙江": ["mirrors.hit.edu.cn"],
    "重庆": ["mirrors.cqu.edu.cn"],
    "山东": ["mirrors.qdu.edu.cn", "mirrors.sdwu.edu.cn"],
    "福建": ["mirrors.xmu.edu.cn"],
    "甘肃": ["mirror.lzu.edu.cn"],
    "贵州": ["mirrors.uestc.edu.cn"],
    "四川": ["mirrors.uestc.edu.cn"],
    "广东": ["mirrors.zzu.edu.cn"],
    "湖南": ["mirrors.csu.edu.cn"],
    "吉林": ["mirrors.jlu.edu.cn"],
}

# Nearby edu mirror assignment (for provinces without local ones)
EDU_NEARBY = {
    "河北": "北京", "天津": "北京", "山西": "陕西", "内蒙古": "黑龙江",
    "河南": "湖北", "江西": "安徽", "广西": "广东", "海南": "广东",
    "云南": "贵州", "宁夏": "甘肃", "青海": "甘肃", "新疆": "陕西",
    "西藏": "四川",
}

def gen_telecom_urls():
    """Generate telecom (电信) URLs using standard CDN hosts and paths"""
    urls = []
    # Aliyun mirrors (standard paths)
    for path, weight in TELECOM_PATHS:
        urls.append((f"https://mirrors.aliyun.com/{path}", "电信", weight))
    # Baidu (specific paths)
    for path, weight in BAIDU_PATHS:
        urls.append((f"https://dlsw.baidu.com/{path}", "电信", weight))
    # MySQL CDN
    for path, weight in MYSQL_PATHS:
        urls.append((f"https://cdn.mysql.com/{path}", "电信", weight))
    # VS CDN
    for path, weight in VS_PATHS:
        urls.append((f"https://download.visualstudio.microsoft.com/{path}", "电信", weight))
    # Ubuntu releases (global)
    for path, weight in UBUNTU_RELEASES_PATHS:
        urls.append((f"https://releases.ubuntu.com/{path}", "电信", weight))
    return urls

def gen_unicom_urls():
    """Generate unicom (联通) URLs"""
    urls = []
    # Tencent Cloud mirrors
    for path, weight in UNICOM_PATHS:
        urls.append((f"https://mirrors.cloud.tencent.com/{path}", "联通", weight))
    # Huawei Cloud
    for path, weight in HUAWEI_PATHS:
        urls.append((f"https://mirrors.huaweicloud.com/{path}", "联通", weight))
    # Huawei repo
    for path, weight in HUAWEI_REPO_PATHS:
        urls.append((f"https://repo.huaweicloud.com/{path}", "联通", weight))
    # QQ downloads
    for path, weight in QQ_PATHS:
        urls.append((f"https://dldir1.qq.com/{path}", "联通", weight))
    # QQ IPv6
    for path, weight in QQ_PATHS[:1]:  # Just WeChat for IPv6
        urls.append((f"https://dldir1v6.qq.com/{path}", "联通", weight))
    # npmmirror
    for path, weight in NPMMIRROR_PATHS:
        urls.append((f"https://registry.npmmirror.com/{path}", "联通", weight))
    return urls

def gen_mobile_urls(province):
    """Generate mobile (移动) URLs for a given province"""
    urls = []
    # IPTV CDN nodes
    node = MOBILE_IPTV_NODES.get(province, "ottrrs.hl.chinamobile.com")
    for ch_id, ch_name in IPTV_CHANNELS:
        urls.append((f"http://{node}/PLTV/88888888/224/{ch_id}/index.m3u8", "移动", 150))
    # Additional mobile-friendly CDN (Huawei Cloud partial mobile access)
    for path, weight in MOBILE_PATHS:
        urls.append((f"https://mirrors.huaweicloud.com/{path}", "移动", weight))
    # General large file downloads accessible via mobile
    urls.append(("https://mirrors.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso", "移动", 250))
    return urls

def gen_edu_urls(province):
    """Generate education network (教育网) URLs for a given province"""
    urls = []
    # Find edu mirrors for this province or nearby
    edu_hosts = []
    if province in EDU_MIRRORS:
        edu_hosts = EDU_MIRRORS[province]
    elif province in EDU_NEARBY:
        nearby = EDU_NEARBY[province]
        if nearby in EDU_MIRRORS:
            edu_hosts = EDU_MIRRORS[nearby][:2]  # Use 2 mirrors from nearby
    else:
        # Default to Tsinghua (nationally accessible)
        edu_hosts = ["mirrors.tuna.tsinghua.edu.cn"]
    
    for host in edu_hosts:
        for path, weight in EDU_PATHS:
            urls.append((f"https://{host}/{path}", "教育网", weight))
    return urls

# Generate the full dictionary
provinces = [
    "北京", "上海", "天津", "重庆",
    "河北", "山西", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "海南",
    "四川", "贵州", "云南", "陕西", "甘肃", "青海",
    "台湾", "内蒙古", "广西", "西藏", "宁夏", "新疆",
]

# Province comments
province_comments = {
    "北京": "# ===== 北京 =====",
    "上海": "# ===== 上海 =====",
    "天津": "# ===== 天津 =====",
    "重庆": "# ===== 重庆 =====",
    "河北": "# ===== 河北 =====",
    "山西": "# ===== 山西 =====",
    "辽宁": "# ===== 辽宁 =====",
    "吉林": "# ===== 吉林 =====",
    "黑龙江": "# ===== 黑龙江 =====",
    "江苏": "# ===== 江苏 =====",
    "浙江": "# ===== 浙江 =====",
    "安徽": "# ===== 安徽 =====",
    "福建": "# ===== 福建 =====",
    "江西": "# ===== 江西 =====",
    "山东": "# ===== 山东 =====",
    "河南": "# ===== 河南 =====",
    "湖北": "# ===== 湖北 =====",
    "湖南": "# ===== 湖南 =====",
    "广东": "# ===== 广东 =====",
    "海南": "# ===== 海南 =====",
    "四川": "# ===== 四川 =====",
    "贵州": "# ===== 贵州 =====",
    "云南": "# ===== 云南 =====",
    "陕西": "# ===== 陕西 =====",
    "甘肃": "# ===== 甘肃 =====",
    "青海": "# ===== 青海 =====",
    "台湾": "# ===== 台湾 =====",
    "内蒙古": "# ===== 内蒙古 =====",
    "广西": "# ===== 广西 =====",
    "西藏": "# ===== 西藏 =====",
    "宁夏": "# ===== 宁夏 =====",
    "新疆": "# ===== 新疆 =====",
}

print("# GENERATED - paste into app.py")
print("PROVINCIAL_MIRRORS = {")

total_urls = 0
for prov in provinces:
    telecom = gen_telecom_urls()
    unicom = gen_unicom_urls()
    mobile = gen_mobile_urls(prov)
    edu = gen_edu_urls(prov)
    
    all_urls = telecom + unicom + mobile + edu
    count = len(all_urls)
    total_urls += count
    
    comment = province_comments.get(prov, f"# ===== {prov} =====")
    print(f"    {comment}  # {count}条")
    print(f'    "{prov}": [')
    for url, isp, weight in all_urls:
        print(f'        ("{url}", "{isp}", {weight}),')
    print("    ],")

# 全国通用
print(f"    # ===== 全国通用（不限省份）=====")
print(f'    "全国": [')
print(f'        ("https://releases.ubuntu.com/24.04/ubuntu-24.04.4-desktop-amd64.iso", "电信", 300),')
print(f'        ("https://cdn.mysql.com/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.17-x86_64.tar.xz", "电信", 200),')
print(f'        ("https://download.visualstudio.microsoft.com/download/pr/visual-studio-community-offline/17.10.0/vs_Community.exe", "电信", 200),')
print(f'        ("https://mirrors.huaweicloud.com/repository/conf/repomd.xml", "联通", 50),')
print(f'        ("https://repo.huaweicloud.com/euler/2.3/os/x86_64/repodata/repomd.xml", "联通", 50),')
print("    ],")
print("}")
print(f"\n# Total: {total_urls + 5} URLs across {len(provinces) + 1} regions")
print(f"# Per province avg: {total_urls // len(provinces)} URLs")
