"""v5.6: 测试热门APP/游戏CDN可达性"""
import urllib.request, ssl, socket, concurrent.futures, time
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
socket.setdefaulttimeout(10)

URLS = [
 # 腾讯系
 "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe",
 "https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe",
 "https://dldir1.qq.com/qqfile/qq/QQNT/Windows/QQ_9.9.15_250610_x64_01.exe",
 "https://dldir1v6.qq.com/qqfile/qq/QQNT/Windows/QQ_9.9.15_250610_x64_01.exe",
 "https://dldir1.qq.com/wemeet/WemeetClient_14.14.1.332_x64.exe",
 "https://dldir1.qq.com/qqpcmgr/PCSetup2024.exe",
 "https://dldir1.qq.com/wegame/WeGameMiniLoader_5.7.6.exe",
 "https://dldir1.qq.com/music/clntupate/QQMusic_Setup_20.18.exe",
 "https://dldir1.qq.com/qqdoc/tim/TIM3.4.8.22138_0x6c9d0c27.exe",
 "https://dldir1.qq.com/wework/work_weixin/WeCom_4.1.36.6038.exe",
 "https://dldir1.qq.com/weiyun/update/Weiyun_5.2.1538.exe",
 "https://dldir1.qq.com/txgameassistant/GameAssistant_147302_Full.exe",
 "https://dl.softmgr.qq.com/im/pc/QQ_music/QQMusic_Setup_20.18.0.0.exe",
 # 网易系
 "https://mirrors.163.com/.help/CentOS9-Base-163.repo",
 "https://uu.gdl.netease.com/uu-gdl/UU-Netease-Setup.exe",
 "https://fm.dl.126.net/jingle/client/neteasemusic_3.0.1.207372_64.exe",
 # 米哈游
 "https://autopatchcn.yuanshen.com/client_app/store/18/0ade34dd8a7e46eeb7bdb2e4aa25cf3c_18",
 "https://autopatchcn.bh3.com/client_app/store/29/bh3_cn_29",
 # Steam/Epic
 "https://cdn.akamai.steamstatic.com/client/installer/SteamSetup.exe",
 "https://cdn1.epicgames.com/EpicGamesLauncher/Installer/EpicInstaller-16.6.1.msi",
 # 字节/抖音
 "https://lf1-cdn-tos.bytegoofy.com/obj/ttfe/tiktok_pc/TikTok_Setup_v35.3.3.exe",
 # 钉钉/阿里
 "https://dl.dingtalk.com/dingtalk-desktop/DingTalk_v7.6.25.4139001.exe",
 # 百度网盘
 "https://d.pcs.baidu.com/file/BaiduNetdisk_7.45.1.3.exe",
 # 360
 "https://down.360safe.com/360/inst.exe",
 "https://down.360safe.com/se/360se16.1.1168.64.exe",
 # 迅雷
 "https://down.sandai.net/thunder11/XunLeiSetup_11.4.6.2080.exe",
 # WPS
 "https://wpsdl.wps.cn/wps/download/ep/WPS2019_11.1.0.15320.exe",
 # 战网
 "https://webstatic.battlenet.com.cn/bnet-client/Battle.net-CN-setup.exe",
 # B站
 "https://dl.hdslb.com/client/bili_win/bilibili-setup.exe",
 # 腾讯云区域CDN (32城)
 "https://cos.ap-beijing.myqcloud.com/healthcheck.txt",
 "https://cos.ap-shanghai.myqcloud.com/healthcheck.txt",
 "https://cos.ap-guangzhou.myqcloud.com/healthcheck.txt",
 "https://cos.ap-chengdu.myqcloud.com/healthcheck.txt",
 "https://cos.ap-nanjing.myqcloud.com/healthcheck.txt",
 "https://cos.ap-chongqing.myqcloud.com/healthcheck.txt",
 "https://cos.ap-hangzhou.myqcloud.com/healthcheck.txt",
 "https://cos.ap-wuhan.myqcloud.com/healthcheck.txt",
 "https://cos.ap-tianjin.myqcloud.com/healthcheck.txt",
 "https://cos.ap-xian.myqcloud.com/healthcheck.txt",
 "https://cos.ap-zhengzhou.myqcloud.com/healthcheck.txt",
 "https://cos.ap-changsha.myqcloud.com/healthcheck.txt",
 "https://cos.ap-shijiazhuang.myqcloud.com/healthcheck.txt",
 "https://cos.ap-harbin.myqcloud.com/healthcheck.txt",
 "https://cos.ap-shenyang.myqcloud.com/healthcheck.txt",
 "https://cos.ap-dalian.myqcloud.com/healthcheck.txt",
 "https://cos.ap-jinan.myqcloud.com/healthcheck.txt",
 "https://cos.ap-qingdao.myqcloud.com/healthcheck.txt",
 "https://cos.ap-fuzhou.myqcloud.com/healthcheck.txt",
 "https://cos.ap-xiamen.myqcloud.com/healthcheck.txt",
 "https://cos.ap-nanchang.myqcloud.com/healthcheck.txt",
 "https://cos.ap-kunming.myqcloud.com/healthcheck.txt",
 "https://cos.ap-guiyang.myqcloud.com/healthcheck.txt",
 "https://cos.ap-lanzhou.myqcloud.com/healthcheck.txt",
 "https://cos.ap-ulanqab.myqcloud.com/healthcheck.txt",
 "https://cos.ap-huhehaote.myqcloud.com/healthcheck.txt",
 "https://cos.ap-yinchuan.myqcloud.com/healthcheck.txt",
 "https://cos.ap-xining.myqcloud.com/healthcheck.txt",
 "https://cos.ap-lasa.myqcloud.com/healthcheck.txt",
 "https://cos.ap-wulumuqi.myqcloud.com/healthcheck.txt",
 "https://cos.ap-nanning.myqcloud.com/healthcheck.txt",
 "https://cos.ap-haikou.myqcloud.com/healthcheck.txt",
 # 阿里云OSS区域
 "https://oss-cn-beijing.aliyuncs.com",
 "https://oss-cn-shanghai.aliyuncs.com",
 "https://oss-cn-shenzhen.aliyuncs.com",
 "https://oss-cn-hangzhou.aliyuncs.com",
 "https://oss-cn-guangzhou.aliyuncs.com",
 "https://oss-cn-chengdu.aliyuncs.com",
 "https://oss-cn-nanjing.aliyuncs.com",
 "https://oss-cn-wuhan-lb.aliyuncs.com",
 "https://oss-cn-zhangjiakou.aliyuncs.com",
 "https://oss-cn-huhehaote.aliyuncs.com",
 "https://oss-cn-shenzhen-finance-1.aliyuncs.com",
 "https://oss-cn-hangzhou-finance.aliyuncs.com",
 "https://oss-cn-shanghai-finance-1.aliyuncs.com",
 "https://oss-ap-southeast-1.aliyuncs.com",
 # 华为云OBS区域
 "https://obs.cn-north-4.myhuaweicloud.com",
 "https://obs.cn-east-3.myhuaweicloud.com",
 "https://obs.cn-south-1.myhuaweicloud.com",
 "https://obs.cn-east-2.myhuaweicloud.com",
 "https://obs.cn-southwest-2.myhuaweicloud.com",
 "https://obs.cn-north-1.myhuaweicloud.com",
 "https://obs.cn-north-9.myhuaweicloud.com",
 # 大文件测速
 "https://mirrors.tuna.tsinghua.edu.cn/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
 "https://mirrors.163.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
 "https://mirrors.sohu.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
 "https://mirrors.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
 "https://repo.huaweicloud.com/centos/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
]

def test_url(url):
    t0=time.time()
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Range":"bytes=0-1023"})
        resp=urllib.request.urlopen(req, timeout=10, context=ctx)
        data=resp.read(1024)
        elapsed=time.time()-t0
        code=resp.status
        size=len(data)
        speed=size/elapsed if elapsed>0 else 0
        return (url, code, size, speed, elapsed, None)
    except Exception as e:
        return (url, 0, 0, 0, 0, str(e)[:80])

print(f"Testing {len(URLS)} URLs...")
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
    results = list(ex.map(test_url, URLS))

ok=[r for r in results if r[1]>0]
fail=[r for r in results if r[1]==0]
print(f"\nOK: {len(ok)} | FAIL: {len(fail)}\n")
print("=== 可达 ===")
for url,code,size,speed,elapsed,err in sorted(ok, key=lambda x:-x[3]):
    from urllib.parse import urlparse
    host=urlparse(url).hostname
    print(f"  {host}: {code} {size}B {speed/1024:.0f}KB/s {elapsed:.2f}s")
print("\n=== 不可达 ===")
for url,code,size,speed,elapsed,err in fail:
    from urllib.parse import urlparse
    host=urlparse(url).hostname
    print(f"  {host}: {err}")
