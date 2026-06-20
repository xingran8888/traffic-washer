# 🌊 Traffic Washer - 流量冲刷器 v5.0

全国各省CDN覆盖 · 运营商识别 · 线程安全 · 域名冷却 · 存活校验

## ✨ v5.0 核心改进

- 🔒 **线程安全** — 6把锁保护所有共享数据结构，多线程并发安全
- ⏱️ **域名冷却** — 同域名2秒冷却间隔，防止被CDN风控
- 🔍 **存活校验** — 每60秒HEAD探测，连续失败3次自动剔除
- ♻️ **黑名单恢复** — 每10分钟恢复10%黑名单链接重新测试
- 💾 **流式下载** — 64KB分块，连接即关，内存占用极低（~30MB）
- 🌍 **全国15+省市CDN覆盖** — 清华/南大/阿里/腾讯/中科大/上交/哈工大等
- 📡 **运营商精确识别** — 电信/联通/移动/教育网，按运营商筛选
- 🧹 **自动清理无流量链接** — 每5分钟检测，剔除无效链接并随机补充
- 🕷️ **爬虫HEAD验证** — 爬到的URL先验证再入池
- 📺 **IPTV直播源** — 央视/卫视/各省地方台
- ⏰ **分时设置** — 3个时间段独立配置线程/流量/限速
- 📊 **实时统计** — 省份分布/运营商分布/实时速率

## 🚀 快速部署

### 方式一：Docker Compose（推荐）

```bash
git clone https://github.com/xingran8888/traffic-washer.git
cd traffic-washer
docker-compose up -d
```

### 方式二：Docker 命令

```bash
docker build -t traffic-washer:latest .
docker run -d --name traffic-washer --restart always -p 9999:9999 traffic-washer:latest
```

### 方式三：爱快路由器部署

#### 方法A：离线导入tar（推荐）

1. 在有Docker的电脑上构建并导出：
```bash
git clone https://github.com/xingran8888/traffic-washer.git
cd traffic-washer
docker build -t traffic-washer:latest .
docker save traffic-washer:latest > traffic-washer.tar
```

2. 爱快后台 → **Docker** → **镜像管理** → **导入镜像** → 上传 `traffic-washer.tar`

3. **容器管理** → 添加容器：
   - 镜像：`traffic-washer:latest`
   - 容器名：`traffic-washer`
   - 网络模式：`host`
   - 重启策略：`always`

4. 访问 `http://爱快IP:9999`

#### 方法B：SSH在线构建

```bash
# SSH登录爱快
cd /tmp
git clone https://github.com/xingran8888/traffic-washer.git
cd traffic-washer
docker build -t traffic-washer:latest .
docker run -d --name traffic-washer --restart always --network host traffic-washer:latest
```

#### 方法C：拉取预构建镜像

```bash
docker pull xingran8888/traffic-washer:latest
docker run -d --name traffic-washer --restart always --network host xingran8888/traffic-washer:latest
```

> **爱快注意：** 建议用 `--network host` 模式避免NAT问题，端口固定9999。

### 访问管理面板

打开浏览器访问: `http://你的服务器IP:9999`

## 📁 项目结构

```
traffic-washer/
├── app.py              # 主程序 (Flask, v5.0, 1506行)
├── Dockerfile          # Docker构建文件
├── docker-compose.yml  # Docker Compose配置
└── README.md           # 项目说明
```

## 📋 系统要求

| 资源 | 最低 | 推荐 |
|------|------|------|
| CPU | 1核 | 2核 |
| 内存 | 64MB | 256MB |
| 磁盘 | 200MB | 500MB |
| 系统 | Linux/爱快/群晖 | Docker |
| 网络 | 公网 | 带宽越大越好 |

## 🔧 配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| 线程数 | 20 | 同时下载线程数 |
| 域名冷却 | 2秒 | 同域名最小请求间隔 |
| 存活校验 | 60秒 | HEAD探测间隔 |
| 失败阈值 | 3次 | 连续失败次数剔除 |
| 黑名单恢复 | 10分钟 | 恢复10%黑名单重测 |
| 每日流量限制 | 0(无限) | GB为单位 |
| 限速 | 0(不限) | MB/s为单位 |

## 📝 更新日志

### v5.0 (2026-06-20)
- 新增线程安全锁（6把锁覆盖所有共享数据结构）
- 新增域名冷却机制（2秒间隔防风控）
- 新增后台存活校验线程（HEAD探测）
- 新增黑名单恢复机制（10分钟恢复10%）
- 优化流式下载（64KB分块+连接释放+GC）
- 优化爬虫URL验证（HEAD预检再入池）
- 优化内存（连接池5→3，chunk 256KB→64KB）

### v4.0 (2026-06-20)
- 全国15+省市CDN覆盖
- 运营商精确识别（电信/联通/移动/教育网）
- 同省/外省智能分流
- IPTV直播源 + 视频CDN链接
- 自动爬取各省镜像站链接

## ⚠️ 免责声明

本项目仅供学习交流使用，请遵守当地法律法规。

## 📄 许可证

MIT License
