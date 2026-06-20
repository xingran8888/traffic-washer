# Traffic Washer v5.4

流量清洗工具，通过下载全球 CDN 大文件来消耗运营商带宽配额。

## ✨ 功能特性

- 🌐 **6800+ CDN 链接池** — 覆盖 32 省 × 5 大运营商（教育网/电信/联通/移动/其他）
- ⚡ **全量测速剔除** — 单线程 15 秒测速，<5MB/s 剔除，≥40MB/s 提升权重
- 📊 **实时监控** — 速度/流量/链接池/线程数/省份分布/ISP 分布
- 🎯 **省份筛选** — 按省份筛选 CDN 节点
- 🔄 **自动清理** — 域名级零流量清理、不可达整批拉黑
- 📋 **运行日志** — 实时日志页面
- 🐳 **Docker 部署** — 一键部署

## 🚀 快速开始

### 方式一：Docker 直接运行

```bash
docker build -t traffic-washer:latest .
docker run -d --name traffic-washer --restart always -p 9999:9999 traffic-washer:latest
```

### 方式二：Docker Compose

```bash
docker-compose up -d
```

### 方式三：爱快路由部署

1. 将 `app.py` 和 `Dockerfile` 上传到路由器
2. 构建并运行 Docker 容器

## 📋 配置说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| base_thread | 120 | 基础线程数 |
| chunk_size | 1MB | 下载分块大小 |
| timeout | 8s | 连接超时 |
| stall_timeout | 3s | 卡死超时 |
| cooldown | 0.2s | 请求间隔 |

## 🔧 API 接口

| 路由 | 说明 |
|------|------|
| `/` | 主页面 |
| `/start` | 开始任务 |
| `/stop` | 停止任务 |
| `/api/stats` | 获取统计数据 |
| `/setconfig` | 更新配置 |
| `/speedtest` | 全量测速剔除 |
| `/fullvalidate` | 全量验证 |
| `/api/logs` | 获取日志 |

## 📈 版本历史

### v5.4 (2026-06-20)
- 全量测速剔除：单线程 15 秒测速，<5MB/s 剔除
- 权重调整：≥40MB/s 链接 3x 权重
- 速率显示改为 MB/s
- CDN 节点扩充至 6800+
- 新增华为云/网易/搜狐 6.3GB 大文件

### v5.3 (2026-06-20)
- 城市级 CDN 流量分发
- 链接预验证
- 零流量清理

### v5.2 (2026-06-20)
- 省份筛选修复
- PROVINCIAL_MIRRORS 扩充

### v5.1 (2026-06-20)
- 省份名称标准化
- 15 个新省份 CDN 源

### v5.0 (2026-06-20)
- 全国各省 CDN 覆盖
- 运营商识别
- 自动清理

## 📄 License

MIT
