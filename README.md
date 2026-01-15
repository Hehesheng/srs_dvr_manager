# SRS DVR Manager

一个基于 FastAPI 的 SRS（Simple Realtime Server）录播管理系统，支持 WebDAV 云存储、自动封面生成、智能存储管理和流式下载。

## 功能特性

### 核心功能
- **WebDAV 云存储集成**：自动上传录播文件到 WebDAV 兼容服务（如 OwnCloud、Nextcloud）
- **自动封面生成**：
  - 录播文件：从视频首帧自动提取封面
  - 实时流：从 RTMP 直播流实时生成封面，支持 5 分钟本地缓存
- **智能存储管理**：自动监控远端存储大小，超过限制时按时间顺序删除旧文件
- **流式下载**：支持 HTTP Range 请求，实现断点续传和快进/快退
- **隐私保护**：所有对外 API 都不暴露 WebDAV 凭据

### 高级特性
- 异步非阻塞设计，支持高并发
- 详细的日志记录和错误追踪
- 自动目录创建和初始化
- 缓存管理和过期清理
- 并发请求去重（避免重复生成封面）

## 系统架构

```
SRS 录播完成
     ↓
DVR 回调接口 (/stream/on_dvr/)
     ↓
上传视频到 WebDAV
     ↓
生成录播封面
     ↓
删除本地文件
     ↓
检查存储大小，删除旧文件
```

### 核心模块

| 模块 | 功能 |
|-----|------|
| `webdav_client.py` | WebDAV 协议客户端实现，处理文件上传/下载/删除 |
| `webdav_record_manager.py` | 录播和流封面管理，缓存控制，存储限制 |
| `api.py` | FastAPI 应用，对外 API 接口 |
| `config.yaml` | 配置文件，WebDAV 和本地目录设置 |

## 快速开始

### 前置要求
- Python 3.8+
- FFmpeg（用于封面生成）
- WebDAV 服务器（OwnCloud、Nextcloud 等）
- SRS 服务器

### 安装

1. **克隆项目**
```bash
git clone <repo-url>
cd srs_dvr_manager
```

2. **创建虚拟环境**
```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\activate  # Windows
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

4. **安装 FFmpeg**
```bash
# Ubuntu/Debian
sudo apt-get install ffmpeg

# macOS
brew install ffmpeg

# 其他系统请参考 FFmpeg 官方文档
```

### 配置

编辑 `config.yaml` 配置 WebDAV 连接和存储路径：

```yaml
webdav:
  hostname: "https://your-owncloud.com/remote.php/dav/files/username/"
  login: "username"
  password: "password"
  root: "/stream_record/"           # WebDAV 根目录
  max_storage_bytes: 53687091200    # 最大存储大小 (50GB)

record:
  local_dir: "./live"               # 本地录播临时目录
  cover_dir: "./live/cover"         # 本地封面临时目录
  cover_remote_dir: "cover"         # 远端封面目录
```

### 运行

```bash
# 使用提供的脚本运行
./run.sh

# 或直接使用 uvicorn
uvicorn api:app --host 0.0.0.0 --port 11985
```

应用将启动在 `http://localhost:11985`

## API 文档

### 1. DVR 回调接口
```
POST /stream/on_dvr/
```

**说明**：SRS 录播完成时的回调接口，自动处理文件上传和管理。

**请求体**（由 SRS 自动发送）：
```json
{
  "server_id": "vid-xxx",
  "action": "on_dvr",
  "stream": "stream_name",
  "file": "/path/to/file.flv",
  "param": "?record=true"
}
```

**响应**：
```json
{"code": 0}
```

**自动操作**：
- 上传视频文件到 WebDAV
- 生成视频封面
- 删除本地文件
- 检查并清理超限存储

### 2. 获取录播文件列表
```
GET /stream/query_record/{stream_name}
```

**说明**：查询指定流的所有录播文件。

**示例**：
```bash
curl http://localhost:11985/stream/query_record/stream_name
```

**响应**：
```json
{
  "stream_name": "stream_name",
  "files": [
    {
      "file_name": "stream_name.1705348800.123.mp4",
      "timestamp": 1705348800,
      "file_size": 1024000000,
      "download_url": "/stream/record/d/stream_name.1705348800.123.mp4",
      "thumb_url": "/stream/record/cover/stream_name.1705348800.123.jpg"
    }
  ]
}
```

### 3. 下载录播文件
```
GET /stream/record/d/{file_name}
GET /stream/record/p/{file_name}
```

**说明**：流式下载录播文件，支持 HTTP Range 请求。

**示例**：
```bash
# 完整下载
curl -O http://localhost:11985/stream/record/d/stream_name.1705348800.123.mp4

# 支持断点续传
curl -H "Range: bytes=0-1000000" http://localhost:11985/stream/record/d/stream_name.1705348800.123.mp4
```

**特性**：
- 支持断点续传（HTTP 206 Partial Content）
- 不暴露 WebDAV 凭据
- 自动处理大文件流传

### 4. 获取录播封面
```
GET /stream/record/cover/{cover_name}
```

**说明**：获取录播文件的封面图片。

**示例**：
```bash
curl http://localhost:11985/stream/record/cover/stream_name.1705348800.123.jpg -o cover.jpg
```

### 5. 获取直播流实时封面
```
GET /stream/cover/{stream_name}
```

**说明**：获取直播流的实时封面，支持 5 分钟本地缓存。

**示例**：
```bash
curl http://localhost:11985/stream/cover/stream_name -o stream_cover.jpg
```

**工作原理**：
1. 首次请求：从 RTMP 源生成封面（~5-10秒）
2. 缓存期间（5分钟内）：返回缓存的封面
3. 缓存过期：下次请求时重新生成
4. 并发请求：只生成一次，其他请求等待结果

**返回**：
- `200 OK`：包含 JPEG 图片数据
- `404 Not Found`：无法生成封面（RTMP 流不存在或已断开）
- `503 Service Unavailable`：服务未初始化

## 配置详解

### WebDAV 配置
| 参数 | 说明 | 示例 |
|-----|------|------|
| `hostname` | WebDAV 服务器地址 | `https://owncloud.example.com/remote.php/dav/files/user/` |
| `login` | WebDAV 用户名 | `username` |
| `password` | WebDAV 密码 | `password123` |
| `root` | 存储根目录 | `/stream_record/` |
| `max_storage_bytes` | 最大存储大小（字节）| `53687091200` (50GB) |

### 本地配置
| 参数 | 说明 | 默认值 |
|-----|------|--------|
| `local_dir` | 本地录播文件临时目录 | `./live` |
| `cover_dir` | 本地封面临时目录 | `./live/cover` |
| `cover_remote_dir` | 远端封面存储目录 | `cover` |

## SRS 配置示例

在 SRS 配置文件中启用 DVR 回调：

```nginx
vhost __defaultVhost__ {
    # DVR 配置
    dvr {
        enabled on;
        dvr_path ./objs/nginx/html/record/[app]/[stream].flv;
        dvr_plan session;
        dvr_duration 30000;
        dvr_wait_keyframe on;
        time_jitter full;
    }
    
    # DVR 回调
    on_dvr {
        enabled on;
        web_hook_url http://localhost:11985/stream/on_dvr/;
    }
}
```

## 存储管理

### 自动清理机制
- 监控远端 WebDAV 存储总大小
- 超过 `max_storage_bytes` 时触发清理
- 按时间戳排序，删除最旧的文件
- 同时删除关联的封面文件
- 详细的清理日志记录

### 计算存储大小
- 文件大小 = 视频文件 + 对应的封面文件
- 50GB = 约 1-2 天的高清录播（取决于码率）

### 示例
```
初始状态：40GB
新录播上传：15GB
触发清理：40 + 15 = 55GB > 50GB
删除动作：按时间删除最旧文件，直到 ≤ 50GB
```

## 日志

应用日志存储在 `logs/app.log`，按天滚动。

### 日志级别
- `DEBUG`：详细的调试信息
- `INFO`：关键操作日志
- `WARNING`：警告信息（如网络异常）
- `ERROR`：错误信息

### 查看日志
```bash
# 查看最新日志
tail -f logs/app.log

# 查看特定操作的日志
grep "Uploading record" logs/app.log
grep "Storage cleanup" logs/app.log
grep "Generated new cover" logs/app.log
```

## 故障排查

### 问题 1：HTTP 409 上传失败

**原因**：WebDAV 目录不存在

**解决**：
- 确保 WebDAV 根目录已创建
- 检查凭据是否正确
- 查看日志中的详细错误信息

### 问题 2：无法生成实时流封面

**原因**：RTMP 流不存在或 FFmpeg 超时

**解决**：
- 确认 RTMP 流正在直播（`rtmp://localhost/live/{stream_name}`）
- 检查 FFmpeg 是否正确安装：`ffmpeg -version`
- 检查本地 `./live/cover` 目录权限

### 问题 3：存储清理不工作

**原因**：WebDAV 连接错误或权限不足

**解决**：
- 检查网络连接
- 验证 WebDAV 用户权限
- 查看详细错误日志

### 问题 4：内存持续增长

**原因**：封面缓存任务泄漏

**解决**：
- 重启应用
- 检查日志中是否有未清理的任务
- 增加监控告警

## 性能优化

### 缓存策略
- 直播流封面：5 分钟 TTL，减少 FFmpeg 调用
- 文件列表：不缓存，每次查询获取最新
- 录播封面：从 WebDAV 读取，使用 HTTP 缓存头

### 并发优化
- 异步 I/O：所有 WebDAV 操作都是异步
- 任务去重：多个请求生成同一封面时只执行一次
- 连接池：复用 aiohttp 连接

### 建议配置
```yaml
# Uvicorn 启动参数
uvicorn api:app --host 0.0.0.0 --port 11985 --workers 4 --loop uvloop
```

## 开发

### 项目结构
```
srs_dvr_manager/
├── api.py                       # FastAPI 应用
├── webdav_client.py             # WebDAV 客户端
├── webdav_record_manager.py     # 录播管理逻辑
├── RecordFileManager.py         # 数据模型
├── config.yaml                  # 配置文件
├── logging_config.yaml          # 日志配置
├── requirements.txt             # 依赖列表
└── README.md                    # 本文件
```

### 添加新功能

#### 添加新的 API 端点
在 `api.py` 中添加路由：

```python
@app.get("/api/new_endpoint")
async def new_endpoint():
    if record_mgr is None:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    # 实现逻辑
    return {"result": "success"}
```

#### 扩展 WebDAV 操作
在 `webdav_client.py` 中添加方法：

```python
async def custom_operation(self, path: str) -> Any:
    """Custom WebDAV operation."""
    # 实现逻辑
    pass
```

### 测试

```bash
# 测试 API 可用性
curl http://localhost:11985/

# 测试回调接口
curl -X POST http://localhost:11985/stream/on_dvr/ \
  -H "Content-Type: application/json" \
  -d '{"stream":"test","file":"./live/test.mp4","action":"on_dvr"}'

# 测试列表接口
curl http://localhost:11985/stream/query_record/test

# 测试封面接口
curl http://localhost:11985/stream/cover/test -O cover.jpg
```

## 常见问题

**Q: 支持哪些 WebDAV 服务？**
A: 支持所有 WebDAV 兼容服务，包括 OwnCloud、Nextcloud、Aliyun Drive 等。

**Q: 如何修改存储限制？**
A: 编辑 `config.yaml` 中的 `max_storage_bytes` 参数（单位：字节）。

**Q: 封面缓存可以禁用吗？**
A: 可以。修改 `webdav_record_manager.py` 中的 `STREAM_COVER_CACHE_TTL = 0`。

**Q: 支持多个 SRS 服务器吗？**
A: 支持。每个 SRS 都可以指向同一个应用实例，应用自动处理并发。

**Q: 如何备份配置？**
A: 只需备份 `config.yaml` 文件。所有数据存储在 WebDAV 上。

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

如有问题或建议，请通过以下方式联系：
- 提交 GitHub Issue
- 发送邮件至项目维护者
