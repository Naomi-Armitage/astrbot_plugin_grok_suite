# Grok AI 助手


## 功能

| 功能 | 命令 | 说明 |
|------|------|------|
| 文生图 | `#生图 [数量] [尺寸] 提示词` | 根据文字描述生成图片 |
| 图生图 | `#生图 提示词 + 图片` | 基于参考图片进行编辑/重绘 |
| 生视频 | `#生视频 [尺寸] [时长] 提示词 [+图片可选]` | 支持文生视频与图生视频 |
| 对话/联网搜索 | `/grok 问题内容 [+图片/语音/文件可选]` | 支持普通对话、实时联网搜索和多模态理解 |
| 帮助 | `/grok帮助` | 查看使用说明 |

## 配置说明

### API 配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `grok_api_url` | API 基础地址 | `https://api.x.ai` |
| `grok_api_key` | API 密钥 | 你的 xAI API Key |

**URL 配置说明**：只需填写基础 URL，插件会自动拼接正确的接口路径。

支持的 URL 格式（以下均可正常工作）：
- `https://api.x.ai`
- `https://api.x.ai/v1`
- `https://api.x.ai/v1/chat/completions`

### 模型配置

| 配置项 | 功能 | 默认值 | 接口 |
|--------|------|--------|------|
| `grok_image_model` | 文生图 | `grok-imagine-1.0` | `/v1/images/generations` |
| `grok_edit_model` | 图生图 | `grok-imagine-1.0-edit` | `/v1/images/edits` |
| `grok_video_model` | 生视频 | `grok-imagine-1.0-video` | `/v1/chat/completions` |
| `grok_search_model` | 对话/联网搜索 | `grok-4-fast` | `/v1/chat/completions` |

**模型说明**：所有模型均通过配置项读取，代码中的默认值仅作为备用。你可以根据 API 提供商支持的模型自行修改。

常见可用模型（参考 [grok2api](https://github.com/chenyme/grok2api)）：

| 模型 | 类型 | 说明 |
|------|------|------|
| `grok-imagine-1.0` | 图像生成 | 标准图像生成 |
| `grok-imagine-1.0-edit` | 图像编辑 | 基于参考图编辑 |
| `grok-imagine-1.0-video` | 视频生成 | 图片转视频 |

### 其他配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `save_media` | 是否保存生成的媒体文件 | `false` |
| `grok_search_mode` | 联网策略：`auto` / `on` / `off` | `auto` |
| `grok_search_enable_thinking` | 是否启用 reasoning 参数 | `true` |
| `grok_search_thinking_budget` | reasoning budget tokens | `32000` |
| `grok_search_timeout_seconds` | 对话/搜索超时时间（秒） | `60` |
| `grok_search_show_sources` | 是否在结果中显示来源链接 | `false` |
| `grok_search_max_sources` | 最多显示来源数量，`0` 表示不限制 | `5` |
| `grok_search_extra_body` | 对话请求额外 JSON body | `{}` |
| `grok_search_extra_headers` | 对话请求额外 JSON headers | `{}` |
| `grok_search_enable_skill` | 启用后移除内置 `grok_web_search` 工具 | `false` |
| `user_whitelist` | 用户白名单（空=不限制） | `[]` |
| `user_blacklist` | 用户黑名单 | `[]` |
| `group_whitelist` | 群聊白名单（空=不限制） | `[]` |
| `group_blacklist` | 群聊黑名单 | `[]` |

## 使用示例

### 文生图

```
#生图 一只可爱的猫咪
#生图 4 3:2 日落海滩风景
#生图 1:1 赛博朋克城市夜景
#生图 9:16 一只猫
#生图 4 1792x1024 日落海滩风景
```

参数说明：
- 数量：1-10（默认 1）
- 尺寸支持两种格式：
  - 比例格式：`1:1` / `2:3` / `3:2` / `9:16` / `16:9`
  - 像素格式：`1024x1024` / `1024x1792` / `1280x720` / `1792x1024` / `720x1280`
- 不加尺寸参数时，默认使用：`9:16`（720x1280）
- 参数顺序任意，如 `4 3:2` 或 `3:2 4` 均可

### 图生图

发送图片或引用图片，附带命令：
```
#生图 把背景换成森林
#生图 转换为油画风格
#生图 4 添加下雪效果
```

说明：
- 自动读取原图分辨率，并映射到最近合法尺寸
- 支持数量参数
- 目标尺寸始终根据原图自动匹配，忽略手动尺寸
- 附带两张图片时，第 2 张会作为局部重绘蒙版

### 生视频（文生/图生）

可直接发文字，或发送图片/引用图片后附带命令：
```
#生视频 让画面动起来
#生视频 10 夜晚海边的慢镜头
#生视频 3:2 夜晚海边的慢镜头
#生视频 16:9 6 让人物眨眼微笑
#生视频 1792x1024 添加飘落的樱花
```

说明：
- 文生视频默认尺寸：`3:2`（1792x1024）
- 文生视频默认时长：`6` 秒（可选 `10` / `15`）
- 尺寸支持比例格式（如 `3:2`、`16:9`）或像素格式（如 `1792x1024`）
- 图生视频自动读取原图分辨率，并匹配最近合法尺寸
- 固定 `720p` 输出（脚本内固定）
- 自动启用增强策略（高细节、低噪点、时序稳定）

### 对话/联网搜索

支持通过 `/grok` 发起普通对话、联网搜索，以及图片/语音/文件理解。

```text
/grok 今天有什么新闻
/grok 这张图片里有什么 +图片
/grok 帮我总结这个语音和文件 +语音/+文件
```

说明：
- `grok_search_mode=auto` 时，由模型判断是否需要联网
- `grok_search_mode=on` 时，始终联网搜索
- `grok_search_mode=off` 时，仅执行普通对话
- 开启 `grok_search_show_sources` 后，会在回复中附带来源链接
