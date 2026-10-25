# 证据链侦探

元宝搜索驱动的公开资料调查工具：先把用户问题拆成子问题和核查点，再用真实搜索逐轮取证，最后按核查点输出答案、证据、冲突和未解决事项。

## 启动

```bash
conda activate my_env1
cd backend
python -m pip install -r requirements.txt
python -m uvicorn main:app --host 127.0.0.1 --port 8000
```

打开 http://127.0.0.1:8000/。在 `backend/.env` 配置 `WSA_API_KEY` 和 `DEEPSEEK_API_KEY`；密钥只在后端使用，不写入前端。

## 调查流程

1. 程序按 Asia/Shanghai 解析今天、明天和周末等日期，不让模型自行猜当前日期。
2. DeepSeek 只生成调查计划：原问题、问题类型、子问题、核查点、必要假设或澄清请求。
3. DeepSeek 根据计划和当前覆盖状态生成搜索任务；程序校验核查点 ID、去重并控制最多 3 轮、12 次真实元宝请求、120 秒总时限。
4. 元宝搜索返回真实来源；来源不再依赖白名单分级。来源类型未知时保留为 `sourceType=unknown`，URL 必须来自实际返回且为 HTTP(S)。
5. DeepSeek 只从实际摘要或正文提取证据；程序检查 sourceId、checkpointId 和逐字摘录是否存在。
6. 覆盖检查器逐个标记核查点为 `pending`、`supported`、`refuted`、`conflicted` 或 `insufficient`。
7. 报告逐个回答子问题，并列出证据来源、冲突、未解决点、实际搜索词和停止原因。

只有摘要时显示 `snippet_only`，不声称读过全文；搜索无结果不等于事实不存在。工具全部失败时显示调查失败，不用模型记忆伪装成功。网页内容中的指令只当资料，不执行。

## 页面

- `/`：提交调查
- `/report?id=...`：调查计划、核查点、搜索任务、证据链和未解决事项
- `/history`：历史调查
- `/submit`：比赛演示材料清单
- `/docs`：FastAPI 文档

截图 OCR、持续盯梢和白名单不属于新规格的核心流程，已从本版主路径移除。

## 测试

```bash
cd backend
python -m unittest discover -s tests -v
```

测试使用模拟返回；比赛录屏必须使用真实元宝搜索返回和可点击来源。当前项目不承诺零幻觉或绝对准确，未知和冲突会明确保留。

## 线上演示部署（Render）

项目已经包含 `Dockerfile` 和 `render.yaml`，适合直接部署成一个公开链接。

1. 将整个项目上传到 GitHub（不要提交 `backend/.env`，它已被 `.gitignore` 排除）。
2. 打开 Render，选择 **New → Blueprint**，连接这个 GitHub 仓库并部署 `render.yaml`。
3. 在 Render 的 Environment 中填写 `WSA_API_KEY` 和 `DEEPSEEK_API_KEY`，保存并等待部署完成。
4. 把 Render 分配的 `https://<服务名>.onrender.com/` 发给评委；健康检查地址是 `/healthz`。

首次打开可能需要等待免费实例唤醒。免费实例重启后 SQLite 历史记录可能被清空；这不影响现场演示，但如果要长期保留报告，需要给服务挂载持久磁盘或改用外部数据库。

> 注意：截图 OCR 依赖 macOS 专属组件（`ocr_bin` / `ocr.swift`），在 Linux 容器里不可用，线上「上传截图」会提示「OCR 组件不存在」。文字输入、事实核查、信息检索、报告、历史、知识库、缓存提示、删除管理等其余功能全部正常。如需线上支持 OCR，可把 OCR 换成纯 Python 方案（如 PaddleOCR）后重新构建镜像。
