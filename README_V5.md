# Flow Gateway V5

当前版本支持单图和多图 reference inputs。图片顺序会从上传、任务创建、后端上传到 Flow、提交 Omni 全程保留。

图片用途由 `prompt` 描述，例如首帧、尾帧、主图、细节图都写在提示词里。系统不强制区分 start frame / end frame 模式。

## 多图流程

1. `POST /api/v1/client/files/batch` 上传一张或多张 JPG / PNG / WebP。
2. 使用返回的 `file_id` 列表创建任务。
3. Gateway 将 `input_file_ids` 保存到 `task_input_media`，用 `position` 保留顺序。
4. Worker 按顺序上传本地文件到 Flow。
5. Omni 收到有序 `reference_media_ids`。

换账号重试时，Gateway 会重新把原始文件路径发给新的账号 Worker，由新账号重新上传，不复用上一个账号的 `media_id`。
