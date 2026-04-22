生产部署命令。只允许部署已提交且工作区干净的当前 HEAD 到固定生产环境。

固定目标：
- host: root@155.138.132.182
- deploy dir: /root/poly-executor

要求：
- 如果工作区不干净，拒绝部署。
- 必须先本地执行 `cargo build --release` 通过后再继续。
- 只能调用仓库内脚本 `bash scripts/deploy_prod.sh --confirm-prod`，不要直接运行任意 ssh。
- 这是生产环境操作，在真正执行前先用一句话提醒用户将要部署到生产。
- 不接受用户传入的任意主机、目录、ssh 参数或远端命令。
- 如果用户额外传了参数，把它们忽略并明确说明此命令不接受自由参数。
