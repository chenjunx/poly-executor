生产运维命令。只允许对固定生产环境执行有限动作。

固定目标：
- host: root@155.138.132.182
- deploy dir: /root/poly-executor

允许动作：
- status
- health
- logs-alerts [lines]
- logs-orders [lines]
- restart

参数规则：
- 使用 `$ARGUMENTS` 解析动作。
- `restart` 是生产变更操作，执行前先提醒用户这是生产重启，然后调用 `bash scripts/ops_prod.sh restart --confirm-prod`。
- `status` 和 `health` 直接调用对应脚本动作。
- `logs-alerts` / `logs-orders` 可附带一个行数参数；若未提供则默认 50。
- 除上面动作外一律拒绝，不要直接运行任意 ssh 或远端 shell。

实现时只能调用仓库内脚本 `bash scripts/ops_prod.sh ...`。

Arguments: $ARGUMENTS
