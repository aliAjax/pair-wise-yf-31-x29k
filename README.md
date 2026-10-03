# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件、恢复方案和机务延期放行。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 机务延期放行与航段重排

机务检查期间，飞机可凭延期放行（maintenance deferral）继续执行航班，但维护、恢复方案和航班执行是连动的：

- **唯一性**：每架飞机同一时间至多一个 `active` 延期（SQLite 部分唯一索引 + 进程内写锁）。两人同时申请只有一人写入成功，输家收到 `409 deferral_conflict`，其中带回 `active_deferral_id` 和该飞机的待重排航段。
- **级联失效**：延期状态一变（到期、撤销或自动扫描过期），依赖它的未执行航段立即变为 `needs_reschedule` 并退回按飞机编号组织的待重排队列（释放飞机/机组占用）；草稿恢复方案中依赖该延期的航段被标记 `deferral_expired`，锁定前必须重新提交；**已落地航段保留回执快照**（`flight_receipts`），不受延期失效影响。
- **重排核对**：`POST /api/reschedule` 按 `aircraft_id` 取队列逐条重新核对维护间隔（可由新延期覆盖）、执勤时限、宵禁与航线许可、飞机/机组时间重叠；通过才重新占用资源，失败留在队列并返回具体问题，可用相同请求或 `POST /api/reschedule/retry` 重试，整条链路幂等。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。维护间隔不足时建航班必须有覆盖该航段的生效延期。
- `POST /api/deferrals`：申请延期放行（scheduler/ops_manager）；`GET /api/deferrals`、`GET /api/deferrals/aircraft/{id}`：查询（读取时自动扫描过期）。
- `POST /api/deferrals/{id}/extend`（`expected_revision` 乐观锁）、`/revoke`（ops_manager）、`/expire`：延长、撤销、到期。
- `POST /api/flights/{id}/land`：登记落地并写回执；`POST /api/reschedule`、`/api/reschedule/retry`：按飞机编号重排待重航段。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响（含 deferrals、receipts、reschedule_queue）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示：延期唯一性靠部分唯一索引可跨进程生效，而进程内写锁只在单实例内串行化写事务，多实例部署需要共享数据库与更强的分布式锁。
