# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `POST /api/deferrals`：申请延期放行（同一飞机同时只能有一份有效延期，并发申请只有一方能办成）。
- `POST /api/deferrals/{id}/resolve`：结束延期（过期/撤销），依赖它的未执行航段立即退回 `pending_reschedule`，已落地航段保留回执。
- `POST /api/flights/{id}/land`：登记落地并保留回执。
- `POST /api/flights/{id}/reschedule`：重排待重排航段，重新核对维护间隔、执勤和宵禁；写入冲突时按飞机编号重试，不重复占用飞机与机组。
- `POST /api/aircraft/{id}/reschedule-pending`：按飞机编号批量重排待重排航段，逐条重试并返回成功/失败。
- `GET /api/deferrals/{id}`、`GET /api/aircraft/{id}/deferrals`：查询延期放行。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示，正式运行需要外部身份系统、共享数据库和更强的跨实例锁。
