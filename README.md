# 砝码账

砝码账为材料测试实验室保存称量修订、当前事实和可按成果号领取的日报。采集员可以补送仪器缓存，质控员可以撤销误录，维护员可以压实分区。值班交班使用原件、日账和成功回执核对同一成果。

## 运行

在仓库根目录使用 Python 3.11。依赖仅为标准库和随 Python 提供的 SQLite。

```sh
python3 -B check_project.py
python3 -B -m src.publish.cli demo examples/day.json
```

`check_project.py` 由运行平台提供，完整发现并执行 `tests` 中的原回归。在独立检出中也可用 `python3 -B -m unittest discover -s tests -t . -v`。演示库位于临时目录。演示先实际录入 12 mg 和 0 mg，再清空并改记 5 mg，补送旧值，重取回执，压实并重开读取。

```sh
python3 -B -m src.publish.cli init lake.sqlite
python3 -B -m src.publish.cli catalog lake.sqlite
python3 -B -m src.publish.cli current lake.sqlite
```

`submit DB REQUEST BATCH.json` 接受一个事件数组文件。`compact DB REQUEST START END SOURCE_VERSION` 压实闭区间内的所有在役段。`reclaim DB REQUEST VERSION RESULT...` 按成果回收旧称量原件，`VERSION` 是回收前的目录版本，后面跟一个或多个确认到期的历史成果号。`catalog DB` 返回可用于压实的全库目录版本和活动段。`receipt DB REQUEST` 领取成功回执。`history DB RESULT` 领取指定成果。`current` 和 `history` 支持 `--start`、`--end` 及可重复的 `--entity`。成果为空时 `current` 输出 JSON null。未知成果、无成功回执、非法输入和存储失败向标准错误输出原因并以状态 1 退出。

## 输入和计量

每个批次是一个非空数组。下面是一条字段级事件的完整示例。

```json
{"event_id":"weigh-1","entity":"E","field":"A","source":"scale-am","seq":1,"day":"2026-04-12","payload":"{\"A\":12.00}"}
```

`source` 是来源批次。`event_id`、`entity`、`field` 和 `source` 四元组唯一定位字段事件。`seq` 是非负 SQLite 整数。同一实体字段的同一序号只能有一个身份。`day` 是实验室使用的 ISO 日历日期。`payload` 是上游 JSON 对象原文，按原字符串保存。一个原件涉及多个字段时可以用各自的字段身份提交同一原文。只读取声明字段的更新，其他键随原件保留。可选 `requires` 数组列出前置事件，每项使用 `[event_id, entity, field, source]`。跨字段前置同样生效。

目标键缺失表示保留，显式 null 表示清空，JSON 数字表示写入称量值。实际 0 是可核账的有值事实。从未写入且只收到缺失修订的字段没有事实行。清空事实记录清空的观测日和事件。连续缺失修订不会改变原观测日或生效身份。数字支持有限十进制，系数至多 100 位，十进制指数绝对值至多 100。输出使用十进制字符串，明细保留输入的十进制精度。每个仍有值的当前字段贡献一条毫克明细，日报只列有贡献的日期。合计按精确值计算，实际零贡献仍计入明细条数。

序号在实体字段内确定相对次序，显式前置可跨字段。互不约束的就绪事件依次按序号、实体、字段、来源及事件号稳定排序。序号空档允许补送。若明确前置缺失、形成环或同一字段同序，整个批次拒绝。接纳和去重比较保存的原文及有效元数据，依赖列表顺序和批次中的事件次序不影响请求匹配。

## 发布和核账

`open_database` 和 `initialize_database` 位于 `src.publish.database`。调用者持有连接并在使用完毕后关闭。初始化可重复，保留已发布数据。`src.publish.coordinator.publish_batch(db, request_id, records)` 和 `publish_compaction(db, request_id, start, end, source_version)` 返回 `PublishReceipt`。应用入口管理自己的事务，应在连接没有外层事务时调用。`find_request_receipt` 可重取回执。对于纯重投，回执引用已存在的成果或列出多个原成果的逐事件回执。压实回执记录替换段及新目录版本，复用现行逻辑成果。读回或提交失败时目录、事实和日账维持上次成功状态。有效参数一经绑定，同号仅可按原参数重试。尝试诊断和成功回执分别保存。

`src.query.current.query_current` 和 `src.query.history.query_history` 支持可选 `start`、`end`、`entities`。查询在固定读事务内核对事实、事件及其生效证据、贡献和日报。日期及实体范围同时应用于当前状态、贡献和本次合计。返回范围内的原件及其出处，跨范围的前置事件仍会在全版核对中验证。返回的出处段号为事件第一次入账的原始段。物理替换后该出处号仍可读。基础回归覆盖三态、精确合计及日期移动、迟到和顺序冲突、重投、提交失败、压实损坏读回、历史固定成果及重开。

## 按成果回收

`src.publish.retention.reclaim(db, request, expired, version)` 回收实验室不再需要的旧称量原件，同时保留受审成果和迟到重投的判定。`expired` 是确认到期的历史成果号集合，`version` 是回收前的目录版本；目录版本过期、到期成果未知、当前成果被列入到期或同一成果重复回收都会拒绝。回收与批次、压实共用尝试和回执机制：参数一经绑定，同号只能按原参数重试，尝试诊断和成功回执分别落库。目录版本、正文回收标记和回执在同一事务内一次落库，任一步失败整体回滚。

未到期历史成果保留回读所需的全套原件：只要某个未到期的非当前快照仍引用一份原件，该原件就受保护。当前账保留事实、明细、合计，并保留一个闭包内的原件——每个实体字段最后一次定值或清空事件（只收到缺失修订、从未写入的字段则保留占用序号的全部修订）、其后仍约束接纳的事件，以及它们的递归显式前置。出处保留事件第一次入账的原段号。仅当原件的接收成果已确认到期、不在当前闭包内、且无未到期历史快照需要时，才移除其正文。

被回收原件不删除身份，而是以墓碑替换正文，保留身份、正文摘要（原文规范 JSON 的 SHA-256 摘要）、归一化修订、序号占用和出处段号。回收后的当前证据可省去已移除的历史原件，事实、明细和合计不变；回收前的正常行为完全不变。同文复领按摘要判定并返回原接收回执，同号异文（摘要或修订不符）拒绝。新事件若在 `requires` 中引用被回收正文，整个批次拒绝且不写入任何事件。到期成果再用 `history` 领取时报 `ReclaimedResultError`（已回收）。压实可在回收后照常运行，当前账仍能完整核对。演示场景中旧称量原件 A 因当前有效事件的显式前置依赖而保留，同批已被覆盖且无人依赖的旧原件 B 被移除。
