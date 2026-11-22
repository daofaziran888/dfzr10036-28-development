#!/usr/bin/env bash
# 分片维护归属明细 端到端核验脚本（curl + 标准库 json，无额外依赖）。
# 用法：BASE=http://127.0.0.1:8000 ./scripts/e2e_shard_maintain.sh
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:8013}/api/shard-maintain"
T=t_e2e_orders
P=s3://wh/e2e/
J='Content-Type: application/json'

post() { curl -s -X POST "$BASE$1" -H "$J" ${2+-d "$2"}; }
get() { curl -s "$BASE$1"; }

post /grants "{\"管道编号\":\"EA\",\"目标表\":\"$T\",\"授权前缀\":\"$P\",\"授权范围\":\"专有\",\"共享管道\":[\"EB\"]}" >/dev/null
post /grants "{\"管道编号\":\"EB\",\"目标表\":\"$T\",\"授权前缀\":\"$P\",\"授权范围\":\"专有\",\"共享管道\":[\"EA\"]}" >/dev/null

# EA 专有 p1=8MB，EB 专有 p2=13MB，p3 双向明示共享=20MB
post /ownership "{\"管道编号\":\"EA\",\"目标表\":\"$T\",\"文件路径\":\"${P}p1.parquet\",\"文件大小MB\":\"8MB\",\"阻塞批次\":\"BLK-EA\",\"清理区间\":\"rA\"}" >/dev/null
post /ownership "{\"管道编号\":\"EB\",\"目标表\":\"$T\",\"文件路径\":\"${P}p2.parquet\",\"文件大小MB\":13,\"阻塞批次\":\"BLK-EB\",\"清理区间\":\"rB\"}" >/dev/null
post /ownership "{\"管道编号\":\"EA\",\"目标表\":\"$T\",\"文件路径\":\"${P}p3.parquet\",\"文件大小MB\":20,\"共享授权管道\":[\"EB\"]}" >/dev/null
post /ownership "{\"管道编号\":\"EB\",\"目标表\":\"$T\",\"文件路径\":\"${P}p3.parquet\",\"文件大小MB\":20,\"共享授权管道\":[\"EA\"]}" >/dev/null

# 越权管道 EC 登记 p1，必须被拦截且不出现在后续来源里
BAD=$(post /ownership "{\"管道编号\":\"EC\",\"目标表\":\"$T\",\"文件路径\":\"${P}p1.parquet\",\"文件大小MB\":1}")
echo "$BAD" | python3 -c "import sys,json; d=json.load(sys.stdin); assert (not d['ok']) and d['entry']['登记状态']=='登记无效-越权', d; print('1) 越权登记已拦截:', d['message'])"

# 并行申请：两个批次都含 p3
BA=$(post /batches "{\"管道编号\":\"EA\",\"目标表\":\"$T\",\"路径清单\":[\"${P}p1.parquet\",\"${P}p3.parquet\"]}" | python3 -c "import sys,json;print(json.load(sys.stdin)['批次']['id'])")
BB=$(post /batches "{\"管道编号\":\"EB\",\"目标表\":\"$T\",\"路径清单\":[\"${P}p2.parquet\",\"${P}p3.parquet\"]}" | python3 -c "import sys,json;print(json.load(sys.stdin)['批次']['id'])")

# EA 先领 => p1+p3；EB 后领 => 只剩 p2，p3 被并发批次锁
post /batches/$BA/claim | python3 -c "import sys,json; d=json.load(sys.stdin); assert {o['文件路径'].split('/')[-1] for o in d['可操作对象']}=={'p1.parquet','p3.parquet'}, d; print('2) EA 领取 p1+p3 成功')"
post /batches/$BB/claim | python3 -c "import sys,json; d=json.load(sys.stdin); assert [o['文件路径'].split('/')[-1] for o in d['可操作对象']]==['p2.parquet'], d; assert d['仍阻塞路径'][0]['文件路径'].endswith('p3.parquet'), d; print('3) EB 仅得 p2，共享 p3 被路径锁拦截')"

# EA 先完成 p1，撤权后 p3 释放给 EB
post /batches/$BA/replace "{\"路径清单\":[\"${P}p1.parquet\"]}" >/dev/null
post /grants/revoke "{\"管道编号\":\"EA\",\"目标表\":\"$T\"}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['冻结归属明细数']>=1; print('4) 撤权生效，冻结未替换明细', d['冻结归属明细数'], '条；p1 替换证据保留')"
post /batches/$BB/claim | python3 -c "import sys,json; d=json.load(sys.stdin); assert {o['文件路径'].split('/')[-1] for o in d['可操作对象']}=={'p2.parquet','p3.parquet'}, d; print('5) 撤权后 EB 可接管共享 p3')"

# EB 完成 p2+p3；p3 再替换一次必须被跳过（最多替换一次）
post /batches/$BB/replace "{\"路径清单\":[\"${P}p2.parquet\",\"${P}p3.parquet\"]}" >/dev/null
post /batches/$BB/replace "{\"路径清单\":[\"${P}p3.parquet\"]}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['本次替换']==[] and d['跳过'][0]['文件路径'].endswith('p3.parquet'), d; print('6) 共享文件 p3 全局最多替换一次')"

# 对账：EA 已替换 8MB（p1），p3 撤权未替换
get /batches/$BA/reconcile | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['已替换空间MB']==8 and d['尚未替换空间MB']==20, d; print('7) EA 对账：已替换 8MB，未替换 20MB（含授权快照可追查）')"
# EB 对账：13+20 全部替换
get /batches/$BB/reconcile | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['已替换空间MB']==33 and d['尚未替换空间MB']==0, d; print('8) EB 对账：已替换 33MB（p2 专有 13 + p3 共享 20）')"

# 来源回传：EA=8(专有)+10(共享)=18；EB=13(专有)+10(共享)=23；越权 EC 不得出现
get /report/$T | python3 -c "
import sys,json
d=json.load(sys.stdin)
b={x['管道编号']:x for x in d['按来源释放空间MB']}
assert 'EC' not in b, '越权管道不得成为释放来源'
assert b['EA']['专有释放MB']==8 and b['EA']['共享释放MB']==10 and b['EA']['释放空间MB']==18, b
assert b['EB']['专有释放MB']==13 and b['EB']['共享释放MB']==10 and b['EB']['释放空间MB']==23, b
assert d['总释放空间MB']==41, d  # 8+13+20，共享物理释放只计一次
blocked={x['文件路径'].split('/')[-1] for x in d['仍阻塞路径']}
assert 'p1.parquet' in blocked, d
print('9) 来源回传正确：EA=18MB、EB=23MB、总物理释放 41MB；越权 EC 已剔除；仍阻塞路径含越权留痕')
"
echo 'ALL E2E CHECKS PASSED'
