#!/usr/bin/env bash
# 이상감지(Nelson Rule 1) 발동용 burst.
# 한 배치에 N개 측정값(단일 센서 TEMP, recipe[10,30] 이탈값 35.0)을 1초 간격으로 넣어,
# 키(EQP-BURST:TEMP)가 5분 윈도우 안에서 >=240 샘플 부트스트랩을 넘기게 한다.
#
# 사용: MSYS_NO_PATHCONV=1 bash local-infra/produce-burst.sh [N=260]
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
N="${1:-260}"

python - "$N" > /tmp/burst.json <<'PY'
import json, sys, time, datetime
n = int(sys.argv[1])
base = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
ms = []
for m in range(n):
    t = (base + datetime.timedelta(seconds=m)).isoformat().replace("+00:00", "Z")
    ms.append({"sequence": m, "measuredAt": t, "status": "OK",
               "sensors": [{"sensorId": "S1", "sensorType": "TEMP", "value": 35.0,
                            "recipeMin": 10.0, "recipeMax": 30.0, "unit": "C", "status": "OK"}]})
batch = {"batchId": "burst-%d" % int(time.time()), "deviceId": "D-BURST",
         "equipmentId": 999, "createdAt": base.isoformat().replace("+00:00", "Z"),
         "intervalSec": 1, "measurements": ms}
print(json.dumps(batch))
PY

bytes=$(wc -c < /tmp/burst.json)
cat /tmp/burst.json | kubectl exec -i -n kafka deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic fab-semiconductor-001
echo "produced 1 burst batch ($N measurements, ${bytes} bytes) for EQP-BURST:TEMP"
