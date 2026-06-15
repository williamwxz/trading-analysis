#!/usr/bin/env bash
# Provision (idempotently) the Telegram contact point + position-divergence alert
# rules into Grafana Cloud via the provisioning API. Safe to re-run; uses fixed
# UIDs and PUT-then-POST upserts. Source of truth: infra/grafana/alerting/.
#
# Required env:
#   GRAFANA_CLOUD_URL     e.g. https://yourstack.grafana.net
#   GRAFANA_CLOUD_TOKEN   service-account token (Alerting writer)
#   TELEGRAM_BOT_TOKEN    bot token (injected into the contact point)
#   TELEGRAM_CHAT_ID      destination chat id
#
# Exit non-zero on hard failure. The CI caller treats this step as non-fatal so a
# transient Grafana error never blocks the dashboard/deploy pipeline.
set -uo pipefail

: "${GRAFANA_CLOUD_URL:?set GRAFANA_CLOUD_URL}"
: "${GRAFANA_CLOUD_TOKEN:?set GRAFANA_CLOUD_TOKEN}"
: "${TELEGRAM_BOT_TOKEN:?set TELEGRAM_BOT_TOKEN}"
: "${TELEGRAM_CHAT_ID:?set TELEGRAM_CHAT_ID}"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$HERE/infra/grafana/alerting"
FOLDER_UID="divergence-alerts"
FOLDER_TITLE="Divergence Alerts"
AUTH=(-H "Authorization: Bearer $GRAFANA_CLOUD_TOKEN")
JSON=(-H "Content-Type: application/json")
PROV=(-H "X-Disable-Provenance: true")
rc=0

req() { # method path body -> echoes http code, writes body to /tmp/ga_resp.json
  local m="$1" p="$2" b="${3:-}"
  if [[ -n "$b" ]]; then
    curl -sS -o /tmp/ga_resp.json -w "%{http_code}" -X "$m" \
      "${AUTH[@]}" "${JSON[@]}" "${PROV[@]}" "$GRAFANA_CLOUD_URL$p" -d "$b"
  else
    curl -sS -o /tmp/ga_resp.json -w "%{http_code}" -X "$m" \
      "${AUTH[@]}" "${JSON[@]}" "${PROV[@]}" "$GRAFANA_CLOUD_URL$p"
  fi
}

ok() { [[ "$1" -ge 200 && "$1" -lt 300 ]]; }

echo "==> Ensuring folder '$FOLDER_TITLE' ($FOLDER_UID)"
code=$(req POST /api/folders "{\"uid\":\"$FOLDER_UID\",\"title\":\"$FOLDER_TITLE\"}")
if ok "$code"; then echo "   created"; elif [[ "$code" == 409 || "$code" == 412 ]]; then
  echo "   already exists"; else echo "   WARN folder HTTP $code: $(cat /tmp/ga_resp.json)"; fi

echo "==> Upserting Telegram contact point"
CP=$(envsubst < "$DIR/contact-point-telegram.json")
code=$(req PUT "/api/v1/provisioning/contact-points/telegram-divergence" "$CP")
if ! ok "$code"; then
  echo "   PUT HTTP $code -> trying POST"
  code=$(req POST "/api/v1/provisioning/contact-points" "$CP")
fi
if ok "$code"; then echo "   OK ($code)"; else
  echo "   ERROR contact point HTTP $code: $(cat /tmp/ga_resp.json)"; rc=1; fi

echo "==> Upserting alert rules"
# substitute the real folder uid into each rule, then upsert by uid
python3 -c "
import json
rules = json.load(open('$DIR/rules-divergence.json'))
for r in rules:
    r['folderUID'] = '$FOLDER_UID'
json.dump(rules, open('/tmp/ga_rules.json', 'w'))
"
count=$(python3 -c "import json;print(len(json.load(open('/tmp/ga_rules.json'))))")
for i in $(seq 0 $((count - 1))); do
  body=$(python3 -c "import json;print(json.dumps(json.load(open('/tmp/ga_rules.json'))[$i]))")
  uid=$(python3 -c "import json;print(json.load(open('/tmp/ga_rules.json'))[$i]['uid'])")
  title=$(python3 -c "import json;print(json.load(open('/tmp/ga_rules.json'))[$i]['title'])")
  code=$(req PUT "/api/v1/provisioning/alert-rules/$uid" "$body")
  if ! ok "$code"; then
    echo "   PUT $uid HTTP $code -> trying POST"
    code=$(req POST "/api/v1/provisioning/alert-rules" "$body")
  fi
  if ok "$code"; then echo "   OK  $title ($code)"; else
    echo "   ERROR $title HTTP $code: $(cat /tmp/ga_resp.json)"; rc=1; fi
done

echo "==> Setting 'divergence' rule-group eval interval to 60s"
GRP="{\"interval\":60}"
code=$(req PUT "/api/v1/provisioning/folder/$FOLDER_UID/rule-groups/divergence" \
  "{\"title\":\"divergence\",\"interval\":60}")
ok "$code" && echo "   OK ($code)" || echo "   WARN group interval HTTP $code (default kept): $(cat /tmp/ga_resp.json)"

exit $rc
