#!/usr/bin/env bash
# Replayable cross-version upgrade check for persisted delta (ACID) tables.
#
# The repository's own regression (internal/engine/delta_upgrade_test.go) rebuilds the old
# physical layout with the current code and restarts the file three times. That is a
# reproduction of the layout, not a file written by an older release. This script closes that
# gap: it builds an OLDER revision of this repository, lets that binary create and fill a
# transactional table, stops it the way a normal shutdown does, then starts the CURRENT binary
# on the same database file three times, asserting each start that the initial history is
# published exactly once, the reported version never moves back, and a write made after a
# restart is still readable from the cursor the previous start handed out.
#
#   ./scripts/cross-version-upgrade.sh <old-ref>            # needs Go and a local build
#   DOCKER_IMAGE=golang:1.27.1-bookworm ./scripts/cross-version-upgrade.sh v1.1.0
#
# Only the assertion outcome is printed with timestamps; nothing else is written outside the
# temporary work directory, which is removed on exit.
#
# exit: 0 every assertion held; 1 an assertion failed; 2 usage or missing prerequisite;
#       3 a required read (server start, SQL submit, session answer) could not be taken -
#       which is never reported as a pass.
set -u -o pipefail
if [ "$#" -ne 1 ]; then echo "usage: $0 <old-ref>" >&2; exit 2; fi
REPO=$(cd "$(dirname "$0")/.." && pwd)
OLD_REF=$1
FAILS=0; READFAILS=0
note() { printf '%s %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf '%s ASSERT-FAIL %s\n' "$(date '+%H:%M:%S')" "$*"; FAILS=$((FAILS+1)); }
readfail() { printf '%s READ-FAIL %s\n' "$(date '+%H:%M:%S')" "$*"; READFAILS=$((READFAILS+1)); }
check() { if [ "$2" = "$3" ]; then note "ok  $1: $2"; else fail "$1: got=$2 want=$3"; fi; }

command -v git >/dev/null || { echo "git missing" >&2; exit 2; }
git -C "$REPO" rev-parse --verify "$OLD_REF^{commit}" >/dev/null 2>&1 || { readfail "old ref $OLD_REF not found"; exit 3; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cross-version-upgrade.XXXXXX") || { echo "mktemp failed" >&2; exit 3; }
DB="$WORK/legacy.duckdb"; PORT=$((18000 + RANDOM % 900)); BASE="http://127.0.0.1:$PORT"; SERVER_PID=""
build() { # build <source-dir> <output-inside-WORK>
  # The output always lands in $WORK, never in a source tree: the current build would
  # otherwise leave an untracked binary in the repository.
  if [ -n "${DOCKER_IMAGE:-}" ]; then
    docker run --rm -v "$1":/src -v "$WORK":/out -w /src ${GO_MOD_CACHE:+-v "$GO_MOD_CACHE":/go/pkg/mod} \
      -e GOTOOLCHAIN=local "$DOCKER_IMAGE" sh -c "go build -o /out/$(basename "$2") ./cmd/emulator" || return 1
    [ -x "$2" ]
  else
    ( cd "$1" && go build -o "$2" ./cmd/emulator )
  fi
}
cleanup() {
  [ -z "$SERVER_PID" ] || { kill -TERM "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
  git -C "$REPO" worktree remove --force "$WORK/old" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT
start() { # start <binary> <logname>
  "$1" -listen "127.0.0.1:$PORT" -database "$DB" -project p >"$WORK/$2" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    code=$(curl -s -o "$WORK/health-$2.json" -w '%{http_code}' "$BASE/healthz" 2>/dev/null)
    [ "$code" = "200" ] && { note "$2 ready ($(head -c 80 "$WORK/health-$2.json"))"; return 0; }
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.5
  done
  readfail "$2 never became ready (http_code=${code:-none})"; tail -5 "$WORK/$2"; return 3
}
stop() {
  [ -n "$SERVER_PID" ] || return 0
  kill -TERM "$SERVER_PID" 2>/dev/null
  for _ in $(seq 1 40); do kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 0.25; done
  kill -0 "$SERVER_PID" 2>/dev/null && { readfail "server did not exit on SIGTERM"; kill -KILL "$SERVER_PID"; }
  SERVER_PID=""; note "server stopped (database file closed)"
}
sql() { # sql <query>
  local code
  code=$(curl -s -o "$WORK/sql.txt" -w '%{http_code}' -X POST -H 'Content-Type: application/xml' \
    --data-binary "<Instance><Job><Tasks><SQL><Name>xver</Name><Query>$1</Query></SQL></Tasks></Job></Instance>" \
    "$BASE/projects/p/instances" 2>/dev/null)
  [ "$code" = "201" ] && { note "sql ok: $1"; return 0; }
  readfail "sql [$1] http=$code body=$(head -c 160 "$WORK/sql.txt")"; return 3
}
window() { # window <label> <from> <mode> <want-events> <want-version>
  curl -s -X POST -H 'Content-Type: application/json' \
    -d "{\"IncrementalReadOptions\":{\"StartVersion\":$2,\"EndVersion\":-1,\"Mode\":\"$3\"}}" \
    "$BASE/api/storage/v1/projects/p/schemas/default/tables/up/sessions?session_type=incremental_read" \
    > "$WORK/session-$1.json" 2>/dev/null
  local out; out=$(python3 - "$WORK/session-$1.json" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception as exc:
    print("READFAIL %s" % exc); raise SystemExit(3)
print("%s %s" % (d.get("LatestVersion", "-"), d.get("RecordCount", "-")))
PY
)
  local lv rc; read -r lv rc <<<"$out"
  [ "$lv" = "READFAIL" ] && { readfail "$1: $out"; return 3; }
  check "$1 events($3)" "$rc" "$4"
  check "$1 version" "$lv" "$5"
}

note "old-ref=$OLD_REF port=$PORT workdir=$WORK"
git -C "$REPO" worktree add --detach "$WORK/old" "$OLD_REF" >"$WORK/wt.log" 2>&1 || { readfail "worktree add"; exit 3; }
note "old HEAD: $(git -C "$WORK/old" log -1 --format='%h %ad' --date=iso)"
build "$WORK/old" "$WORK/emulator-old" || { readfail "old build failed"; exit 3; }
start "$WORK/emulator-old" old.log || exit 3
sql "create table up(id bigint, s string, primary key(id)) tblproperties('transactional'='true')" || exit 3
sql "insert into up values (1,'a'),(2,'b')" || exit 3
stop
check "old build left no version columns in the file" "$(strings "$DB" 2>/dev/null | grep -c '__em_')" "0"

build "$REPO" "$WORK/emulator-new" || { readfail "current build failed"; exit 3; }
start "$WORK/emulator-new" run1.log || exit 3
window "run1 baseline" 0 cdc 2 1 || exit 3
window "run1 live rows" 0 append 2 1 || exit 3
sql "insert into up values (3,'c')" || exit 3
window "run1 resume" 1 cdc 1 2 || exit 3
stop

start "$WORK/emulator-new" run2.log || exit 3
window "run2 history(no duplicate baseline)" 0 cdc 3 2 || exit 3
window "run2 live rows" 0 append 3 2 || exit 3
sql "insert into up values (4,'d')" || exit 3
window "run2 resume" 2 cdc 1 3 || exit 3
stop

start "$WORK/emulator-new" run3.log || exit 3
window "run3 history(no duplicate baseline)" 0 cdc 4 3 || exit 3
window "run3 live rows" 0 append 4 3 || exit 3
stop

note "summary: assertions_failed=$FAILS read_failures=$READFAILS"
[ "$READFAILS" -gt 0 ] && { echo "READ FAILURE - not a pass"; exit 3; }
[ "$FAILS" -gt 0 ] && exit 1
note "PASS: file written by $OLD_REF upgraded once and stayed stable across three starts"
exit 0
