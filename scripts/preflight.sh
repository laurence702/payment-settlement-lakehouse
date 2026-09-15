#!/usr/bin/env bash
# Checks the things that actually break this stack, before you waste ten
# minutes pulling images. Exits non-zero on a hard blocker.
set -uo pipefail
fail=0
ok()   { printf '  \033[32mok\033[0m    %s\n' "$1"; }
warn() { printf '  \033[33mwarn\033[0m  %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }

echo "preflight"

command -v docker >/dev/null 2>&1 || { bad "docker CLI not on PATH"; exit 1; }
docker info >/dev/null 2>&1 || { bad "docker daemon unreachable. Run: colima start"; exit 1; }
ok "docker daemon reachable"

# Keep stderr. Reporting "plugin not found" for any empty output once accused
# a working install of being absent, because the real error went to /dev/null.
# A check that discards the cause will invent one.
cv_err=$(docker compose version --short 2>&1 >/dev/null)
cv_raw=$(docker compose version --short 2>/dev/null)
cv=$(printf '%s' "$cv_raw" | sed 's/^[[:space:]]*v//' | tr -d '[:space:]')

if [ -n "$cv" ]; then
  # Leading numeric components only: tolerates suffixes like 2.29.1-desktop.1
  maj=$(printf '%s' "$cv" | sed -n 's/^\([0-9][0-9]*\).*/\1/p')
  rest=${cv#*.}
  min=$(printf '%s' "$rest" | sed -n 's/^\([0-9][0-9]*\).*/\1/p')
  : "${maj:=0}" "${min:=0}"
  if [ "$maj" -gt 2 ] || { [ "$maj" -eq 2 ] && [ "$min" -ge 20 ]; }; then
    ok "docker compose $cv"
  else
    bad "docker compose $cv is too old; need >= 2.20"
  fi
elif [ -z "$cv_err" ]; then
  bad "\`docker compose version --short\` printed nothing and reported no error. That is not a missing plugin; run it by hand to see what it does."
else
  case "$cv_err" in
    *"unknown command"*|*"is not a docker command"*)
      bad "docker compose plugin not installed. brew install docker-compose && ln -sfn \"\$(brew --prefix)/opt/docker-compose/bin/docker-compose\" ~/.docker/cli-plugins/docker-compose" ;;
    *"docker-credential"*|*"error listing credentials"*)
      bad "compose exists but the docker CLI cannot read credentials. Stale credsStore in ~/.docker/config.json:"
      printf '        %s\n' "${cv_err:0:200}" ;;
    *"unknown flag: --short"*|*"Usage:  docker"*)
      bad 'the docker CLI has no compose plugin: it parsed `compose version --short` itself.'
      printf '        %s\n' "Check for a dangling link:  ls -lL ~/.docker/cli-plugins/docker-compose"
      printf '        %s\n' "Install and point at it:    brew install docker-compose docker-buildx" ;;
    *)
      bad 'docker compose exists but `docker compose version --short` failed. It said:'
      printf '        %s\n' "${cv_err:0:200}" ;;
  esac
fi

# Memory. Airflow's own init refuses to run under 4 GB, and this stack has to
# fit Kafka and ClickHouse alongside it.
# `docker info` reports the guest kernel's MemTotal, which is always less than
# Colima was told to allocate: the kernel reserves a slice first. --memory 6
# shows up as ~5.8 GiB. Compare in MiB with headroom for that cut, and print a
# decimal, because integer GiB arithmetic truncates 5.8 to 5 and then lies.
mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
mem_mib=$(( mem_bytes / 1048576 ))
mem_disp=$(awk "BEGIN{printf \"%.1f\", $mem_mib/1024}")
if   [ "$mem_mib" -ge 7600 ]; then
  ok "VM memory ${mem_disp} GiB"
elif [ "$mem_mib" -ge 5600 ]; then
  warn "VM memory ${mem_disp} GiB. This is the designed target (--memory 6) and the ~5.5 GB of limits fit, but with no slack. See docs/adr/0003."
else
  bad "VM memory ${mem_disp} GiB is below the budget in docs/adr/0003. colima stop && colima start --cpu 4 --memory 6 --disk 60"
fi

cpus=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)
[ "$cpus" -ge 4 ] && ok "VM cpus ${cpus}" || warn "VM cpus ${cpus}; 4 recommended"

# Disk inside the VM, which is the 60 GB cap, not the host's free space.
avail=$(docker run --rm --entrypoint sh alpine:3.20 -c "df -BG /  | awk 'NR==2{print \$4}' | tr -d G" 2>/dev/null || echo "")
if [ -n "$avail" ]; then
  [ "$avail" -ge 20 ] && ok "VM disk free ${avail} GB" || warn "VM disk free ${avail} GB; images alone need ~12 GB"
fi

# .env must survive being sourced by a shell, not just read by compose.
# This tree can live under a directory with a space in it ("Data
# Engineering"), so any unquoted value containing a space makes `. ./.env`
# try to run its second word as a command. That failure is confusing enough
# to be worth its own check.
if [ -f .env ]; then
  if err=$( set -a; . ./.env; set +a 2>&1 ); then
    ok ".env sources cleanly"
  else
    bad ".env does not source cleanly (unquoted value with a space?): ${err}"
  fi
  bare=$(grep -nE '^[A-Z_]+=[^"#]*[[:space:]]+[^"#]*$' .env || true)
  if [ -n "$bare" ]; then
    bad "unquoted .env value(s) containing spaces:"; echo "$bare" | sed 's/^/          /'
  fi
  set -a; . ./.env; set +a
fi

# Host port collisions.
# A port held by one of this stack's own containers is not a collision, it is
# a stack that is already up. Saying "change PORT_* in .env" there sends you to
# edit config when the answer is `make down`. Ask docker who publishes the port
# before blaming the machine.
check_port() {
  if lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; then
    mine=$(docker ps --filter "publish=$1" --format '{{.Names}}' 2>/dev/null | head -1)
    if [ -n "$mine" ]; then
      bad "host port $1 ($2) is published by this stack's own container '$mine'. Run: make down"
    else
      bad "host port $1 ($2) is in use by something else on this machine. Change PORT_* in .env, or stop that service."
    fi
  else ok "host port $1 free ($2)"; fi
}
# Every port this stack publishes, not a subset.
check_port "${PORT_POSTGRES:-5434}"          postgres
check_port "${PORT_S3_API:-9010}"            s3-api
check_port "${PORT_S3_UI:-9011}"             s3-filer-ui
check_port "${PORT_CLICKHOUSE_HTTP:-8123}"   clickhouse-http
check_port "${PORT_CLICKHOUSE_NATIVE:-9100}" clickhouse-native
check_port "${PORT_KAFKA:-9092}"             kafka
check_port "${PORT_REDIS:-6379}"             redis
check_port "${PORT_AIRFLOW:-8081}"           airflow

# The bug this repo was born with.
if grep -rq '/Users/laurence' --include='*.yml' --include='*.yaml' . 2>/dev/null; then
  bad "a hardcoded /Users/laurence path is still present"
else ok "no hardcoded home directories"; fi

# Image pins. This was a separate target you had to remember to run, until a
# pin nobody verified sat broken for a day. A check that exists is not a check
# that ran, so it runs here.
echo
echo "image pins"
# verify-images exits 1 for a pin that is WRONG and 2 for a pin it could not
# CHECK. Collapsing both into a failure blocks a run over an image the registry
# would not answer about, which is not a problem with this repo. Only exit 1
# fails preflight.
./scripts/verify-images.sh; rc=$?
if [ "$rc" -eq 1 ]; then
  fail=1
elif [ "$rc" -eq 2 ]; then
  warn "some pins could not be verified. Not a blocker: an unverified pin only matters when you start the profile that uses it."
fi

echo
[ "$fail" -eq 0 ] && echo "preflight passed" || echo "preflight FAILED, fix the above first"
exit $fail
