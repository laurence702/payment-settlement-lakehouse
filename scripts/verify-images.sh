#!/usr/bin/env bash
# Every image tag in .env is checked against the registry WITHOUT pulling it.
# A pinned tag that does not exist is the single most likely reason a fresh
# clone of this repo fails, so check it deliberately instead of finding out
# eight minutes into a pull.
#
# Reporting every non-zero exit as "tag does not resolve" once declared three
# real tags fake, seconds after they had passed, because a rate-limited
# registry response got thrown away. So: separate "does not exist" from "could
# not be checked", print what the registry said, and retry once.
set -uo pipefail
[ -f .env ] || { echo ".env missing. Run: make bootstrap"; exit 1; }
set -a; . ./.env; set +a

missing=0   # tag genuinely not in the registry: a real failure
unknown=0   # could not reach a verdict: not the repo's fault, do not pretend it is

probe() {  # prints a classification, sets $err
  # Local store first. An image already on disk is not merely resolvable, it is
  # usable, and that is the question this check is really asking. `docker
  # manifest inspect` always goes to the registry even for an image sitting in
  # the local store, which is how a laptop that already has every image burns
  # ten anonymous Docker Hub requests per preflight. Behind carrier-grade NAT
  # that budget is shared with strangers and runs out without you doing
  # anything. Checking locally also means preflight works with no network.
  if docker image inspect "$1" >/dev/null 2>&1; then echo local; return; fi
  err=$(docker manifest inspect "$1" 2>&1 >/dev/null)
  [ -z "$err" ] && { echo found; return; }
  case "$err" in
    *"manifest unknown"*|*"not found"*|*"no such manifest"*|*"MANIFEST_UNKNOWN"*) echo absent ;;
    *toomanyrequests*|*"rate limit"*|*"429"*)                                     echo ratelimited ;;
    *unauthorized*|*"authentication required"*|*"denied"*)                        echo unauthorized ;;
    *)                                                                            echo unreachable ;;
  esac
}

for var in KAFKA_IMAGE POSTGRES_IMAGE CLICKHOUSE_IMAGE REDIS_IMAGE \
           SEAWEEDFS_IMAGE AWSCLI_IMAGE; do
  img="${!var:-}"
  [ -z "$img" ] && { printf '  \033[31mFAIL\033[0m  %-18s unset in .env\n' "$var"; missing=1; continue; }

  verdict=$(probe "$img")
  # One retry, only for the transient classes. A real 404 does not get better.
  if [ "$verdict" != found ] && [ "$verdict" != absent ] && [ "$verdict" != local ]; then
    sleep 2; verdict=$(probe "$img")
  fi

  case "$verdict" in
    local)  printf '  \033[32mok\033[0m    %-18s %s  (local)\n' "$var" "$img" ;;
    found)  printf '  \033[32mok\033[0m    %-18s %s  (registry)\n' "$var" "$img" ;;
    absent) printf '  \033[31mFAIL\033[0m  %-18s %s  <- NOT IN REGISTRY\n' "$var" "$img"; missing=1 ;;
    ratelimited)
            printf '  \033[33m????\033[0m  %-18s %s  <- registry rate limited us\n' "$var" "$img"; unknown=1 ;;
    unauthorized)
            printf '  \033[33m????\033[0m  %-18s %s  <- registry refused: not logged in?\n' "$var" "$img"; unknown=1 ;;
    *)      printf '  \033[33m????\033[0m  %-18s %s  <- could not reach the registry\n' "$var" "$img"
            [ -n "${err// /}" ] && printf '        %s\n' "${err:0:160}"
            unknown=1 ;;
  esac
done

echo
if [ "$missing" -ne 0 ]; then
  cat <<'MSG'
A pin does not exist in the registry. Find a real tag and edit .env only:
  docker run --rm quay.io/skopeo/stable list-tags docker://<image> | head -40
Nothing else in this repo references a tag, so .env is the only file to change.
MSG
  exit 1
fi

if [ "$unknown" -ne 0 ]; then
  cat <<'MSG'
Some pins could not be CHECKED. That is not the same as being wrong, and
nothing above should be edited on the strength of it.

Only pins NOT already in the local image store are checked against the
registry, so this only happens for an image you have never pulled.

Docker Hub allows 100 anonymous requests per 6 hours per IPv4 address or IPv6
/64. Behind carrier-grade NAT that is shared with everyone else on the same
ISP pool, so it can be exhausted without you doing anything. Authenticated
limits are per account rather than per IP, which is the way out:

  docker login -u <your-hub-username>     # then paste a Personal Access Token

Use -u. Without it the CLI uses the browser device-code flow, which has its
own separate rate limit and fails with "Global rate limit exceeded".

Or just pull the one image it could not check, and re-run:
  docker pull <image>
MSG
  exit 2   # distinct from 1: could not verify, rather than verified as wrong
fi

echo "every pin resolves"
