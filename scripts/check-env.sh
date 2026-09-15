#!/usr/bin/env bash
# Guards this project's .env against the one way it has actually broken:
# junk on a line. Compose rejects any key containing a space, and the error
# names a line number with no other context, so catch it here with a
# message that says what is actually wrong.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
fail=0
ok()  { printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }

[ -f .env ] || { bad ".env missing. Run: make bootstrap"; exit 1; }

# 1. Every meaningful line must be KEY=VALUE. Catches pasted prose.
if junk=$(grep -nvE '^[[:space:]]*(#|$)' .env | grep -vE '^[0-9]+:[A-Za-z_][A-Za-z0-9_]*=' ); then
  bad "line(s) in .env are not KEY=VALUE:"; echo "$junk" | sed 's/^/          /'
else
  ok "every .env line is a valid KEY=VALUE"
fi

# 2. Secrets must have been generated, not left as the committed placeholder.
#    Both must also be identical across every Airflow container; a mismatch
#    surfaces only as "Invalid auth token: Signature verification failed".
for v in AIRFLOW_JWT_SECRET AIRFLOW_FERNET_KEY; do
  val=$(grep -E "^${v}=" .env | head -1 | cut -d= -f2-)
  if [ "$val" = "GENERATE_ME" ] || [ -z "$val" ]; then
    bad "${v} is not set. Run: make bootstrap"
  else
    ok "${v} is set"
  fi
done

# 3. The path this project cannot run without.
v=PROJECT_ROOT
val=$(grep -E "^${v}=" .env | head -1 | cut -d= -f2- | tr -d '"')
if [ -z "$val" ];      then bad "${v} is unset in .env. Run: make bootstrap"
elif [ ! -d "$val" ];  then bad "${v}=${val} does not exist"
else                        ok "${v} resolves"; fi

echo
[ "$fail" -eq 0 ] || { echo "project .env check FAILED"; exit 1; }
echo "project .env check passed"
