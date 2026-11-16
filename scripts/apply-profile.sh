#!/usr/bin/env bash
# Apply a resource profile (config/profiles/<name>.env) to .env.
#
# Only the keys the profile lists are rewritten, in place, so secrets and paths
# that `make bootstrap` generated are never touched. A key missing from .env is
# appended. Running it twice changes nothing.
#
# Usage: scripts/apply-profile.sh small|large
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

name="${1:-}"
[ -n "$name" ] || { echo "usage: $0 <profile>   (one of: $(ls config/profiles | sed 's/\.env$//' | tr '\n' ' '))"; exit 2; }
profile="config/profiles/${name}.env"
[ -f "$profile" ] || { echo "no such profile: $name"; exit 2; }
[ -f .env ] || { echo "no .env. Run: make bootstrap"; exit 2; }

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
cp .env "$tmp"

while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  key="${line%%=*}"
  if grep -qE "^${key}=" "$tmp"; then
    # Rewrite via awk, not sed: values contain characters (quotes, slashes)
    # that make a sed replacement string fragile.
    awk -v k="$key" -v l="$line" 'BEGIN{FS=OFS="="} $1==k {print l; next} {print}' "$tmp" > "$tmp.new"
    mv "$tmp.new" "$tmp"
  else
    printf '%s\n' "$line" >> "$tmp"
  fi
done < "$profile"

cp "$tmp" .env
echo "applied profile '${name}' to .env"
