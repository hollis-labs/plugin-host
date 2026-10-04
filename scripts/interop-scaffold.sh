#!/usr/bin/env bash
# Build the pinned test-only child once and inventory the host replay obligations.
set -euo pipefail
: "${TMPDIR:?team scratch directory must be set}"
: "${INTEROP_SDK_REPO:?set to the local plugin-sdk Git checkout}"
pin=90adf1f02ddffde708fa3784f0063bab5171462b
base_pin=d04ab2149506a96e8c54b58829f58ee480e0de41
host_root=$(git rev-parse --show-toplevel)
scratch="$TMPDIR/plugin-host-interop-$pin"
source_dir="$scratch/source"
mkdir -p "$scratch"
if [[ ! -f "$source_dir/.interop-source-commit" ]]; then
  source_stage=$(mktemp -d "$scratch/source.XXXXXX")
  git -C "$INTEROP_SDK_REPO" archive "$pin" | tar -x -C "$source_stage"
  printf '%s\n' "$pin" > "$source_stage/.interop-source-commit"
  mv "$source_stage" "$source_dir"
fi
[[ $(cat "$source_dir/.interop-source-commit") == "$pin" ]]
compiler=$(go version)
git -C "$INTEROP_SDK_REPO" cat-file -e "$base_pin^{commit}"
child="$scratch/duplex-child"
# The compilation receipt also distinguishes race instrumentation and toolchain.
receipt="$pin|$compiler|GOMAXPROCS=4|GOFLAGS=-p=2|race=false"
if [[ ! -x "$child" || ! -f "$scratch/build-receipt" || $(cat "$scratch/build-receipt") != "$receipt" ]]; then
  (
    cd "$source_dir"
    GOWORK=off GOMAXPROCS=4 GOFLAGS=-p=2 heavytest go test -p 2 -c ./subprocess -o "$child.building"
  )
  mv "$child.building" "$child"
  printf '%s\n' "$receipt" > "$scratch/build-receipt"
fi
cd "$host_root"
INTEROP_SDK_SOURCE="$source_dir" INTEROP_GO_CHILD="$child" \
INTEROP_HOST_COMMIT=$(git rev-parse HEAD) \
INTEROP_HOST_DIRTY=$(if [[ -n $(git status --porcelain) ]]; then printf true; else printf false; fi) \
INTEROP_SDK_BASE="$base_pin" INTEROP_REPORT="$scratch/report.json" \
GOWORK=off GOMAXPROCS=4 GOFLAGS=-p=2 \
heavytest go test -p 2 ./pluginhosttest -run '^TestSDKInteropScaffold$' -v
printf 'Scaffold report: %s\n' "$scratch/report.json"
