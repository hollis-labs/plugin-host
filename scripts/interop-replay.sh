#!/usr/bin/env bash
# Isolated fixture assets and genuine host replay; never a consumer module pin.
set -euo pipefail
: "${TMPDIR:?team scratch required}"
: "${INTEROP_SDK_REPO:?local SDK Git repository required}"
pin=ea8ec0dca862d0c7284cc6a130a4b27fb812ed21
scratch="$TMPDIR/plugin-host-interop-$pin"
source_dir="$scratch/source"
mkdir -p "$scratch"
if [[ ! -f "$source_dir/.interop-source-commit" ]]; then
  stage=$(mktemp -d "$scratch/source.XXXXXX")
  git -C "$INTEROP_SDK_REPO" archive "$pin" | tar -x -C "$stage"
  printf '%s\n' "$pin" > "$stage/.interop-source-commit"
  mv "$stage" "$source_dir"
fi
[[ $(cat "$source_dir/.interop-source-commit") == "$pin" ]]
[[ $(sha256sum "$source_dir/protocol/v2/fixtures/duplex-child.json" | cut -d' ' -f1) == 30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9 ]]
export GOMAXPROCS=4 GOFLAGS=-p=2 GOWORK=off GOTMPDIR="$TMPDIR"
receipt="$pin|$(go version)|GOMAXPROCS=4|GOFLAGS=-p=2|race=false"
child="$scratch/duplex-child"
if [[ ! -x "$child" || ! -f "$scratch/build-receipt" || $(cat "$scratch/build-receipt") != "$receipt" ]]; then
  (cd "$source_dir" && heavytest go test -p 2 -c ./subprocess -o "$child.building")
  mv "$child.building" "$child"
  printf '%s\n' "$receipt" > "$scratch/build-receipt"
fi
ts_receipt="$pin|$(node --version)|$(npm --version)|npm-ci-ignore-scripts|workspace-build"
if [[ ! -f "$scratch/ts-build-receipt" || $(cat "$scratch/ts-build-receipt") != "$ts_receipt" ]]; then
  (cd "$source_dir/ts" && heavytest bash -c 'npm ci --ignore-scripts && npm run build')
  printf '%s\n' "$ts_receipt" > "$scratch/ts-build-receipt"
fi
export INTEROP_SDK_SOURCE="$source_dir" INTEROP_GO_CHILD="$child"
export INTEROP_REPORT="$scratch/replay-report.json"
export INTEROP_RECEIVED_REPORT="$scratch/received-budget-report.json"
export INTEROP_CONTROL_REPORT="$scratch/control-report.json"
export INTEROP_HOST_COMMIT="$(git rev-parse HEAD)"
export INTEROP_HOST_DIRTY="$(if [[ -n $(git status --porcelain) ]]; then printf true; else printf false; fi)"
export INTEROP_GO_VERSION="$(go version)" INTEROP_NODE_VERSION="$(node --version)" INTEROP_DENO_VERSION="$(deno --version)"
heavytest go test -p 2 -run "${INTEROP_TEST_PATTERN:-^TestSDKManifest}" -v . | tee "$scratch/run-$(date -u +%Y%m%dT%H%M%SZ).log"
