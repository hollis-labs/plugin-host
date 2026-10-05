#!/usr/bin/env bash
# Isolated reviewed-candidate assets; no SDK module/release pin or old receipt mutation.
set -euo pipefail
: "${TMPDIR:?owned scratch required}"
: "${INTEROP_SDK_REPO:?local SDK repository required}"
: "${INTEROP_LIFECYCLE_SDK_HEAD:?explicitly permitted immutable SDK candidate required}"
: "${INTEROP_LIFECYCLE_MANIFEST_SHA256:?permitted candidate manifest hash required}"
[[ $INTEROP_LIFECYCLE_SDK_HEAD =~ ^[0-9a-f]{40}$ ]]
[[ $INTEROP_LIFECYCLE_MANIFEST_SHA256 =~ ^[0-9a-f]{64}$ ]]
pin=$INTEROP_LIFECYCLE_SDK_HEAD
base=ea8ec0dca862d0c7284cc6a130a4b27fb812ed21
[[ $(git -C "$INTEROP_SDK_REPO" rev-parse "$pin^{commit}") == "$pin" ]]
git -C "$INTEROP_SDK_REPO" merge-base --is-ancestor "$base" "$pin"
scratch="${INTEROP_LIFECYCLE_ASSET_DIR:-$TMPDIR/plugin-host-lifecycle-compatible-$pin}"
source_dir="$scratch/source"
mkdir -p "$scratch"
if [[ ! -f "$source_dir/.interop-source-commit" ]]; then
  stage=$(mktemp -d "$scratch/source.XXXXXX")
  git -C "$INTEROP_SDK_REPO" archive "$pin" | tar -x -C "$stage"
  printf '%s\n' "$pin" > "$stage/.interop-source-commit"
  mv "$stage" "$source_dir"
fi
[[ $(cat "$source_dir/.interop-source-commit") == "$pin" ]]
[[ $(sha256sum "$source_dir/protocol/v2/fixtures/duplex-child-lifecycle-compatible-v2.json" | cut -d' ' -f1) == "$INTEROP_LIFECYCLE_MANIFEST_SHA256" ]]
export GOMAXPROCS=4 GOFLAGS=-p=2 GOWORK=off GOTMPDIR="$TMPDIR"
receipt="$pin|$(go version)|GOMAXPROCS=4|GOFLAGS=-p=2|race=false"
child="$scratch/duplex-child"
if [[ -f "$scratch/asset-import-receipt.json" ]]; then
  # Reuse the explicitly permitted SDK-author build; never silently rebuild it.
  : "${INTEROP_LIFECYCLE_IMPORT_SHA256:?permitted import receipt hash required for reuse}"
  [[ $(sha256sum "$scratch/asset-import-receipt.json" | cut -d' ' -f1) == "$INTEROP_LIFECYCLE_IMPORT_SHA256" ]]
  python3 - "$scratch" "$pin" <<'PYVERIFY'
import hashlib, json, pathlib, sys
root, pin = pathlib.Path(sys.argv[1]), sys.argv[2]
receipt = json.loads((root / 'asset-import-receipt.json').read_bytes())
if receipt['sdk_head'] != pin:
    raise SystemExit('foreign SDK asset import')
child = root / 'duplex-child'
if hashlib.sha256(child.read_bytes()).hexdigest() != receipt['go_child_sha256']:
    raise SystemExit('changed imported Go child')
checked = set()
for entry in receipt['asset_checks']:
    name = entry['path']
    if not name.startswith('ts-dist/'):
        continue
    relative = pathlib.PurePosixPath(name).relative_to('ts-dist')
    if '..' in relative.parts:
        raise SystemExit('invalid imported TS asset path')
    target = root / 'source/ts/packages/plugin-sdk/dist' / relative
    raw = target.read_bytes()
    if len(raw) != entry['bytes'] or hashlib.sha256(raw).hexdigest() != entry['sha256']:
        raise SystemExit('changed imported TS asset: ' + name)
    checked.add(relative.as_posix())
actual = {p.relative_to(root / 'source/ts/packages/plugin-sdk/dist').as_posix()
          for p in (root / 'source/ts/packages/plugin-sdk/dist').rglob('*') if p.is_file()}
if not checked or checked != actual:
    raise SystemExit('incomplete imported TS asset inventory')
for name in ['build-receipt', 'ts-build-receipt']:
    if not (root / name).is_file():
        raise SystemExit('missing original SDK build attribution')
PYVERIFY
else
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
fi
(
  cd "$source_dir"
  sha256sum protocol/v2/fixtures/duplex-child-lifecycle-compatible-v2.json \
    ts/packages/plugin-sdk/test/lifecycle-compatible-selection.js \
    ts/packages/plugin-sdk/test/lifecycle-compatible-replay.js \
    ts/packages/plugin-sdk/test/negotiated-worker.js \
    ts/packages/plugin-sdk/test/child-cases-worker.js \
    ts/packages/plugin-sdk/dist/index.js
  sha256sum "$child"
) > "$scratch/assets.sha256"
export INTEROP_SDK_SOURCE="$source_dir" INTEROP_GO_CHILD="$child" INTEROP_LIFECYCLE_REPLAY=1
export INTEROP_LIFECYCLE_REPORT="$scratch/public-replay-report.json"
export INTEROP_HOST_COMMIT="$(git rev-parse HEAD)"
export INTEROP_HOST_DIRTY="$(if [[ -n $(git status --porcelain) ]]; then printf true; else printf false; fi)"
export INTEROP_GO_VERSION="$(go version)" INTEROP_NODE_VERSION="$(node --version)" INTEROP_DENO_VERSION="$(deno --version)"
heavytest go test -p 2 -run '^TestSDKLifecycleCompatiblePublicReplay$' -count=1 -timeout=30s -v . 2>&1 | tee "$scratch/public-run.log"
printf 'Isolated public candidate receipt: %s\n' "$INTEROP_LIFECYCLE_REPORT"
