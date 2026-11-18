#!/bin/sh
# Build the release and the same release with the two upstream TakeMany changes.
set -eu
out=${1:?provide a new output directory}
mkdir -p "$out"
out=$(cd "$out" && pwd)
cd "$(dirname "$0")"
repo=$(git rev-parse --show-toplevel)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
processor=processor/tailsamplingprocessor
storage=extension/tailstorage/pebbletailstorageextension
module=github.com/open-telemetry/opentelemetry-collector-contrib
export GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=go1.26.0 GOWORK=off
go build -mod=readonly -modfile=v165.mod -trimpath -ldflags='-s -w' -o "$out/release" .
git -C "$repo" archive v0.165.1 "$processor" "$storage" > "$scratch/base.tar"
tar -xf "$scratch/base.tar" -C "$scratch"
apply_change() {
    git -C "$repo" diff "$1^" "$1" -- "$2/*.go" > "$scratch/upstream.patch"
    (cd "$scratch" && git apply --no-index upstream.patch)
}
apply_change 87a35b1875b "$processor"
apply_change 4d125cc12a1 "$storage"
cp v165.mod "$scratch/patched.mod"
cp v165.sum "$scratch/patched.sum"
go mod edit -modfile="$scratch/patched.mod" \
    -replace="$module/$processor=$scratch/$processor" \
    -replace="$module/$storage=$scratch/$storage"
go build -mod=readonly -modfile="$scratch/patched.mod" -trimpath -ldflags='-s -w' -o "$out/batched" .
