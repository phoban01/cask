#!/usr/bin/env bash
# build.sh — render the paper (coordination-without-a-log.md) to a PDF with the
# evaluation figures embedded. Output: build/cask-paper.pdf.
#
# Needs: go, and either pandoc+tectonic on PATH or nix (the script falls back to
# `nix-shell -p pandoc tectonic`). tectonic (XeTeX) handles the paper's Unicode;
# figures are generated from the measured benchmark data by ./mkpdf.
set -euo pipefail
cd "$(dirname "$0")"
PAPER_DIR="$(pwd)"
BUILD="$PAPER_DIR/build"
SRC="$PAPER_DIR/coordination-without-a-log.md"
mkdir -p "$BUILD"

echo "== generating figures =="
( cd mkpdf && go run . figures "$BUILD" )

echo "== preparing markdown (front matter, figures, unicode) =="
( cd mkpdf && go run . prep "$SRC" "$BUILD/paper.md" )

PANDOC="pandoc paper.md -o cask-paper.pdf --pdf-engine=tectonic \
  -V geometry:margin=1in -V fontsize=11pt \
  -V colorlinks=true -V linkcolor=RoyalBlue --toc --toc-depth=2"

echo "== typesetting =="
if command -v pandoc >/dev/null 2>&1 && command -v tectonic >/dev/null 2>&1; then
  ( cd "$BUILD" && eval "$PANDOC" )
elif command -v nix-shell >/dev/null 2>&1; then
  nix-shell -p pandoc tectonic --run "cd '$BUILD' && $PANDOC"
else
  echo "need pandoc+tectonic on PATH or nix-shell available" >&2
  exit 1
fi

echo "== done: $BUILD/cask-paper.pdf =="
