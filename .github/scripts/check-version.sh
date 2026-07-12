#!/usr/bin/env bash
# tauri.conf.json / Cargo.toml / package.json のバージョン一致を検証する。
# 引数に期待バージョン(リリースタグ由来)を渡すと、それとの一致も検証する。
set -euo pipefail

cd "$(dirname "$0")/../.."

v_tauri=$(jq -r .version src-tauri/tauri.conf.json)
v_pkg=$(jq -r .version package.json)
# 行頭の `version = "..."` は [package] のバージョンのみ(依存関係はインライン表記のため一致しない)
v_cargo=$(sed -n 's/^version *= *"\(.*\)"/\1/p' src-tauri/Cargo.toml | head -n1)

echo "tauri.conf.json: ${v_tauri}"
echo "package.json:    ${v_pkg}"
echo "Cargo.toml:      ${v_cargo}"

if [[ "${v_tauri}" != "${v_pkg}" || "${v_tauri}" != "${v_cargo}" ]]; then
  echo "::error::バージョンが一致しません (tauri.conf.json=${v_tauri}, package.json=${v_pkg}, Cargo.toml=${v_cargo})"
  exit 1
fi

if [[ $# -ge 1 && "${v_tauri}" != "$1" ]]; then
  echo "::error::タグのバージョン ($1) とファイルのバージョン (${v_tauri}) が一致しません"
  exit 1
fi

echo "version=${v_tauri}"
