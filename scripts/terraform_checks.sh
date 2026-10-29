#!/usr/bin/env bash
# Static Terraform checks, no AWS access (no backend, mocked provider, plan only).
#   scripts/terraform_checks.sh            # prints and writes evidence/terraform/checks.txt
# Needs terraform, tflint (PATH or .tools/bin) and uv (for checkov via uvx).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/evidence/terraform/checks.txt"
tflint="$(command -v tflint || echo "$root/.tools/bin/tflint")"
# plugins pinned in infra/.tflint.hcl; installed with `tflint --init` (CI) or kept in .tools/
if [ -z "${TFLINT_PLUGIN_DIR:-}" ] && [ -d "$root/.tools/tflint-plugins" ]; then
  export TFLINT_PLUGIN_DIR="$root/.tools/tflint-plugins"
fi
cd "$root/infra"

{
  echo "# $(date -u +%FT%TZ)  $(terraform version | head -1), tflint $("$tflint" --version | head -1 | awk '{print $3}')"
  echo "## terraform fmt -check -recursive"
  terraform fmt -check -recursive
  echo pass
  for dir in modules/secure_bucket bootstrap environments/dev; do
    terraform -chdir="$dir" init -backend=false -input=false -lockfile=readonly >/dev/null
    echo "## $dir: validate"
    terraform -chdir="$dir" validate -no-color
    echo "## $dir: test"
    if [ -d "$dir/tests" ]; then terraform -chdir="$dir" test -no-color | grep -E 'run "|Success|Failure|passed|failed'; fi
    echo "## $dir: tflint"
    "$tflint" --chdir="$dir" --config="$PWD/.tflint.hcl"
    echo "0 issues"
  done
  echo "## checkov 3.4.0"
  uvx --python 3.12 checkov@3.4.0 -d . --framework terraform --compact --quiet
} 2>&1 | tee "$out"
