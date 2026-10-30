#!/usr/bin/env bash
# Static Terraform checks, no AWS access (no backend, mocked provider, plan only).
#   scripts/terraform_checks.sh            # prints and writes evidence/terraform/checks.txt
# Needs terraform, tflint (PATH or .tools/bin) and uv (for checkov via uvx).
#
# The checks run on a temporary copy of the versioned files, with AWS credentials hidden:
# a local .terraform/ left by a real deployment (S3 backend metadata), *.tfvars or backend.hcl
# can never be picked up, and no command can open an AWS session.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
out="${ERL_TF_CHECKS_OUT:-$root/evidence/terraform/checks.txt}"
tflint="$(command -v tflint || echo "$root/.tools/bin/tflint")"
# plugins pinned in infra/.tflint.hcl; installed with `tflint --init` (CI) or kept in .tools/
if [ -z "${TFLINT_PLUGIN_DIR:-}" ] && [ -d "$root/.tools/tflint-plugins" ]; then
  export TFLINT_PLUGIN_DIR="$root/.tools/tflint-plugins"
fi
if [ -z "${TF_PLUGIN_CACHE_DIR:-}" ] && [ -d "$root/.tools" ]; then
  export TF_PLUGIN_CACHE_DIR="$root/.tools/terraform-plugin-cache"
  mkdir -p "$TF_PLUGIN_CACHE_DIR"
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
if git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  (cd "$root" && git ls-files -co --exclude-standard -z | tar --null --ignore-failed-read -T - -cf - | tar -C "$work" -xf -)
else # copy used by the script's own tests (no git checkout)
  tar -C "$root" --exclude=.terraform --exclude='*.tfvars' --exclude=backend.hcl -cf - infra | tar -C "$work" -xf -
fi
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_EC2_METADATA_DISABLED=true
cd "$work/infra"

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
    # dist/ is built by scripts/build and not versioned; lint dev against the test fixtures
    vars=(); [ -d "$dir/tests/fixtures/dist" ] && vars=(--var="artifacts_dir=tests/fixtures/dist")
    "$tflint" --chdir="$dir" --config="$PWD/.tflint.hcl" "${vars[@]}"
    echo "0 issues"
  done
  echo "## checkov 3.4.0"
  uvx --python 3.12 checkov@3.4.0 -d . --framework terraform --compact --quiet
} 2>&1 | tee "$out"
