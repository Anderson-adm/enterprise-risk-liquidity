#!/usr/bin/env bash
# Build build/lambda.zip for the three ingestion Lambdas (python3.12, x86_64).
#   * erl package (only the modules the handlers import + contracts/XSDs)
#   * lxml wheel for manylinux x86_64 / CPython 3.12
#   * pyarrow and boto3 are NOT bundled: pyarrow comes from the AWS-managed
#     "AWS SDK for pandas" layer, boto3 from the Lambda runtime.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=build/lambda
ZIP=build/lambda.zip
LXML_VERSION=$(uv export --frozen --no-hashes --format requirements-txt | sed -n 's/^lxml==\([0-9.]*\).*/\1/p')

rm -rf "$OUT" "$ZIP"
mkdir -p "$OUT/erl"
uv pip install --quiet --target "$OUT" --python-platform x86_64-manylinux_2_28 --python-version 3.12 \
  --only-binary :all: --no-deps "lxml==${LXML_VERSION}"

for f in __init__.py errors.py log.py storage.py; do cp "src/erl/$f" "$OUT/erl/"; done
mkdir -p "$OUT/erl/ingestion" "$OUT/erl/aws" "$OUT/erl/contracts"
cp src/erl/ingestion/{__init__,bronze,files}.py "$OUT/erl/ingestion/"
cp src/erl/aws/{__init__,lambda_handlers}.py "$OUT/erl/aws/"
cp src/erl/contracts/{__init__.py,*.json,*.xsd} "$OUT/erl/contracts/"

# deterministic archive: fixed timestamps and sorted entries
find "$OUT" -name '__pycache__' -prune -exec rm -rf {} +
python3 - "$OUT" "$ZIP" <<'PY'
import os, sys, zipfile
src, dst = sys.argv[1], sys.argv[2]
files = sorted(os.path.join(r, f) for r, _, fs in os.walk(src) for f in fs)
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    for path in files:
        info = zipfile.ZipInfo(os.path.relpath(path, src), date_time=(2020, 1, 1, 0, 0, 0))
        info.external_attr = 0o644 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        with open(path, "rb") as fh:
            z.writestr(info, fh.read())
PY
echo "built $ZIP ($(du -h "$ZIP" | cut -f1)), lxml ${LXML_VERSION}, sha256 $(sha256sum "$ZIP" | cut -c1-16)"
