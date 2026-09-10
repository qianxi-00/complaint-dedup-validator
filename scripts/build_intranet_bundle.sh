#!/usr/bin/env bash
set -Eeuo pipefail

IMAGE_NAME="complaint-dedup-validator-intranet"
EXPIRE_DATE=""
IMAGE_TAG=""
WORK_DIR="build/intranet-linux"
OUTPUT_DIR="build/dist-linux"
PYARMOR_ZIP=""
REQUIRE_FULL=0
SKIP_TESTS=0

usage() {
    printf '%s\n' \
        '用法: scripts/build_intranet_bundle.sh --expire-date YYYY-MM-DD [选项]' \
        '--expire-date DATE          必填,上海时区授权到期日' \
        '--image-name NAME           默认 complaint-dedup-validator-intranet' \
        '--image-tag TAG             默认使用到期日' \
        '--work-dir DIR              默认 build/intranet-linux' \
        '--output-dir DIR            默认 build/dist-linux' \
        '--pyarmor-zip FILE          可选,已激活的 PyArmor .zip 注册文件' \
        '--require-full-obfuscation  强制全包混淆,失败即停止' \
        '--skip-tests                跳过源码测试' \
        '-h, --help                  显示帮助'
}

die() {
    echo "[build] ERROR: $*" >&2
    exit 1
}

while (($#)); do
    case "$1" in
        --expire-date) EXPIRE_DATE="${2:-}"; shift 2 ;;
        --image-name) IMAGE_NAME="${2:-}"; shift 2 ;;
        --image-tag) IMAGE_TAG="${2:-}"; shift 2 ;;
        --work-dir) WORK_DIR="${2:-}"; shift 2 ;;
        --output-dir) OUTPUT_DIR="${2:-}"; shift 2 ;;
        --pyarmor-zip) PYARMOR_ZIP="${2:-}"; shift 2 ;;
        --require-full-obfuscation) REQUIRE_FULL=1; shift ;;
        --skip-tests) SKIP_TESTS=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "未知参数: $1" ;;
    esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
[[ -n "$EXPIRE_DATE" ]] || die "必须提供 --expire-date"
[[ "$EXPIRE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "到期日期必须为 YYYY-MM-DD"
python3 - "$EXPIRE_DATE" <<'PY'
from datetime import date, datetime
from zoneinfo import ZoneInfo
import sys
expire = date.fromisoformat(sys.argv[1])
today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
if expire <= today:
    raise SystemExit(f"到期日期必须晚于上海当前日期 {today.isoformat()}")
PY

ARCH="$(uname -m)"
[[ "$ARCH" == "x86_64" || "$ARCH" == "amd64" ]] || die "构建机必须是 x86_64,当前为 $ARCH"
command -v docker >/dev/null || die "未找到 docker"
command -v uv >/dev/null || die "未找到 uv"
command -v python3 >/dev/null || die "未找到 python3"
command -v sha256sum >/dev/null || die "未找到 sha256sum"
[[ -n "$IMAGE_TAG" ]] || IMAGE_TAG="$EXPIRE_DATE"
if [[ -n "$PYARMOR_ZIP" ]]; then
    [[ -f "$PYARMOR_ZIP" ]] || die "PyArmor 注册文件不存在: $PYARMOR_ZIP"
    [[ "${PYARMOR_ZIP##*.}" == "zip" ]] || die "不能直接使用 .txt 注册码,请先生成 .zip"
fi
if (( REQUIRE_FULL == 1 )) && [[ -z "$PYARMOR_ZIP" ]]; then
    die "--require-full-obfuscation 必须配合 --pyarmor-zip 使用"
fi
if (( SKIP_TESTS == 0 )); then
    echo "[build] 运行源码测试"
    uv run pytest -q
fi

rm -rf "$WORK_DIR" "$OUTPUT_DIR"
STAGING="$WORK_DIR/app"
mkdir -p "$STAGING/licenses" "$OUTPUT_DIR"
cp pyproject.toml uv.lock Dockerfile.intranet "$STAGING/"
cp -R templates static alembic "$STAGING/"
cp alembic.ini "$STAGING/"
cp -R src tests scripts deploy "$STAGING/"
printf '%s\n' '.git' 'runtime' 'output' 'docs' \
    'scripts/build_intranet.ps1' 'scripts/build_intranet_bundle.sh' \
    '**/__pycache__' '**/*.pyc' '*.db' '*.log' '.env' > "$STAGING/.dockerignore"
if [[ -n "$PYARMOR_ZIP" ]]; then
    cp "$PYARMOR_ZIP" "$STAGING/licenses/"
else
    echo "[build] 未提供 PyArmor .zip,使用免费/试用模式"
fi

DOCKERFILE="$WORK_DIR/Dockerfile.intranet.deploy"
tail -n +2 "$STAGING/Dockerfile.intranet" > "$DOCKERFILE"
BUILD_ARGS=(
    --platform linux/amd64
    --build-arg "EXPIRE_DATE=$EXPIRE_DATE"
    --build-arg "PYARMOR_REQUIRE_FULL=$REQUIRE_FULL"
)
IMAGE_REF="$IMAGE_NAME:$IMAGE_TAG"

echo "[build] 构建 tester 镜像: $IMAGE_REF"
docker build --progress=plain --target tester -f "$DOCKERFILE" "${BUILD_ARGS[@]}" -t "$IMAGE_REF" "$STAGING"
echo "[build] 构建 runtime 镜像: $IMAGE_REF"
docker build --progress=plain --target runtime -f "$DOCKERFILE" "${BUILD_ARGS[@]}" -t "$IMAGE_REF" "$STAGING"
docker tag "$IMAGE_REF" "$IMAGE_NAME:latest"

docker image inspect postgres:16-alpine >/dev/null 2>&1 || docker pull --platform linux/amd64 postgres:16-alpine
APP_TAR="$OUTPUT_DIR/${IMAGE_NAME}-${IMAGE_TAG}.tar"
PG_TAR="$OUTPUT_DIR/postgres-16-alpine.tar"
docker save "$IMAGE_REF" "$IMAGE_NAME:latest" -o "$APP_TAR"
docker save postgres:16-alpine -o "$PG_TAR"
gzip -f "$APP_TAR"
gzip -f "$PG_TAR"
cp deploy/compose.intranet.yaml deploy/env.intranet.example "$OUTPUT_DIR/"
cp docs/内网离线交付与续期部署手册.md "$OUTPUT_DIR/"

MANIFEST="$OUTPUT_DIR/SHA256SUMS.txt"
{
    echo "# Image: $IMAGE_REF"
    echo "# EXPIRE_DATE: $EXPIRE_DATE (Asia/Shanghai)"
    find "$OUTPUT_DIR" -maxdepth 1 -type f ! -name "$(basename "$MANIFEST")" -printf '%f\n' | sort | while read -r file; do
        (cd "$OUTPUT_DIR" && sha256sum "$file")
    done
} > "$MANIFEST"

BUNDLE="$OUTPUT_DIR/${IMAGE_NAME}-bundle-${IMAGE_TAG}.tar.gz"
tar -czf "$BUNDLE" -C "$OUTPUT_DIR" \
    "${IMAGE_NAME}-${IMAGE_TAG}.tar.gz" postgres-16-alpine.tar.gz \
    compose.intranet.yaml env.intranet.example \
    内网离线交付与续期部署手册.md SHA256SUMS.txt

echo "[build] 完成: $BUNDLE"
find "$OUTPUT_DIR" -maxdepth 1 -type f -printf '%f %s bytes\n' | sort
