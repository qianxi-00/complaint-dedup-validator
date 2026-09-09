#!/bin/bash
# PyArmor 混淆步骤(Docker builder 阶段内执行,保证运行时文件与 Linux 目标平台匹配)。
# 全量混淆失败(典型原因:试用版单文件 32KB 上限)时退回逐文件模式,
# 超限文件保留明文并打印醒目警告 —— 正式交付必须使用付费许可证走全量模式。
set -u

cd /build
rm -rf obf
if pyarmor gen --output obf src/complaint_dedup; then
    echo "[obfuscate] full-package obfuscation OK"
    exit 0
fi

if [ "${PYARMOR_REQUIRE_FULL:-0}" = "1" ]; then
    echo "[obfuscate] ERROR: full-package obfuscation required but failed" >&2
    exit 1
fi

echo "[obfuscate] WARNING: package-level obfuscation failed (trial size limit?)"
echo "[obfuscate] WARNING: falling back to per-file mode; oversized files stay PLAINTEXT"
echo "[obfuscate] WARNING: a fully encrypted delivery requires the paid PyArmor license"
mkdir -p obf/complaint_dedup
runtime_copied=0
for f in src/complaint_dedup/*.py; do
    name="$(basename "$f")"
    rm -rf tmp_obf
    mkdir -p tmp_obf
    if pyarmor gen --output tmp_obf "$f"; then
        cp "tmp_obf/$name" "obf/complaint_dedup/$name"
        if [ "$runtime_copied" -eq 0 ]; then
            cp -r tmp_obf/pyarmor_runtime_000000 obf/
            runtime_copied=1
        fi
        echo "[obfuscate]   ok: $name"
    else
        cp "$f" "obf/complaint_dedup/$name"
        echo "[obfuscate]   PLAINTEXT (trial limit): $name"
    fi
done
rm -rf tmp_obf

if [ "$runtime_copied" -eq 0 ]; then
    echo "[obfuscate] ERROR: no runtime generated" >&2
    exit 1
fi
echo "[obfuscate] per-file fallback finished"
