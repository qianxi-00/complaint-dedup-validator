#Requires -Version 7
<#
.SYNOPSIS
    内网交付构建脚本:测试 → Docker 多阶段构建(PyArmor 混淆 + 授权烧录) → 导出镜像包。
.DESCRIPTION
    产物(build/dist/):
      - 应用镜像 tar.gz(含混淆后代码,授权日期烧录)
      - postgres:16-alpine 镜像 tar.gz
      - compose.intranet.yaml + env 模板 + SHA256 清单
.EXAMPLE
    .\scripts\build_intranet.ps1 -ExpireDate 2027-09-01
    .\scripts\build_intranet.ps1 -ExpireDate 2027-09-01 -PyArmorLicense .\pyarmor-license.zip
    .\scripts\build_intranet.ps1 -ExpireDate 2027-09-01 -SkipDocker   # 只做测试+staging,不构建镜像
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$ExpireDate,
    [string]$ImageName = "complaint-dedup-validator-intranet",
    [string]$ImageTag = "",
    [string]$WorkDir = "build/intranet",
    [string]$OutputDir = "build/dist",
    [string]$PyArmorLicense = "",
    [switch]$RequireFullObfuscation,
    [switch]$SkipTests,
    [switch]$SkipDocker
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    # ---- 0. 校验授权日期(YYYY-MM-DD,必须晚于今天) ----
    if ($ExpireDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
        throw "EXPIRE_DATE 格式必须为 YYYY-MM-DD,收到: $ExpireDate"
    }
    $expire = [datetime]::ParseExact($ExpireDate, 'yyyy-MM-dd', $null)
    if ($expire.Date -le (Get-Date).Date) {
        throw "EXPIRE_DATE ($ExpireDate) 必须晚于今天"
    }
    if (-not $ImageTag) { $ImageTag = $ExpireDate }
    $requireFull = if ($RequireFullObfuscation) { "1" } else { "0" }
    if ($RequireFullObfuscation -and -not $PyArmorLicense) {
        throw "启用 -RequireFullObfuscation 时必须提供付费 PyArmor 注册文件"
    }
    Write-Host "== 授权到期日: $ExpireDate (Asia/Shanghai) ==" -ForegroundColor Cyan

    # ---- 1. 源码测试闸门 ----
    if (-not $SkipTests) {
        Write-Host "== [1/5] 运行全量测试(源码) ==" -ForegroundColor Cyan
        uv run pytest -q
        if ($LASTEXITCODE -ne 0) { throw "源码测试未通过" }
    } else {
        Write-Host "== [1/5] 跳过源码测试(-SkipTests) ==" -ForegroundColor Yellow
    }

    # ---- 2. 组装 Docker 构建上下文 ----
    Write-Host "== [2/5] 组装构建上下文 ($WorkDir/app) ==" -ForegroundColor Cyan
    $staging = Join-Path $WorkDir "app"
    if (Test-Path $WorkDir) { Remove-Item -Recurse -Force $WorkDir }
    New-Item -ItemType Directory -Force -Path $staging, "$staging/licenses", $OutputDir | Out-Null

    Copy-Item pyproject.toml, uv.lock, Dockerfile.intranet $staging
    Copy-Item templates, static, alembic $staging -Recurse
    Copy-Item alembic.ini $staging
    Copy-Item src $staging -Recurse
    Copy-Item tests $staging -Recurse
    Copy-Item scripts $staging -Recurse

    # 混淆构建上下文中的排除项(源码只在构建机本地,不进最终镜像)
    @'
.git
runtime
output
docs
deploy
scripts/build_intranet.ps1
**/__pycache__
**/*.pyc
*.db
*.log
.env
'@ | Set-Content "$staging/.dockerignore" -Encoding UTF8

    # PyArmor 商业许可证(可选)
    if ($PyArmorLicense) {
        if (-not (Test-Path $PyArmorLicense)) { throw "许可证文件不存在: $PyArmorLicense" }
        if ([IO.Path]::GetExtension($PyArmorLicense).ToLowerInvariant() -ne ".zip") {
            throw "Docker 构建只接受 PyArmor .zip 注册文件；请先在构建机激活 .txt 注册码"
        }
        Copy-Item $PyArmorLicense "$staging/licenses/"
        Write-Host "  已包含 PyArmor 许可证: $PyArmorLicense" -ForegroundColor Green
    } else {
        Write-Host "  未提供 PyArmor 许可证 → 试用模式(超 32KB 文件将保留明文,禁止对外交付)" -ForegroundColor Yellow
    }

    # ---- 3. Docker 构建:tester 阶段(内含混淆+测试闸门)→ 正式镜像 ----
    if ($SkipDocker) {
        Write-Host "== [3/5] 跳过 Docker 构建(-SkipDocker);构建上下文已就绪: $staging ==" -ForegroundColor Yellow
        Write-Host "  手动构建:" 
        Write-Host "  docker build --target tester -f Dockerfile.intranet --build-arg EXPIRE_DATE=`$ExpireDate --build-arg PYARMOR_REQUIRE_FULL=$requireFull `$staging"
        Write-Host "  docker build -f Dockerfile.intranet -t ${ImageName}:$ImageTag --build-arg EXPIRE_DATE=$ExpireDate --build-arg PYARMOR_REQUIRE_FULL=$requireFull $staging"
        return
    }
    Write-Host "== [3/5] Docker 构建:tester 阶段(混淆 + 镜像内测试闸门) ==" -ForegroundColor Cyan
    docker build --target tester -f Dockerfile.intranet --build-arg EXPIRE_DATE=`$ExpireDate --build-arg PYARMOR_REQUIRE_FULL=$requireFull `$staging
    if ($LASTEXITCODE -ne 0) { throw "镜像内测试闸门未通过(混淆产物回归失败)" }

    Write-Host "== [3/5] Docker 构建:runtime 正式镜像 ==" -ForegroundColor Cyan
    docker build -f Dockerfile.intranet -t "${ImageName}:$ImageTag" -t "${ImageName}:latest" --build-arg EXPIRE_DATE=`$ExpireDate --build-arg PYARMOR_REQUIRE_FULL=$requireFull `$staging
    if ($LASTEXITCODE -ne 0) { throw "镜像构建失败" }

    # ---- 4. 导出镜像 ----
    Write-Host "== [4/5] 导出镜像 ==" -ForegroundColor Cyan
    docker pull postgres:16-alpine
    if ($LASTEXITCODE -ne 0) { throw "拉取 postgres:16-alpine 失败" }

    $appTar = Join-Path $OutputDir "${ImageName}-${ImageTag}.tar"
    $pgTar = Join-Path $OutputDir "postgres-16-alpine.tar"
    docker save "${ImageName}:$ImageTag" -o $appTar
    docker save postgres:16-alpine -o $pgTar
    tar -czf "$appTar.gz" -C $OutputDir (Split-Path $appTar -Leaf)
    tar -czf "$pgTar.gz" -C $OutputDir (Split-Path $pgTar -Leaf)
    Remove-Item $appTar, $pgTar

    # ---- 5. 打包部署物料 ----
    Write-Host "== [5/5] 打包部署物料 ==" -ForegroundColor Cyan
    Copy-Item deploy/compose.intranet.yaml $OutputDir
    Copy-Item deploy/env.intranet.example $OutputDir

    $manifest = Join-Path $OutputDir "SHA256SUMS.txt"
    $lines = Get-ChildItem $OutputDir -File | ForEach-Object {
        $hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower()
        "$hash  $($_.Name)"
    }
    $header = @(
        "# 内网交付包 授权到期日: `$ExpireDate (Asia/Shanghai) 镜像: `${ImageName}:`$ImageTag",
        "# 部署步骤见 docs/内网部署与授权说明.md"
    )
    $header + $lines | Set-Content $manifest -Encoding UTF8

    Write-Host ""
    Write-Host "== 构建完成,交付物位于 $OutputDir ==" -ForegroundColor Green
    Get-ChildItem $OutputDir | Format-Table Name, @{N='MB';E={[math]::Round($_.Length/1MB, 1)}} -AutoSize
} finally {
    Pop-Location
}
