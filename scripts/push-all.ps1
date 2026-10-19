<#
.SYNOPSIS
把当前分支推到本仓库已配置的全部远端（本机只保留 GitHub），推完取回并核对远端分支与本地 HEAD 一致。

.EXAMPLE
.\scripts\push-all.ps1
#>

# 不强推：远端拒绝（例如远端有本地没有的提交）就停下，先 git pull --ff-only 或合并后再重试。
# 按去重后的 push URL 各推一次，同一地址不会被推两遍。
# 推 URL 不会刷新跟踪分支，所以最后 fetch 全部远端，避免 git status 显示过期的 "ahead N"。
# game_wz_ops/scripts/sync-sibling-repos.ps1 会先拉取各工程，再逐个调用本脚本。

$repo = git rev-parse --show-toplevel
if ($LASTEXITCODE -ne 0) { Write-Error '当前目录不在 Git 仓库里'; exit 1 }
Set-Location $repo

$branch = git symbolic-ref --quiet --short HEAD
if ($LASTEXITCODE -ne 0) { Write-Error '当前处于分离 HEAD，先切到要推送的分支'; exit 1 }
$head = git rev-parse HEAD

$remotes = @(git remote)
$urls = @($remotes | ForEach-Object { git remote get-url --all --push $_ } | Sort-Object -Unique)

foreach ($url in $urls) {
    Write-Host "==> 推送 $branch 到 $url"
    git push $url "HEAD:refs/heads/$branch"
    if ($LASTEXITCODE -ne 0) { Write-Error "推送到 $url 失败"; exit 1 }
}

git fetch --all --prune --quiet
if ($LASTEXITCODE -ne 0) { Write-Error '取回远端失败，无法核对推送结果'; exit 1 }

$mismatch = $false
foreach ($remote in $remotes) {
    $remoteHead = git rev-parse --verify --quiet "refs/remotes/$remote/$branch"
    if ($remoteHead -eq $head) {
        Write-Host "OK   $remote/$branch = $($head.Substring(0, 7))" -ForegroundColor Green
    } else {
        Write-Host "不一致 $remote/$branch = $remoteHead，本地 HEAD = $head" -ForegroundColor Red
        $mismatch = $true
    }
}
git status -sb
if ($mismatch) { exit 1 }
