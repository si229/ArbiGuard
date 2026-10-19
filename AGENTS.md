# ArbiGuard AI 工作规范

## GitHub 唯一托管规则

- 本机可修改的项目仓库只保留 GitHub fetch、push URL，不得添加或恢复 Gitee 等其他镜像远程；活动依赖配置和锁文件只使用 GitHub。
- 各工程的 `push-all.ps1`（有 `scripts/` 目录的放在 `scripts/`，否则放仓库根目录）只推当前分支、不强推，推完取回并核对远端；`game_wz_ops/scripts/sync-sibling-repos.ps1` 先对各工程 `git pull --ff-only`，再逐个调用它。新建工程时一并补上这个脚本。
- 切换依赖托管地址时保留已锁定的准确提交并确认 GitHub 可获取，不借此升级业务依赖或重写历史。
- Git 操作使用非交互方式；凭据不可用时报告明确失败，不触发登录弹窗。远端权限仍遵循当前任务授权，三个既定只读参考仓库保持排除。

## Erlang 基础调试规则

- 所有 Erlang 代码仓库必须直接依赖并锁定已验证版本的 `observer_cli`，提供统一的 `wz_trace` 模块，方便查看节点状态和定位函数调用问题。
- 独立运行的服务必须把 `observer_cli`、其所需的 `recon` 以及 `wz_trace` 纳入实际运行包；各发布 profile 和 escript 入口都要核对，不能只在测试环境可用。
- 被其他服务引用的库在独立 `debug_src/` 中提供 `wz_trace`，通过根 `extra_src_dirs` 供本仓 Shell 和测试使用；不将它编入库的 `ebin` 或 `.app` 模块清单，避免依赖图中重复装载同名模块。服务使用自己的唯一调试入口。
- 复用现有统一实现；追踪默认有条数限制，支持限定 PID，并可通过 `wz_trace:clear_all/0` 显式停止。不在启动路径自动开启交互 Observer 或 trace，不把敏感业务数据写入调试输出。
- 变更后验证模块可加载、依赖和发布清单完整、实际受限追踪可正常结束；库还须验证调试模块没有进入被消费的应用模块清单。
