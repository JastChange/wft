# M1/M2 配置与脚本操作指南

## 1. 准备 Python 3.12

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
wft --help
```

WFT 1.0 锁定 Python `>=3.12,<3.13`，安装包包含 AsyncSSH。CLI 包含 `admin`、`config`、`inventory`、`scripts`、`run` 和 `task` 六组命令。

## 2. 创建私有配置

以 `config/wft.example.yaml` 为字段参考，把真实配置放在不受 Git 管理的位置：

```bash
sudo install -d -m 0700 /etc/wft
sudo install -m 0600 config/wft.example.yaml /etc/wft/wft.yaml
sudo chmod 0600 /run/secrets/scripts-deploy-key
wft config check --config /etc/wft/wft.yaml
```

配置文件必须恰好为 `0600`。`data_dir` 和 `inventory_path` 可以相对配置文件目录书写，加载后会规范化为绝对路径。`script_repository.deploy_key_path` 必须从一开始就是绝对路径，目标文件必须存在且为 `0600`。

脚本 Deploy Key 应在 Git 服务端只授予目标仓库读取权限。WFT 使用 `BatchMode=yes` 和 `IdentitiesOnly=yes` 调用 Git/SSH，不进入密码交互。WFT 直接信任配置的仓库 URL 和分支，不验证 commit 或 tag 签名。

`--json` 成功输出不会包含 Deploy Key 路径或密钥内容。管理员一次性密码只由 `admin` 文本命令显示。

## 3. 初始化管理员密码

```bash
wft admin init --auth-file /var/lib/wft/auth/admin.json
wft admin reset-password --auth-file /var/lib/wft/auth/admin.json
```

`init` 在文件已存在时失败，避免意外覆盖。`reset-password` 原子替换原哈希。两条命令都会生成并显示一次性密码；磁盘只保存 Argon2 哈希，文件权限为 `0600`。

## 4. 维护 Inventory

参考 `config/inventory.example.yaml`：

```yaml
nodes:
  - name: node-a
    host: 10.0.0.11
    port: 22
    username: root
    private_key_path: /run/secrets/node-a-key
    os: ubuntu-24.04
    groups: [batch]
    tags: [memory]
    enabled: true
```

约束：

- 节点名必须唯一；
- 私钥必须使用绝对路径，WFT 不读取或输出私钥正文；
- 目标系统只允许 `ubuntu-22.04` 或 `ubuntu-24.04`；
- 选择结果只包含启用节点，按节点名排序并去重；
- 显式节点、group 和 tag 使用并集语义；运行任务时 `--all` 必须单独使用。

验证与选择：

```bash
wft inventory check --config /etc/wft/wft.yaml
wft inventory select --config /etc/wft/wft.yaml --node node-a
wft inventory select --config /etc/wft/wft.yaml --group batch --tag memory --json
wft inventory select --config /etc/wft/wft.yaml --all
```

不提供任何选择器、显式节点不存在或结果为空时，命令以退出码 `2` 失败。

## 5. 脚本仓库与 Manifest

外部脚本仓库根目录必须包含 `manifest.yaml`。`config/manifest.example.yaml` 和 `config/example-check.sh` 构成可验证的本地示例。

每个脚本登记：

- `id`：仓库内唯一脚本标识；
- `path`：相对仓库根目录且不得逃逸；
- `sha256`：源文件完整 SHA-256；
- `interpreter`：`bash`、`sh` 或 `python3`；
- `timeout_seconds`：`1`～`3600`；
- `expected_exit_codes`：非空的 `0`～`255` 列表；
- `read_only`：必须为 `true`；
- `supported_os`：只包含 Ubuntu 22.04/24.04；
- `plans`：按执行顺序引用已登记脚本。

WFT 的只读声明依赖脚本仓库维护者审核，不做脚本静态沙箱。

## 6. 获取最新脚本和缓存回退

```bash
wft scripts check --config /etc/wft/wft.yaml
wft scripts sync --config /etc/wft/wft.yaml
wft scripts sync --config /etc/wft/wft.yaml --allow-cached-scripts
```

每次命令先 fetch 配置分支。通过 Manifest 和哈希验证后，提交发布到：

```text
<data_dir>/script-snapshots/<commit-sha>/
```

快照元数据记录仓库 URL、分支、commit SHA、提交时间和 Manifest SHA-256；任务执行只能引用该不可变快照，不从可变 Git cache 执行。

fetch 失败时，CLI 先显示缓存 commit、提交时间、仓库和分支。操作员必须在交互提示中确认，或显式传入 `--allow-cached-scripts`。拒绝、无可用缓存或 JSON 模式未提供显式参数时以退出码 `2` 失败。成功 JSON 中以 `used_cached_snapshot` 记录是否使用缓存。

## 7. M1/M2 退出码

- `0`：命令完成并通过验证；
- `1`：任务完成，但发现问题，或安装验证结论为 `FAIL/INCONCLUSIVE`；
- `2`：配置、Inventory、Manifest、Git、缓存授权、文件操作失败，或任务未完成/被取消。

任务创建、执行、查询与删除见[任务操作指南](tasks.md)。
