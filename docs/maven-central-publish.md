# Maven Central 发布说明

本文档说明如何为当前仓库配置 GPG，并使用 GitHub Actions 发布到 Maven Central。

## 1. GitHub Secrets

当前工作流依赖以下 Secrets：

- `CENTRAL_TOKEN_USERNAME`
- `CENTRAL_TOKEN_PASSWORD`
- `GPG_PRIVATE_KEY`

其中：

- `CENTRAL_TOKEN_USERNAME` 和 `CENTRAL_TOKEN_PASSWORD` 来自 Sonatype Central Portal 的 User Token
- `GPG_PRIVATE_KEY` 是 ASCII Armor 格式的私钥全文

当前仓库工作流按“无密码私钥”配置，不再要求 `GPG_PASSPHRASE`。

## 2. 本地生成 GPG 密钥

建议使用无密码的 RSA 4096 位密钥，以匹配当前仓库的 GitHub Actions 发布流程。

```bash
gpg --full-generate-key
```

推荐选项：

- 密钥类型：`RSA and RSA`
- 长度：`4096`
- 过期时间：按需设置
- 姓名：建议填你的 GitHub 名称或发布者名称
- 邮箱：建议和 `pom.xml` 的开发者邮箱一致

生成完成后，查看密钥：

```bash
gpg --list-secret-keys --keyid-format LONG
```

示例输出：

```text
sec   rsa4096/0123456789ABCDEF 2026-04-14 [SC]
      1111222233334444555566667777888899990000
uid                 [ultimate] jolinleaf <xiaoyaoyunlian@gmail.com>
ssb   rsa4096/FEDCBA9876543210 2026-04-14 [E]
```

其中主密钥 ID 类似 `0123456789ABCDEF`。

## 3. 导出 GitHub Actions 使用的私钥

导出 ASCII Armor 私钥：

```bash
gpg --armor --export-secret-keys 0123456789ABCDEF
```

把完整输出内容复制到 GitHub Secret `GPG_PRIVATE_KEY`。

## 4. 可选：导出并上传公钥

为了让 Central 和外部用户能验证签名，建议公开上传公钥：

```bash
gpg --armor --export 0123456789ABCDEF
```

可以上传到常见 key server，或放在项目文档中。

## 5. 发布流程

当前仓库的工作流文件：

- `.github/workflows/publish-maven-central.yml`

支持两种触发方式：

### 方式一：GitHub Release

发布一个 tag，例如：

- `v1.0.0`

然后创建 GitHub Release，工作流会自动：

1. 检出代码
2. 安装 JDK 25
3. 导入 GPG 私钥
4. 把 Maven 版本临时设置为 `1.0.0`
5. 运行 `mvn -Prelease clean deploy`
6. 上传并等待 Central 发布完成

### 方式二：手动触发

在 GitHub Actions 页面手动运行工作流，并填写：

- `release_version=1.0.0`

## 6. 本地发布前自检

建议先在本地确认基础构建通过：

```bash
mvn test
```

如果要模拟 release profile，可以在本地执行：

```bash
mvn -Prelease -DskipTests package
```

注意：

- 如果本地没有导入 GPG 私钥，签名相关步骤会失败
- 如果当前 Maven `settings.xml` 强制镜像到不可用私服，先切回 Maven Central

## 7. 当前仓库的发布约束

当前 `pom.xml` 已经补齐以下 Maven Central 必需项：

- 项目坐标
- 项目描述
- 项目主页 URL
- SCM 信息
- 开发者信息
- License 信息
- source jar
- javadoc jar
- GPG 签名
- Central Portal 发布插件

如果后续你调整仓库归属、邮箱、许可证或发布域名，请同步修改 `pom.xml`。
