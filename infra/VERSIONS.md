# Pinned versions

Every tool and image that builds, runs or checks HDS, in one place (`docs/hds-api.md` §10
Dependencies: "Container base images pinned by version"). Each one is used at exactly this version
wherever it appears. Update a row and every use together, in one pull request.

Tools are pinned to a release at least two weeks old when it was chosen, so a broken release has
time to be pulled. Images are pinned by tag and by the digest of their multi-platform index, so the
same bytes run on `linux/amd64` (CI) and `linux/arm64` (the operator's Mac).

Chosen on 2026-11-06 (specs/006-deploy-operations, T001).

## Images

| Image | Tag | Digest | Used for |
| --- | --- | --- | --- |
| `python` | `3.12.16-slim` | `sha256:4840d514c583a4d3d8cad3940e68dee6f0f6f3635c12fcd0a3d74c21a5b93a54` | the API, worker and migrations image (`infra/Dockerfile.api`) |
| `ghcr.io/astral-sh/uv` | `0.13.5` | `sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff` | the `uv` binary copied into the API image |
| `postgres` | `16.15-alpine` | `sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139` | the database in compose and in CI |
| `postgres` | `16.15` | `sha256:933463a6c8eba2b9f5cd414138738cd8d5519cd2c471214c222652f547a66a32` | the database in the cluster, the backup image's client tools, the restore-test sidecar |
| `nginxinc/nginx-unprivileged` | `1.30.6-alpine` | `sha256:f5116ed4606f3eb015dd4a8e28cb9b351476dc0ad4bc6e9f93b19cec130cdf03` | serving the web build (`infra/Dockerfile.web`) |
| `node` | `22.23.4-bookworm-slim` | `sha256:5ec23b7c622b00547753bc6ffd78085dd868a68aeb41c8e0a1418f033e13d96d` | building the web export (`infra/Dockerfile.web`); Node 22, as CI and `package.json` |
| `koalaman/shellcheck` | `v0.11.0` | `sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d` | the CI ShellCheck step |
| `ghcr.io/gitleaks/gitleaks` | `v8.30.1` | `sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f` | the CI `secrets` job |
| `alpine/helm` | `3.22.1` | `sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1` | `helm lint` and `helm template` in the CI `chart` job (`infra/helm/tests/assert.sh`); Helm v3.22.1 below |
| `ghcr.io/yannh/kubeconform` | `v0.8.0` | `sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e` | validating the rendered manifests in the `chart` job |
| `mikefarah/yq` | `4.54.1` | `sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e` | the assertions on the rendered manifests in the `chart` job |
| `golang` | `1.27.3-trixie` | `sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80` | building rclone and age from source for the backup image (Go 1.27.3, 2026-10-19; review R101) |
| `nginxinc/nginx-unprivileged` (the web image) | as above | as above | with `apk upgrade` over it: Alpine's fixes since the base was built (review R101) |
| `aquasec/trivy` | `0.76.1` | `sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c` | the release's vulnerability and secret scans (`infra/release/scan.sh`). Never through `aquasecurity/trivy-action` or `setup-trivy`: their tags, and Trivy 0.69.4 to 0.69.6, were hijacked in March 2026 (CVE-2026-33634). 0.76.2 (2026-11-05) was too new |
| `quay.io/skopeo/stable` | `v1.22.3-immutable` | `sha256:c0ee1f4edca5c01cb8d5611124f92f3cc47196ecab68aee5d0f90834e00574d5` | pushing the scanned image layouts as they are (`infra/release/push_images.sh`). The `-immutable` tag: the plain ones are rebuilt nightly. Built 2026-09-21 |
| `rhysd/actionlint` | `1.7.12` | `sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667` | linting the workflows (CI `lint` job, `make ci-local`) |
| `moby/buildkit` | `v0.34.1` | `sha256:99368f3e46177c6405b9740e4461d91cee004128daeecca78d4ceac50ec1da5c` | the release workflow's builder (`setup-buildx-action`'s `driver-opts`); v0.34.2 (2026-11-03) was too new |
| `tonistiigi/binfmt` | `qemu-v10.2.3` | `sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0` | the release workflow's arm64 emulation (`setup-qemu-action`) |
| `registry` | `3` | `sha256:38907510915c3f1604c7e83d6ab602ab3020d092f73bdaed8696773693ff768c` | the throwaway registry of the local release proof (`infra/tests/release_gates.sh`) |
| `postgres` (old, on purpose) | `16.0` | `sha256:a80d0c1b119cf3d6bab27f72782f16e47ab8534ced937fa813ec2ab26e1fd81e` | only the local release proof's base with known fixed CVEs (2023-09-14); never run |

The backup image (`infra/Dockerfile.backup`) also pins, on the `postgres:16.15` base (Debian 13):

| Component | Version | Source | Used for |
| --- | --- | --- | --- |
| rclone | `v1.75.2`, shown as `v1.75.2-hds` | `go mod download github.com/rclone/rclone@v1.75.2`, built with Go 1.27.3 | the encrypted copies |
| age | `v1.3.2` | `filippo.io/age/cmd/age@v1.3.2`, built with Go 1.27.3 | Vault's snapshot |
| Debian `jq`, `curl`, `ca-certificates` | trixie's at build time | apt | the manifests; Vault's API; TLS |

rclone and age are built from source (review R101): the release binaries carry an older Go standard
library with fixed HIGH findings that the release's scan stops on, and no newer release that is two
weeks old fixes them. Go's checksum database verifies each module and its dependencies, as the
release tarballs' SHA-256 did. Both stages cross-compile on the build machine with
`CGO_ENABLED=0`, `GOTOOLCHAIN=local` and `-trimpath`. rclone keeps its own `go.mod` except the
modules `RCLONE_RAISE` lists, each two weeks old or more; none yet (tasks.md T047's follow-ups):
`golang.org/x/net` v0.62.0 fixes CVE-2026-78661 and turns two weeks old on 2026-11-10, and
rclone 1.76.0 (2026-11-06) on 2026-11-20. The postgres base's `gosu` is removed: the image never
switches user.

The three Debian packages are not pinned: they follow trixie's security updates whenever the image
is built, since Debian's archive keeps only the newest version and a pin would stop the build at
the next update (review R91). The base image's digest fixes the suite. Built on 2026-11-06 they
were jq `1.7.1-6+deb13u4`, curl `8.14.1-2+deb13u7` and ca-certificates `20250419+deb13u1`.

## Cluster

| Tool or chart | Version | Released | Used for |
| --- | --- | --- | --- |
| k3d | `v5.9.0` | 2026-06-02 | runs k3s in Docker on the operator's Mac |
| k3s | `v1.37.1+k3s1` | 2026-09-30 | the cluster |
| Helm | `v3.22.1` | 2026-10-15 | installing the chart (Helm 3, as the plan names) |
| Flux | `v2.9.6` | 2026-10-01 | pulling releases into the cluster; its `crd-schemas.tar.gz` (`sha256:380b713bd28a2c27cfc7710219aae79e1c9ceff9bee50197c023047190532987`, as the release's checksum file) validates `infra/flux` in the `chart` job. v2.10.0 (2026-10-26) was too new |
| Vault chart (`hashicorp/vault`) | `0.34.1` | 2026-08-13 | secrets |
| Vault Secrets Operator chart (`hashicorp/vault-secrets-operator`) | `1.6.0` | 2026-09-25 | syncing Vault into Kubernetes Secrets |
| kube-prometheus-stack chart | `92.11.0` | 2026-10-20 | Prometheus, Alertmanager, kube-state-metrics |
| age | `v1.3.2` | 2026-08-29 | encrypting Vault's snapshot |

## CI tools

| Tool | Version | Released | Used for |
| --- | --- | --- | --- |
| gitleaks | `v8.30.1` | 2026-03-21 | the secret scan (CI `secrets` job and pre-commit) |
| Trivy | `v0.76.1` | 2026-10-21 | the image vulnerability and secret scans, from its image above |
| actionlint | `v1.7.12` | 2026-03-30 | the workflows, from its image above |
| skopeo | `v1.22.3` | 2026-09-21 | the release's image push, from its image above |
| `actions/checkout` (release workflow) | `v7.0.1`, commit `3d3c42e5aac5ba805825da76410c181273ba90b1` | 2026-07-20 | the release workflow pins its actions by commit, since it can write packages |
| `docker/setup-qemu-action` | `v4.4.0`, commit `99012661954931238ded8c8b007157a8430204e1` | 2026-09-15 | the release's arm64 emulation |
| `docker/setup-buildx-action` | `v4.4.1`, commit `f87e5991a6d7451dcb8d9637bfbc97413f497069` | 2026-09-16 | the release's builder |
| ShellCheck | `v0.11.0` | 2025-08-04 | the shell scripts |
| kubeconform | `v0.8.0` | 2026-06-04 | validating rendered manifests |
| kubeconform's schemas (`yannh/kubernetes-json-schema`) | commit `b2ba4defe30ed5a0104987433896f1fd32807304` | 2026-10-21 | the Kubernetes v1.37.1 schemas kubeconform checks against, pinned in place of its default, which follows master |
| yq | `v4.54.1` | 2026-09-29 | the chart assertions (v4.54.2, 2026-11-06, was too new) |
| lychee (`lycheeverse/lychee:0.24.2@sha256:e2d19e57cf6ab037026f20b8e449a1f30d9d7f81eef4194763aab2eab20bd28d`) | `0.24.2` | 2026-05-01 | the Markdown link check in `make ci-local` |
| commitlint (`@commitlint/cli`, `@commitlint/config-conventional`) | `21.2.2`, `21.2.4` | 2026-08-13, 2026-10-18 | commitlint in `make ci-local` |
| pre-commit | `4.7.0` | 2026-10-12 | the local git hooks (`make hooks`), installed as a uv tool |
| commitlint-pre-commit-hook | `v9.27.0` | 2026-09-20 | the local commit-message hook (commitlint 21, `@commitlint/config-conventional` 21.2.4) |

## Python packages

Each is added to `apps/api/pyproject.toml` and `apps/api/uv.lock` in the group that first imports it
(metrics in Group C, traces in Group D, the SMTP test server in Group E), so the image never carries
an unused dependency. These are the versions to add.

| Package | Version | Used for |
| --- | --- | --- |
| `prometheus-client` | `0.26.0` | the metrics |
| `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http` | `1.45.1` | traces |
| `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-sqlalchemy` | `0.66b1` | traces. The SQLAlchemy one declares SQLAlchemy below 2.1 only (0.66b2 too); `core/tracing.py` passes `skip_dep_check=True` on 2.1, proved by `tests/integration/test_sql_spans.py` (review R29). Drop it once a release declares 2.1 |
| `aiosmtpd` (dev only) | `1.4.6` | a fake SMTP server for the tests |

The OpenTelemetry packages are pinned exactly, because the SDK and its instrumentation are released
together and must match.
