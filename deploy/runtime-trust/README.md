<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 Justin Dodrill -->
# Runtime trust directory (image builds)

Put the **public** keys (`<name>.pub`, PEM) that sign your Spark runtime manifests in this directory, or point the `RUNTIME_TRUST_DIR` build argument at another directory
inside the build context. `deploy/Dockerfile` and `deploy/Dockerfile.worker` copy it to `/etc/olcf/runtime-trust` and the entrypoints export `OLCF_RUNTIME_TRUST_DIR` to it when
it holds a key. Put an operator-signed manifest next to the keys to bake it (`--build-arg BAKE_RUNTIME=olcf-rt-2.0.0.json`). Never put a private key here.
Without a `*.pub` file this directory changes nothing. See [deploy/README.md](../README.md#runtime-trust-in-production).
