# syntax=docker/dockerfile:1.7
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# OLCF control plane + embedded Spark worker in one image.
#
#   docker build -f deploy/Dockerfile -t olcf:local .
#   docker build -f deploy/Dockerfile --build-arg EXTRAS=all --build-arg BAKE_RUNTIME=olcf-rt-2.0.0 -t olcf:local-runtime .        # development-signed runtime (try-out)
#   docker build -f deploy/Dockerfile --build-arg RUNTIME_TRUST_DIR=my-trust --build-arg BAKE_RUNTIME=olcf-rt-2.0.0.json -t olcf:prod .  # operator-signed runtime
#
# Runtime trust: the final image is ALWAYS OLCF_ENVIRONMENT=production.  The OLCF development key (private half public) is trusted only inside the throwaway
# `runtime-seed` stage, and only when no operator key is given; see deploy/docker/bake-runtime.sh and deploy/README.md#runtime-trust-in-production.
# RUNTIME_TRUST_DIR is a directory *inside the build context* (default deploy/runtime-trust, empty) with the public keys (*.pub) that sign your runtime manifests.
#
# Stages: build (wheel from this checkout, wheels-only virtualenv) -> jre (Eclipse Temurin 21 JRE, copied) -> final (python:3.13-slim, non-root uid 10001).
# The build context is the repository root.  NOTE: this Dockerfile was written without access to a Docker daemon and has not been built yet.

ARG PYTHON_IMAGE=python:3.13-slim-bookworm
# Ubuntu 22.04 (glibc 2.35) based JRE: runs on Debian bookworm (glibc 2.36).  Do not use the noble-based default tag here.
ARG JRE_IMAGE=eclipse-temurin:21-jre-jammy
ARG UV_VERSION=0.11.32

# ---------------------------------------------------------------------------------------------------------------- uv
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# ---------------------------------------------------------------------------------------------------------------- build
FROM ${PYTHON_IMAGE} AS build
ARG EXTRAS=postgres,s3,k8s
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never UV_NO_PROGRESS=1
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
COPY schemas ./schemas
RUN uv build --wheel --out-dir /wheels
# wheels only: a missing wheel fails the build instead of pulling in a compiler
RUN uv venv --python /usr/local/bin/python /opt/olcf \
 && WHEEL="$(ls /wheels/olcf-*.whl)" \
 && uv pip install --python /opt/olcf/bin/python --only-binary :all: "olcf[${EXTRAS}] @ file://${WHEEL}" \
 && /opt/olcf/bin/olcf --version

# ---------------------------------------------------------------------------------------------------------------- JRE (copied into later stages)
FROM ${JRE_IMAGE} AS jre_src

# ---------------------------------------------------------------------------------------------------------------- optional runtime layer
FROM build AS runtime-seed
ARG BAKE_RUNTIME=
ARG RUNTIME_TRUST_DIR=deploy/runtime-trust
COPY --from=jre_src /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk OLCF_JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:/opt/olcf/bin:$PATH
# NOTE: no OLCF_ENVIRONMENT here and none in the final image but "production": bake-runtime.sh decides per process (operator keys -> production + OLCF_RUNTIME_TRUST_DIR;
# none -> OLCF_ENVIRONMENT=development for the single `olcf runtime install` call).  This stage is discarded; only /opt/olcf-seed is copied on.
COPY ${RUNTIME_TRUST_DIR}/ /etc/olcf/runtime-trust/
COPY deploy/docker/bake-runtime.sh /usr/local/bin/olcf-bake-runtime
RUN chmod 0755 /usr/local/bin/olcf-bake-runtime && /usr/local/bin/olcf-bake-runtime "${BAKE_RUNTIME}" /opt/olcf-seed /etc/olcf/runtime-trust

# ---------------------------------------------------------------------------------------------------------------- final
FROM ${PYTHON_IMAGE} AS final
ARG RUNTIME_TRUST_DIR=deploy/runtime-trust
LABEL org.opencontainers.image.title="Open Lakehouse Compute Fabric" \
      org.opencontainers.image.description="OLCF control plane with embedded worker" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.authors="Justin Dodrill"
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 olcf \
 && useradd --system --uid 10001 --gid 10001 --home-dir /data/home --shell /usr/sbin/nologin olcf \
 && mkdir -p /data /opt/olcf-seed \
 && chown -R 10001:10001 /data
COPY --from=jre_src /opt/java/openjdk /opt/java/openjdk
COPY --from=build /opt/olcf /opt/olcf
COPY --from=runtime-seed --chown=10001:10001 /opt/olcf-seed /opt/olcf-seed
# public keys only; the entrypoint exports OLCF_RUNTIME_TRUST_DIR to it when it holds a *.pub (an explicit OLCF_RUNTIME_TRUST_DIR wins)
COPY ${RUNTIME_TRUST_DIR}/ /etc/olcf/runtime-trust/
COPY deploy/docker/entrypoint.sh /usr/local/bin/olcf-entrypoint
COPY deploy/docker/enroll.sh /usr/local/bin/olcf-enroll
COPY deploy/docker/healthcheck.py /usr/local/bin/olcf-healthcheck
RUN chmod 0755 /usr/local/bin/olcf-entrypoint /usr/local/bin/olcf-enroll /usr/local/bin/olcf-healthcheck
ENV PATH=/opt/olcf/bin:/opt/java/openjdk/bin:$PATH \
    JAVA_HOME=/opt/java/openjdk OLCF_JAVA_HOME=/opt/java/openjdk \
    OLCF_DATA_DIR=/data OLCF_CONFIG_DIR=/data/.cli HOME=/data/home \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=random \
    OLCF_ENVIRONMENT=production
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8470
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=5 CMD ["/opt/olcf/bin/python", "/usr/local/bin/olcf-healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/olcf-entrypoint"]
CMD ["serve"]
