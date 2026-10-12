<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 Justin Dodrill -->
# OLCF in Docker: from nothing to a first query

Works the same with **Docker Desktop** (macOS, Windows, Linux) and plain Docker Engine. Nothing is installed on the host except the containers;
on a Mac without the Xcode Command Line Tools this is the easiest route (Docker Desktop does not need them).

## 1. Get the files

Download the repository archive from your release page (no git needed) and unpack it, or clone it if you have git. Then, from the repository root:

```sh
sh deploy/scripts/gen-env.sh          # writes deploy/.env with fresh random secrets (mode 0600). Keep it; never commit it.
```

On Windows PowerShell without `sh`: copy `deploy/.env.example` to `deploy/.env` and fill in the secrets listed at the top
(`OLCF_MASTER_KEY` = 32 random bytes in base64, `OLCF_JWT_SIGNING_KEY`, `OLCF_ADMIN_PASSWORD`).

## 2. Start it

```sh
docker compose -f deploy/docker-compose.yml --profile allinone up -d --build
docker compose -f deploy/docker-compose.yml ps                      # wait until "olcf" shows (healthy)
```

The first build takes a few minutes (it compiles nothing: wheels only). The server listens on `127.0.0.1:8470` only. Open <http://127.0.0.1:8470>.

Sign in as `admin` with the `OLCF_ADMIN_PASSWORD` from `deploy/.env`. If you left it empty, a password was generated on the first start and printed once:

```sh
docker compose -f deploy/docker-compose.yml logs olcf | grep -A3 "FIRST RUN"
```

## 3. Install the Spark runtime (once)

SQL on DuckDB, lakehouses and the UI work immediately. Notebooks and Spark jobs need the runtime, which is installed into the data volume:

```sh
docker compose -f deploy/docker-compose.yml exec olcf olcf runtime install
```

The images run as `production`, which does not trust the development key that signs the bundled runtime manifests: for this local try-out put `OLCF_ENVIRONMENT=development` in `deploy/.env` first (then recreate the container). For production, give the containers your own runtime trust directory instead (see [deploy/README.md](../deploy/README.md#runtime-trust-in-production)).

(or bake it into the image at build time with `OLCF_BAKE_RUNTIME=...` in `deploy/.env`; see the deploy README for the two modes.)

## 4. Use it

```sh
docker compose -f deploy/docker-compose.yml exec olcf olcf --url http://127.0.0.1:8470 login -u admin     # prompts for the password
docker compose -f deploy/docker-compose.yml exec olcf olcf sample install                                 # a sample workspace with data and notebooks
docker compose -f deploy/docker-compose.yml exec olcf olcf sql run "select 42 as answer"
```

From the host (with the SDK or CLI installed) the same server is `OLCF_URL=http://127.0.0.1:8470`.

## 5. A real cluster on one machine

```sh
docker compose -f deploy/docker-compose.yml --profile cluster up -d --build
```

adds PostgreSQL, MinIO (S3-compatible storage) and two workers that enrol themselves, with TLS between the pieces. Add
`--profile tls` for a Caddy reverse proxy with automatic certificates.

## 6. Stop, back up, remove

```sh
docker compose -f deploy/docker-compose.yml exec olcf olcf backup create        # an encrypted backup inside the data volume
docker compose -f deploy/docker-compose.yml --profile allinone down             # stop; the data volume is kept
docker compose -f deploy/docker-compose.yml --profile allinone down -v          # stop AND DELETE all data (asks nothing: be sure)
```

Back up `deploy/.env` too: without `OLCF_MASTER_KEY` encrypted secrets and backups cannot be read.
