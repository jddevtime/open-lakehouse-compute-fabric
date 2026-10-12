# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Control-plane application factory."""

from __future__ import annotations

import importlib
import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from olcf.api.errors import install_error_handlers
from olcf.api.middleware import CorrelationAndSecurityMiddleware
from olcf.api.modules import MODULES
from olcf.core.config import Settings, load_settings
from olcf.core.context import AppContext, Module, build_context
from olcf.core.logging import configure_logging
from olcf.version import __version__

log = logging.getLogger("olcf.app")
STATIC_DIR = Path(__file__).resolve().parent.parent / "web" / "static"


def install_modules(ctx: AppContext, names: list[str] | None = None) -> list[Module]:
    mods: list[Module] = []
    for path in names or MODULES:
        try:
            mod = importlib.import_module(path)
        except ModuleNotFoundError as exc:
            if exc.name and (path == exc.name or path.startswith(exc.name + ".")):
                log.warning("module %s not present; skipping", path)
                continue
            raise
        installed = mod.install(ctx)
        if installed is not None:
            mods.append(installed)
    return mods


class LeaderLoop(threading.Thread):
    """Renews the leader lease so singleton services (scheduler, triggers, alerts) run on one replica."""

    def __init__(self, ctx: AppContext) -> None:
        super().__init__(daemon=True, name="olcf-leader")
        self.ctx, self._stop = ctx, threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                was = self.ctx.leader.is_leader()
                now = self.ctx.leader.acquire()
                if now != was:
                    log.info("leadership %s (node=%s term=%s)", "ACQUIRED" if now else "LOST", self.ctx.node_id, self.ctx.leader.term)
                    self.ctx.events.publish("controlplane.leader_changed", {"node": self.ctx.node_id, "leader": now, "term": self.ctx.leader.term}, persist=True)
            except Exception:  # noqa: BLE001
                log.exception("leader renewal failed")
            self._stop.wait(self.ctx.leader.ttl / 3)

    def stop(self) -> None:
        self._stop.set()
        try:
            self.ctx.leader.release()
        except Exception:  # noqa: BLE001
            pass


def create_app(settings: Settings | None = None, ctx: AppContext | None = None, *, module_names: list[str] | None = None) -> FastAPI:
    settings = settings or (ctx.settings if ctx else load_settings())
    configure_logging(settings.log_level, json_logs=settings.log_json, log_dir=settings.path("logs"), name="control-plane")
    ctx = ctx or build_context(settings)

    leader_loop = LeaderLoop(ctx)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # bootstrap the first administrator
        from olcf.api.core.identity import bootstrap_admin

        created = bootstrap_admin(ctx)
        if created:
            import os

            row, pw = created
            if os.environ.get("OLCF_BOOTSTRAP_ADMIN_PASSWORD"):
                # the operator chose this password: it is never printed (a container logs this output) and never written to disk
                print(f"\n  FIRST RUN: administrator '{row['name']}' created with the password from OLCF_BOOTSTRAP_ADMIN_PASSWORD.\n", flush=True)  # noqa: T201
            else:
                banner = (
                    "\n" + "=" * 72 + f"\n  FIRST RUN: administrator account created\n  username: {row['name']}\n  password: {pw}\n"
                    "  Change it after logging in. This is shown only once.\n" + "=" * 72 + "\n"
                )
                print(banner, flush=True)  # noqa: T201
                try:
                    p = ctx.settings.path("secrets", "bootstrap-admin.txt")
                    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                    with os.fdopen(fd, "w") as fh:
                        fh.write(f"username={row['name']}\npassword={pw}\n")
                    print(f"  (also written to {p}; delete the file after you have saved the password)\n", flush=True)  # noqa: T201
                except OSError:
                    pass
        leader_loop.start()
        for m in ctx.modules:
            m.start()
        log.info("OLCF control plane %s started on node %s (%d modules)", __version__, ctx.node_id, len(ctx.modules))
        try:
            yield
        finally:
            for m in reversed(ctx.modules):
                try:
                    m.stop()
                except Exception:  # noqa: BLE001
                    log.exception("module %s failed to stop", m.name)
            leader_loop.stop()
            ctx.db.dispose()

    app = FastAPI(
        title="Open Lakehouse Compute Fabric API", version=__version__, lifespan=lifespan,
        description="REST control plane for the Open Lakehouse Compute Fabric (OLCF), author: Justin Dodrill. "
                    "Authenticate with `Authorization: Bearer <token>`.",
        openapi_url="/v1/openapi.json", docs_url=None, redoc_url=None,
        license_info={"name": "Apache-2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0"},
    )
    app.state.ctx = ctx
    app.add_middleware(CorrelationAndSecurityMiddleware, hsts=settings.tls.enabled)
    if settings.server.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=settings.server.cors_origins, allow_methods=["*"], allow_headers=["*"],
                           expose_headers=["ETag", "X-Correlation-ID"], allow_credentials=False)
    install_error_handlers(app)

    ctx.modules = install_modules(ctx, module_names)
    v1 = APIRouter(prefix="/v1")
    for m in ctx.modules:
        for r, prefix in m.routers:
            v1.include_router(r, prefix=prefix)
        for r, prefix in getattr(m, "v1_public", []):
            v1.include_router(r, prefix=prefix)
        for r, prefix in m.public_routers:
            app.include_router(r, prefix=prefix)
    app.include_router(v1)

    # API documentation UI served from vendored assets when present (offline friendly)
    docs_dir = STATIC_DIR / "vendor" / "swagger-ui"
    if docs_dir.is_dir():
        from fastapi.openapi.docs import get_swagger_ui_html
        from fastapi.responses import HTMLResponse

        @app.get("/v1/docs", include_in_schema=False)
        def swagger() -> HTMLResponse:
            return get_swagger_ui_html(openapi_url="/v1/openapi.json", title="OLCF API", swagger_js_url="/vendor/swagger-ui/swagger-ui-bundle.js",
                                       swagger_css_url="/vendor/swagger-ui/swagger-ui.css")

    if settings.server.serve_ui and STATIC_DIR.is_dir() and (STATIC_DIR / "index.html").exists():
        app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="ui")
    return app
