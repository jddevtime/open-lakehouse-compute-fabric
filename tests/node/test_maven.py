# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Maven resolver against a fake local Maven repository served over real HTTP (and ``file://`` for the offline bundle path)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from olcf.runtime.errors import ChecksumMismatch, DependencyResolutionError
from olcf.runtime.maven import (
    Coordinate,
    MavenResolver,
    Repository,
    jar_artifact_index,
    sha256_file,
    verify_locked_jars,
)
from tests.node.fakes import FakeMavenRepo, serve_directory


@pytest.fixture()
def repo(tmp_path: Path) -> FakeMavenRepo:
    r = FakeMavenRepo(tmp_path / "repo")
    # root -> mid -> leaf ; root -> testdep (test scope) ; root -> opt (optional) ; root -> spark-core (provided by Spark)
    r.add("org.leaf", "leaf", "1.0")
    r.add("org.mid", "mid", "1.0", deps=[("org.leaf", "leaf", "1.0")])
    r.add("org.t", "testdep", "1.0")
    r.add("org.o", "opt", "1.0")
    r.add("org.apache.spark", "spark-core_2.13", "4.1.1")
    r.add("org.root", "root", "1.0", deps=[("org.mid", "mid", "1.0"), ("org.t", "testdep", "1.0", "test"), ("org.o", "opt", "1.0", None, True),
                                          ("org.apache.spark", "spark-core_2.13", "4.1.1", "provided")])
    return r


def resolve(repo: FakeMavenRepo, tmp_path: Path, roots: list[str], url: str, **kw) -> tuple[list[Coordinate], MavenResolver]:
    res = MavenResolver([url], cache_dir=tmp_path / "cache", **kw)
    return res.resolve(roots), res


def test_transitive_compile_scope_only(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        coords, _ = resolve(repo, tmp_path, ["org.root:root:1.0"], url)
    assert [str(c) for c in coords] == ["org.root:root:1.0", "org.mid:mid:1.0", "org.leaf:leaf:1.0"]  # test/optional/provided never resolved


def test_artifacts_provided_by_spark_are_excluded_even_when_compile_scope(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.add("org.x", "x", "1.0", deps=[("org.apache.spark", "spark-core_2.13", "4.1.1"), ("org.leaf", "leaf", "1.0"), ("org.pat", "patterned-thing", "2.0")])
    repo.add("org.pat", "patterned-thing", "2.0")
    with serve_directory(repo.root) as url:
        coords, _ = resolve(repo, tmp_path, ["org.x:x:1.0"], url, provided_artifacts={"spark-core_2.13"}, provided_patterns=["org.pat:*"])
    assert [c.artifact for c in coords] == ["x", "leaf"]


def test_fetch_records_sha256_and_files(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        entries = res.resolve_and_fetch(["org.root:root:1.0"], tmp_path / "jars")
    by = {e["coordinate"]: e for e in entries}
    assert set(by) == {"org.root:root:1.0", "org.mid:mid:1.0", "org.leaf:leaf:1.0"}
    leaf = by["org.leaf:leaf:1.0"]
    jar = tmp_path / "jars" / leaf["file"]
    assert jar.is_file() and leaf["sha256"] == hashlib.sha256(jar.read_bytes()).hexdigest() == sha256_file(jar)
    assert leaf["sha1"] and leaf["size"] == jar.stat().st_size and leaf["repository"].startswith("http://127.0.0.1")
    assert verify_locked_jars(tmp_path / "jars", entries) == []


def test_checksum_mismatch_fails_closed_and_installs_nothing(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.corrupt_jar("org.leaf", "leaf", "1.0")
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        with pytest.raises(ChecksumMismatch) as ei:
            res.resolve_and_fetch(["org.root:root:1.0"], tmp_path / "jars")
    assert "leaf" in ei.value.message and ei.value.remediation
    assert not list((tmp_path / "jars").glob("leaf*.jar")), "the corrupted jar must not be left in the destination"
    assert not list((tmp_path / "cache").rglob("leaf-1.0.jar")), "and must not be cached either"


def test_artifact_without_published_checksum_is_refused(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.drop_checksums("org.leaf", "leaf", "1.0")
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        with pytest.raises(ChecksumMismatch, match="publishes no .sha256/.sha1"):
            res.resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "jars")


def test_sha1_only_repository_is_accepted(repo: FakeMavenRepo, tmp_path: Path) -> None:
    p = repo.jar_path("org.leaf", "leaf", "1.0")
    Path(str(p) + ".sha256").unlink()
    with serve_directory(repo.root) as url:
        entries = MavenResolver([url], cache_dir=tmp_path / "cache").resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "jars")
    assert entries[0]["sha256"]  # we always compute our own sha256 for the lock


def test_locked_hash_is_enforced_on_reinstall(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        first = MavenResolver([url], cache_dir=tmp_path / "c1").resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j1")
        lock = {e["coordinate"]: e["sha256"] for e in first}
        # same bytes: fine
        MavenResolver([url], cache_dir=tmp_path / "c2").resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j2", locked=lock)
        # upstream "re-released" the artifact with new (internally consistent) bytes: the lock catches it
        repo.add("org.leaf", "leaf", "1.0", jar=b"different bytes, valid checksums")
        with pytest.raises(ChecksumMismatch, match="locked"):
            MavenResolver([url], cache_dir=tmp_path / "c3").resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j3", locked=lock)


def test_cache_is_reverified_and_a_corrupt_cache_entry_is_refetched(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        res.resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j1")
        cached = next((tmp_path / "cache").rglob("leaf-1.0.jar"))
        cached.write_bytes(b"corrupted in cache")
        entries = MavenResolver([url], cache_dir=tmp_path / "cache").resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j2")
    assert sha256_file(tmp_path / "j2" / entries[0]["file"]) == hashlib.sha256(repo.jar_path("org.leaf", "leaf", "1.0").read_bytes()).hexdigest()


def test_nearest_wins_and_exclusions(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.add("g", "c", "1", )
    repo.add("g", "c", "2")
    repo.add("g", "b", "1", deps=[("g", "c", "1")])
    repo.add("g", "a", "1", deps=[("g", "b", "1"), ("g", "c", "2")])  # c:2 is nearer than c:1 (via b)
    repo.add("g", "ex", "1", deps=[("g", "b", "1", None, False, [("g", "c")])])  # c excluded below b
    with serve_directory(repo.root) as url:
        coords, _ = resolve(repo, tmp_path, ["g:a:1"], url)
        assert {str(c) for c in coords} == {"g:a:1", "g:b:1", "g:c:2"}
        coords, _ = resolve(repo, tmp_path, ["g:ex:1"], url)
        assert {c.artifact for c in coords} == {"ex", "b"}


def test_parent_pom_properties_and_dependency_management(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.add("g", "leaf2", "3.1")
    repo.add("g", "parent", "9", props={"leaf2.version": "3.1"}, managed=[("g", "leaf2", "${leaf2.version}")], packaging="pom")
    repo.add("g", "child", "1", parent=("g", "parent", "9"), deps=[("g", "leaf2", None)])  # version comes from the parent's dependencyManagement
    with serve_directory(repo.root) as url:
        coords, _ = resolve(repo, tmp_path, ["g:child:1"], url)
    assert [str(c) for c in coords] == ["g:child:1", "g:leaf2:3.1"]


def test_bom_import_scope(repo: FakeMavenRepo, tmp_path: Path) -> None:
    repo.add("g", "managed", "5.5")
    repo.add("g", "bom", "1", managed=[("g", "managed", "5.5")], packaging="pom")
    repo.add("g", "user", "1", managed=None, deps=[("g", "managed", None)])
    # the importing project declares the BOM in its own dependencyManagement with scope import
    p = repo._dir("g", "user", "1") / "user-1.pom"
    text = p.read_text().replace("<dependencies>", "<dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>bom</artifactId><version>1</version>"
                                                      "<type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement><dependencies>", 1)
    repo._write(p, text.encode())
    with serve_directory(repo.root) as url:
        coords, _ = resolve(repo, tmp_path, ["g:user:1"], url)
    assert [str(c) for c in coords] == ["g:user:1", "g:managed:5.5"]


@pytest.mark.parametrize("version", ["LATEST", "RELEASE", "1.0-SNAPSHOT", "[1.0,2.0)", "(1.0,)"])
def test_floating_versions_are_rejected(repo: FakeMavenRepo, tmp_path: Path, version: str) -> None:
    with serve_directory(repo.root) as url, pytest.raises(DependencyResolutionError):
        resolve(repo, tmp_path, [f"org.leaf:leaf:{version}"], url)


def test_missing_artifact_gives_actionable_error(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url, pytest.raises(DependencyResolutionError) as ei:
        resolve(repo, tmp_path, ["org.nope:nothing:1.0"], url)
    assert "not found in any repository" in ei.value.message and ei.value.remediation


def test_repository_list_is_tried_in_order(repo: FakeMavenRepo, tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with serve_directory(empty) as url1, serve_directory(repo.root) as url2:
        res = MavenResolver([url1, url2], cache_dir=tmp_path / "cache")
        entries = res.resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "jars")
    assert entries[0]["repository"] == url2


def test_offline_bundle_reuse_resolves_without_network(repo: FakeMavenRepo, tmp_path: Path) -> None:
    """export_repository -> file:// repo + offline transport reproduces the same jars (the offline-bundle path)."""
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        online = res.resolve_and_fetch(["org.root:root:1.0"], tmp_path / "online")
        n = res.export_repository(tmp_path / "bundle-repo")
    assert n >= 6  # 3 poms + 3 jars at least
    # no server is running any more: only the bundle repository exists
    off = MavenResolver([(tmp_path / "bundle-repo").as_uri()], cache_dir=tmp_path / "cache2", offline=True)
    offline = off.resolve_and_fetch(["org.root:root:1.0"], tmp_path / "offline", locked={e["coordinate"]: e["sha256"] for e in online})
    assert {e["coordinate"]: e["sha256"] for e in offline} == {e["coordinate"]: e["sha256"] for e in online}


def test_offline_flag_blocks_http(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url, pytest.raises(DependencyResolutionError):
        MavenResolver([url], cache_dir=tmp_path / "cache", offline=True).resolve(["org.leaf:leaf:1.0"])


def test_bundle_repository_tampering_is_detected(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        res = MavenResolver([url], cache_dir=tmp_path / "cache")
        res.resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j")
        res.export_repository(tmp_path / "bundle-repo")
    jar = next((tmp_path / "bundle-repo").rglob("leaf-1.0.jar"))
    jar.write_bytes(jar.read_bytes() + b"x")
    with pytest.raises(ChecksumMismatch):
        MavenResolver([(tmp_path / "bundle-repo").as_uri()], cache_dir=tmp_path / "c2", offline=True).resolve_and_fetch(["org.leaf:leaf:1.0"], tmp_path / "j2")


def test_verify_locked_jars_detects_modification_and_missing(repo: FakeMavenRepo, tmp_path: Path) -> None:
    with serve_directory(repo.root) as url:
        entries = MavenResolver([url], cache_dir=tmp_path / "cache").resolve_and_fetch(["org.root:root:1.0"], tmp_path / "jars")
    jars = tmp_path / "jars"
    assert verify_locked_jars(jars, entries) == []
    (jars / "mid-1.0.jar").write_bytes(b"evil")
    (jars / "leaf-1.0.jar").unlink()
    problems = verify_locked_jars(jars, entries)
    assert any("mid-1.0.jar" in p and "differs" in p for p in problems) and any("missing jar leaf-1.0.jar" in p for p in problems)


def test_jar_artifact_index_and_repository_coercion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    d = tmp_path / "jars"
    d.mkdir()
    for n in ("spark-core_2.13-4.1.1.jar", "scala-library-2.13.16.jar", "guava-33.0.0-jre.jar", "weird.jar"):
        (d / n).write_bytes(b"x")
    assert jar_artifact_index(d) == {"spark-core_2.13", "scala-library", "guava", "weird"}
    monkeypatch.setenv("MVN_USER", "bob")
    monkeypatch.setenv("MVN_PASS", "s3cret")
    r = Repository.coerce({"url": "https://nexus.example/maven/", "username_env": "MVN_USER", "password_env": "MVN_PASS"})
    assert (r.username, r.password, r.base) == ("bob", "s3cret", "https://nexus.example/maven")
    assert Repository.coerce("https://x/y").url == "https://x/y"


def test_default_repositories_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.runtime.maven import default_repositories

    monkeypatch.setenv("OLCF_MAVEN_REPOSITORIES", "https://mirror.corp/maven, file:///opt/offline-repo")
    assert [r.url for r in default_repositories()] == ["https://mirror.corp/maven", "file:///opt/offline-repo"]
    monkeypatch.delenv("OLCF_MAVEN_REPOSITORIES")
    assert default_repositories()[0].url.startswith("https://repo1.maven.org/maven2")


def test_hostile_relpath_cannot_escape_the_cache(tmp_path: Path) -> None:
    res = MavenResolver(["http://127.0.0.1:1"], cache_dir=tmp_path / "cache")
    with pytest.raises(DependencyResolutionError, match="outside the cache"):
        res._cached("../../etc/passwd")  # noqa: SLF001
