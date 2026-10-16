"""Local stand-in for the Databricks Academy `%run ./Includes/Classroom-Setup-X.Y` notebooks.

    from adelab.classroom import setup
    DA = setup("1.1")

After this cell the notebook has the same globals a Databricks classroom notebook has:
`spark`, `display`, `dbutils` (files subset), `DA`, plus `%sql` / `%%sql` magics.
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import SparkSession

from . import notebook
from .spark import DATA_ROOT, PROJECT_ROOT, build_spark, file_uri

COURSES = {
    "1": ("sdp", "Advanced Techniques with Spark Declarative Pipelines", "01-advanced-sdp"),
    "2": ("privacy", "Databricks Data Privacy", "02-data-privacy"),
    "3": ("perf", "Databricks Performance Optimization", "03-performance-optimization"),
    "4": ("dabs", "Automated Deployment with Declarative Automation Bundles", "04-automated-deployment-dabs"),
}


def _local_path(path: str) -> Path:
    p = str(path)
    for prefix in ("dbfs:", "file:"):
        if p.startswith(prefix):
            p = p[len(prefix):]
    if os.name == "nt" and len(p) > 2 and p[0] == "/" and p[2] == ":":
        p = p[1:]
    return Path(p)


@dataclass
class FileInfo:
    path: str
    name: str
    size: int
    modificationTime: int


class _FsUtils:
    """The subset of `dbutils.fs` the course notebooks use, backed by the local filesystem."""

    def ls(self, path: str) -> list[FileInfo]:
        p = _local_path(path)
        out = []
        for c in sorted(p.iterdir()):
            st = c.stat()
            name = c.name + ("/" if c.is_dir() else "")
            out.append(FileInfo(str(c) + ("/" if c.is_dir() else ""), name,
                                0 if c.is_dir() else st.st_size, int(st.st_mtime * 1000)))
        return out

    def mkdirs(self, path: str) -> bool:
        _local_path(path).mkdir(parents=True, exist_ok=True)
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        p = _local_path(path)
        if not p.exists():
            return False
        if p.is_dir():
            if not recurse:
                raise OSError(f"{path} is a directory; pass recurse=True")
            shutil.rmtree(p)
        else:
            p.unlink()
        return True

    def cp(self, src: str, dst: str, recurse: bool = False) -> bool:
        s, d = _local_path(src), _local_path(dst)
        if s.is_dir():
            if not recurse:
                raise OSError(f"{src} is a directory; pass recurse=True")
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            d.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(s, d)
        return True

    def head(self, path: str, max_bytes: int = 65536) -> str:
        with open(_local_path(path), "rb") as fh:
            return fh.read(max_bytes).decode("utf-8", errors="replace")

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        p = _local_path(path)
        if p.exists() and not overwrite:
            raise FileExistsError(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(contents, encoding="utf-8")
        return True


dbutils = SimpleNamespace(fs=_FsUtils())


@dataclass
class DBAcademyHelper:
    spark: SparkSession
    lesson: str
    course_slug: str
    course_title: str
    schema_name: str
    course_dir: Path = PROJECT_ROOT
    catalog_name: str = "spark_catalog"
    paths: SimpleNamespace = field(default_factory=SimpleNamespace)

    @property
    def schema_location(self) -> Path:
        return DATA_ROOT / "warehouse" / f"{self.schema_name}.db"

    # ------------------------------------------------------------------ lifecycle
    def _create_schema(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
        self.spark.sql(f"USE {self.schema_name}")
        for p in vars(self.paths).values():
            Path(p).mkdir(parents=True, exist_ok=True)

    def _register_existing_tables(self) -> list[str]:
        """Spark's local catalog is in-memory: after a kernel restart, re-attach the Delta tables on disk."""
        found = []
        if not self.schema_location.exists():
            return found
        for d in sorted(self.schema_location.iterdir()):
            if (d / "_delta_log").is_dir() and any((d / "_delta_log").glob("*.json")):
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.schema_name}.{d.name} "
                               f"USING DELTA LOCATION '{file_uri(d)}'")
                found.append(d.name)
        return found

    def reset(self) -> None:
        """Drop every table and file this lesson created and start from an empty schema."""
        self.cleanup(quiet=True)
        self._create_schema()
        print(f"Lesson {self.lesson} reset: schema `{self.schema_name}` is empty, "
              f"working dir {self.paths.working_dir}")

    def cleanup(self, quiet: bool = False) -> None:
        """Equivalent of `DA.cleanup()` at the end of a course notebook."""
        for q in self.spark.streams.active:
            q.stop()
        self.spark.sql("USE default")
        self.spark.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        self.spark.catalog.clearCache()
        shutil.rmtree(self.schema_location, ignore_errors=True)
        shutil.rmtree(self.paths.working_dir, ignore_errors=True)
        if not quiet:
            print(f"Cleaned up schema `{self.schema_name}` and {self.paths.working_dir}")

    # ------------------------------------------------------------------ helpers
    def pipeline(self, name: str, source: str | os.PathLike, configuration: dict | None = None):
        """Create a local Spark Declarative Pipeline (the "Pipelines" UI equivalent)."""
        from .sdp import Pipeline

        sources = source if isinstance(source, (list, tuple)) else [source]
        sources = [s if Path(s).is_absolute() else self.course_dir / s for s in sources]
        return Pipeline(self.spark, name=name, source=sources, schema=self.schema_name,
                        storage=Path(self.paths.working_dir) / "pipelines" / name,
                        configuration=configuration or {})

    def show_source(self, path: str | os.PathLike) -> None:
        """Print pipeline/bundle source files (relative to the course folder) with syntax highlighting."""
        from IPython.display import Code, Markdown
        from IPython.display import display as ipy_display

        root = Path(path) if Path(path).is_absolute() else self.course_dir / path
        files = [root] if root.is_file() else sorted(p for p in root.rglob("*") if p.is_file()
                                                     and "__pycache__" not in p.parts)
        lang = {".py": "python", ".sql": "sql", ".yml": "yaml", ".yaml": "yaml", ".toml": "toml",
                ".json": "json", ".md": "markdown", ".sh": "bash", ".ps1": "powershell"}
        for f in files:
            ipy_display(Markdown(f"**`{f.relative_to(self.course_dir) if f.is_relative_to(self.course_dir) else f}`**"))
            ipy_display(Code(f.read_text(encoding="utf-8"), language=lang.get(f.suffix, "text")))

    def show_slide(self, name: str, width: int = 900) -> None:
        """Show your own screenshot from `<course>/slides/<name>` (git-ignored: course slides are copyrighted)."""
        from IPython.display import Image, Markdown
        from IPython.display import display as ipy_display

        path = self.course_dir / "slides" / name
        if path.exists():
            ipy_display(Image(filename=str(path), width=width))
        else:
            ipy_display(Markdown(f"*Slide `{name}` not found. Save your screenshot as "
                                 f"`{path.relative_to(self.course_dir.parent)}` to show it here.*"))

    def print_config(self) -> None:
        print(f"  Course       : {self.course_title}")
        print(f"  Lesson       : {self.lesson}")
        print(f"  Schema       : {self.schema_name}   (USE {self.schema_name})")
        for k, v in vars(self.paths).items():
            print(f"  DA.paths.{k:<12}: {v}")


def setup(lesson: str, *, reset: bool = False, spark_conf: dict[str, str] | None = None,
          shuffle_partitions: int = 8) -> DBAcademyHelper:
    """Classroom setup for lesson `X.Y` (course X). Idempotent; safe to re-run."""
    course = lesson.split(".")[0]
    slug, title, folder = COURSES[course]
    spark = build_spark(f"adelab-{lesson}", shuffle_partitions=shuffle_partitions, extra_conf=spark_conf)
    for k, v in (spark_conf or {}).items():
        try:
            spark.conf.set(k, v)
        except Exception:  # static confs only take effect on a fresh kernel
            pass

    schema = f"{slug}_{lesson.replace('.', '_')}"
    working = DATA_ROOT / "courses" / schema
    da = DBAcademyHelper(
        spark=spark, lesson=lesson, course_slug=slug, course_title=title, schema_name=schema,
        course_dir=PROJECT_ROOT / folder,
        paths=SimpleNamespace(working_dir=str(working), landing=str(working / "landing"),
                              checkpoints=str(working / "checkpoints")))
    if reset:
        da.reset()
    else:
        da._create_schema()
    restored = da._register_existing_tables()

    notebook.register(spark)
    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip is not None:
            ip.push({"spark": spark, "display": notebook.display, "dbutils": dbutils, "DA": da})
    except ImportError:
        pass

    print("=" * 78)
    print(f"Classroom setup complete  |  Spark {spark.version}  |  Spark UI: {spark.sparkContext.uiWebUrl}")
    da.print_config()
    if restored:
        print(f"  Re-attached tables : {restored}")
    print("  Globals: spark, display(df), dbutils.fs, DA, %sql / %%sql")
    print("=" * 78)
    return da
