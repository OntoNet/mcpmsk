"""High-level pipeline runner for automated MCP data imports.

This module implements a minimal but feature-complete CLI workflow that
performs the following sequence without manual interaction:

```
PREPARE -> ANALYZE -> RESOLVE_STORAGE -> UPLOAD -> TRIGGER_DAG -> DONE
```

The behaviour follows the functional specification from the project brief:

* The runner talks to an MCP server over HTTP, invoking tools such as
  ``preflight_plan``, ``preflight_submit``, ``upload_url`` and ``dag_trigger``.
* Local shell actions from ``preflight_plan`` are executed directly. The
  resulting ``payload.json`` is cached next to the source file to guarantee
  idempotency.
* Uploads support both single PUT and multipart modes with concurrent part
  uploads, retries and detailed progress reporting.
* DAG triggering and optional polling are handled automatically.

The module exposes a :func:`main` entry point that powers the
``run-import`` command line utility.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import requests


# ---------------------------------------------------------------------------
# Helpers for configuration and logging
# ---------------------------------------------------------------------------


def _env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _mask_sensitive(value: str | None) -> str | None:
    if not value:
        return value
    if len(value) <= 8:
        return "***"
    return value[:4] + "…" + value[-4:]


def _format_bytes(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    units = ["KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f} {unit}"
    return f"{value:.1f} PiB"


class Stage(str):
    PREPARE = "PREPARE"
    ANALYZE = "ANALYZE"
    RESOLVE_STORAGE = "RESOLVE_STORAGE"
    UPLOAD = "UPLOAD"
    TRIGGER_DAG = "TRIGGER_DAG"
    DONE = "DONE"


@dataclass(slots=True)
class RunnerConfig:
    mcp_url: str
    mcp_token: str | None
    timeout: int
    upload_parallel: int
    part_size_mib: int
    temp_dir: Path
    dry_run: bool
    debug: bool

    @classmethod
    def from_env(
        cls,
        *,
        dry_run_override: bool | None = None,
        debug_override: bool | None = None,
    ) -> "RunnerConfig":
        url = os.getenv("MCP_URL") or "http://localhost:8899"
        token = os.getenv("MCP_TOKEN") or None
        timeout = _env_int("MCP_TIMEOUT", default=60)
        upload_parallel = max(1, _env_int("UPLOAD_PARALLEL", default=4))
        part_size_mib = max(8, _env_int("PART_SIZE_MIB", default=64))
        temp_dir = Path(os.getenv("MCP_TEMP_DIR") or Path.home() / ".onto-mcp" / "work")
        dry_run = dry_run_override if dry_run_override is not None else _env_flag("DRY_RUN", default=False)
        debug = debug_override if debug_override is not None else _env_flag("MCP_DEBUG", default=False)
        temp_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            mcp_url=url.rstrip("/"),
            mcp_token=token.strip() if token else None,
            timeout=timeout,
            upload_parallel=upload_parallel,
            part_size_mib=part_size_mib,
            temp_dir=temp_dir,
            dry_run=dry_run,
            debug=debug,
        )


def _setup_logging(debug: bool) -> logging.Logger:
    logger = logging.getLogger("onto_mcp.pipeline")
    if logger.handlers:
        # Avoid duplicate handlers when invoked multiple times
        return logger
    level = logging.DEBUG if debug else logging.INFO
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# MCP HTTP client wrapper
# ---------------------------------------------------------------------------


class MCPClientError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, payload: Any | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class MCPClient:
    """Minimal HTTP client for calling MCP tools."""

    MAX_RETRIES = 5

    def __init__(self, config: RunnerConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        headers = {"Content-Type": "application/json"}
        if config.mcp_token:
            headers["Authorization"] = f"Bearer {config.mcp_token}"
        self._headers = headers
        self._session = requests.Session()

    def call_tool(self, name: str, arguments: Mapping[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self._config.mcp_url}/tools/{name}"
        payload = {"arguments": arguments or {}}
        masked_payload = json.loads(json.dumps(payload))
        if self._config.debug:
            self._logger.debug(
                "[DEBUG] -> %s %s", url, json.dumps(masked_payload, ensure_ascii=False, indent=2)
            )

        delay = 1.0
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._session.post(
                    url,
                    headers=self._headers,
                    json=payload,
                    timeout=self._config.timeout,
                )
            except requests.RequestException as exc:  # Network error
                if attempt == self.MAX_RETRIES:
                    raise MCPClientError(f"network error calling {name}: {exc}") from exc
                self._logger.warning(
                    "[WARN] network error on %s (attempt %d/%d): %s",
                    name,
                    attempt,
                    self.MAX_RETRIES,
                    exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            if self._config.debug:
                masked_headers = {k: (_mask_sensitive(v) if k.lower() == "authorization" else v) for k, v in response.request.headers.items()}
                self._logger.debug(
                    "[DEBUG] <- %s %s | status=%s headers=%s",
                    name,
                    response.url,
                    response.status_code,
                    masked_headers,
                )

            if response.status_code >= 500:
                if attempt == self.MAX_RETRIES:
                    raise MCPClientError(
                        f"server error calling {name}: {response.status_code} {response.text.strip()}",
                        status_code=response.status_code,
                    )
                self._logger.warning(
                    "[WARN] server error %s on %s (attempt %d/%d)",
                    response.status_code,
                    name,
                    attempt,
                    self.MAX_RETRIES,
                )
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            try:
                data = response.json()
            except ValueError as exc:
                raise MCPClientError(
                    f"invalid JSON from MCP tool {name}: {exc}", status_code=response.status_code
                ) from exc

            if response.status_code >= 400:
                message = data.get("message") if isinstance(data, dict) else response.text
                raise MCPClientError(
                    f"{response.status_code}: {message or 'tool invocation failed'}",
                    status_code=response.status_code,
                    payload=data,
                )

            # FastMCP HTTP API wraps successful responses in {"result": {...}}
            if isinstance(data, dict):
                if "result" in data and isinstance(data["result"], dict):
                    return data["result"]
                if "content" in data and isinstance(data["content"], dict):
                    return data["content"]
            if isinstance(data, dict):
                return data

            raise MCPClientError(
                f"unexpected response structure from tool {name}: {data!r}",
                status_code=response.status_code,
                payload=data,
            )

        raise MCPClientError(f"failed to invoke tool {name}")


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------


class ProgressTracker:
    def __init__(self, total: int, logger: logging.Logger) -> None:
        self._total = total
        self._logger = logger
        self._lock = threading.Lock()
        self._uploaded = 0
        self._start = time.monotonic()

    def increment(self, amount: int) -> None:
        with self._lock:
            self._uploaded += amount
            self._report()

    def _report(self) -> None:
        elapsed = max(time.monotonic() - self._start, 1e-6)
        speed = self._uploaded / elapsed
        percent = (self._uploaded / self._total * 100) if self._total else 0
        self._logger.info(
            "[UPLOAD] %.1f%% (%s / %s, %.1f MiB/s)",
            percent,
            _format_bytes(self._uploaded),
            _format_bytes(self._total),
            speed / (1024 * 1024),
        )


def _put_with_retry(url: str, *, data: bytes, headers: Mapping[str, str] | None, timeout: int, logger: logging.Logger) -> requests.Response:
    headers = dict(headers or {})
    delay = 1.0
    for attempt in range(1, 6):
        try:
            response = requests.put(url, data=data, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            if attempt == 5:
                raise MCPClientError(f"network error uploading part to {url}: {exc}") from exc
            logger.warning(
                "[WARN] network error on upload attempt %d/5: %s", attempt, exc
            )
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue

        if response.status_code >= 500:
            if attempt == 5:
                raise MCPClientError(
                    f"server error during upload: {response.status_code} {response.text.strip()}",
                    status_code=response.status_code,
                )
            logger.warning(
                "[WARN] upload server error %s (attempt %d/5)", response.status_code, attempt
            )
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue

        if response.status_code >= 400:
            raise MCPClientError(
                f"upload failed: {response.status_code} {response.text.strip()}",
                status_code=response.status_code,
            )

        return response

    raise MCPClientError("upload attempts exhausted")


def _load_payload(payload_path: Path, *, source_path: Path) -> Dict[str, Any] | None:
    if not payload_path.exists():
        return None
    try:
        data = json.loads(payload_path.read_text("utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    meta = data.get("fileName"), data.get("fileSize")
    if meta[0] != source_path.name:
        return None
    if not isinstance(meta[1], int):
        return None
    try:
        actual_size = source_path.stat().st_size
    except OSError:
        return None
    if meta[1] != actual_size:
        return None
    return data


# ---------------------------------------------------------------------------
# Pipeline implementation
# ---------------------------------------------------------------------------


class PipelineRunner:
    def __init__(self, config: RunnerConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._client = MCPClient(config, logger)

    # Public API ---------------------------------------------------------
    def run(
        self,
        *,
        source: Path,
        sep: str | None,
        encoding: str | None,
        strategy: str,
        wait_for_dag: bool,
    ) -> Dict[str, Any]:
        source = source.resolve()
        if not source.exists() or not source.is_file():
            raise MCPClientError(f"source file not found: {source}")

        self._set_stage(Stage.PREPARE)
        payload_path = source.parent / "payload.json"
        payload = _load_payload(payload_path, source_path=source)
        if payload is None:
            payload = self._run_preflight_plan(source, payload_path, sep=sep, encoding=encoding)
        else:
            self._logger.info("[CACHE] using cached payload at %s", payload_path)

        if self._config.dry_run:
            self._logger.info("[DRY-RUN] analysis complete, skipping upload/dag")
            return {"payload": payload, "dryRun": True}

        self._set_stage(Stage.ANALYZE)
        plan_result = self._client.call_tool("preflight_submit", {"payload": payload})
        storage_info = plan_result.get("storage")
        if not storage_info:
            raise MCPClientError(
                "storage_config_not_found: сервер не вернул блок storage. "
                "Создайте [INFRA] StorageConfig или свяжите шаблон PipelineTemplate.",
                status_code=424,
            )

        dataset_id = plan_result.get("datasetClassEntityId")
        signature_id = plan_result.get("signatureId")
        template_id = plan_result.get("templateId")
        if not signature_id:
            raise MCPClientError("preflight_submit did not return signatureId")

        self._logger.info(
            "[INFO] matched dataset=%s signature=%s template=%s",
            dataset_id,
            signature_id,
            template_id or "-",
        )

        self._set_stage(Stage.RESOLVE_STORAGE)
        content_type = payload.get("signature", {}).get("contentType") or "text/csv"

        upload_info = self._client.call_tool(
            "upload_url",
            {
                "signatureId": signature_id,
                "fileName": source.name,
                "fileSize": payload.get("fileSize"),
                "contentType": content_type,
                "strategy": strategy,
            },
        )

        upload_summary = self._perform_upload(
            source,
            upload_info,
            signature_id=signature_id,
            content_type=content_type,
        )

        self._set_stage(Stage.TRIGGER_DAG)
        dag_params = {
            "dagId": upload_info.get("dagId", "csv_ingest_pg"),
            "params": {
                "signature_id": signature_id,
                "class_id": dataset_id,
                "template_id": template_id,
                "sep": payload.get("signature", {}).get("sep"),
                "encoding": payload.get("signature", {}).get("encoding"),
                "bucket": upload_info.get("bucket"),
                "key": upload_info.get("s3Key"),
                "target": plan_result.get("target"),
            },
        }

        presigned = upload_summary.get("presigned_get_url")
        if presigned:
            dag_params["params"]["presigned_get_url"] = presigned
        else:
            endpoint = upload_info.get("externalEndpoint") or upload_info.get("endpoint")
            if endpoint:
                dag_params["params"].update({"s3_endpoint": endpoint})

        dag_trigger = self._client.call_tool("dag_trigger", dag_params)
        run_id = dag_trigger.get("runId")
        self._logger.info("[DAG] triggered runId=%s", run_id)

        if wait_for_dag and run_id:
            self._wait_for_dag(dag_id=dag_params["dagId"], run_id=run_id)

        self._set_stage(Stage.DONE)
        result = {
            "datasetClassEntityId": dataset_id,
            "signatureId": signature_id,
            "templateId": template_id,
            "storage": upload_info,
            "upload": upload_summary,
            "dagRunId": run_id,
        }
        return result

    # Internal helpers ---------------------------------------------------

    def _set_stage(self, stage: Stage) -> None:
        self._logger.info("[%s]", stage)

    def _run_preflight_plan(
        self,
        source: Path,
        payload_path: Path,
        *,
        sep: str | None,
        encoding: str | None,
    ) -> Dict[str, Any]:
        self._set_stage(Stage.ANALYZE)
        arguments: Dict[str, Any] = {"source": str(source)}
        if sep:
            arguments["forceSep"] = sep
        if encoding:
            arguments["forceEncoding"] = encoding

        plan = self._client.call_tool("preflight_plan", arguments)
        actions = plan.get("actions") or []
        if not isinstance(actions, list):
            raise MCPClientError("preflight_plan returned malformed actions")

        for action in actions:
            kind = action.get("type")
            if kind == "shell":
                self._run_shell_action(action, cwd=source.parent)
            elif kind == "mcp_call":
                # These calls will be executed manually after payload is loaded
                continue
            else:
                raise MCPClientError(f"unsupported action type from preflight_plan: {kind}")

        if not payload_path.exists():
            raise MCPClientError(
                f"preflight_plan did not produce payload.json at {payload_path}. "
                "Проверьте корректность анализатора."
            )

        payload = json.loads(payload_path.read_text("utf-8"))
        if not isinstance(payload, dict):
            raise MCPClientError("payload.json содержимое некорректно (ожидался объект)")

        return payload

    def _run_shell_action(self, action: Mapping[str, Any], *, cwd: Path) -> None:
        cmd = action.get("cmd")
        if not isinstance(cmd, str) or not cmd.strip():
            raise MCPClientError("shell action missing command")

        env = os.environ.copy()
        for key, value in (action.get("env") or {}).items():
            env[str(key)] = str(value)

        self._logger.info("[ANALYZE] running shell action: %s", action.get("name") or cmd)
        process = subprocess.run(
            cmd,
            shell=True,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if process.returncode != 0:
            raise MCPClientError(
                f"analysis shell command failed with exit code {process.returncode}:\n{process.stdout}"
            )
        if process.stdout:
            self._logger.info(process.stdout.strip())

    def _perform_upload(
        self,
        source: Path,
        upload_info: Mapping[str, Any],
        *,
        signature_id: str,
        content_type: str,
    ) -> Dict[str, Any]:
        mode = upload_info.get("mode") or "single"
        mode = str(mode)
        if mode not in {"single", "multipart"}:
            raise MCPClientError(f"unsupported upload mode: {mode}")

        self._set_stage(Stage.UPLOAD)
        if mode == "single":
            if upload_info.get("maxSize") and source.stat().st_size > int(upload_info["maxSize"]):
                raise MCPClientError(
                    "multipart_required: Файл крупнее single-лимита. Используйте --strategy multipart.",
                    status_code=413,
                )
            return self._upload_single(source, upload_info, content_type=content_type)

        return self._upload_multipart(
            source,
            upload_info,
            signature_id=signature_id,
            content_type=content_type,
        )

    def _upload_single(
        self,
        source: Path,
        upload_info: Mapping[str, Any],
        *,
        content_type: str,
    ) -> Dict[str, Any]:
        put_url = upload_info.get("putUrl")
        if not put_url:
            raise MCPClientError("upload_url response missing putUrl for single mode")

        headers = dict(upload_info.get("headers") or {})
        headers.setdefault("Content-Type", content_type)
        tracker = ProgressTracker(source.stat().st_size, self._logger)
        chunk_size = 1024 * 1024
        last_response: requests.Response | None = None
        with source.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                last_response = _put_with_retry(
                    put_url,
                    data=chunk,
                    headers=headers,
                    timeout=self._config.timeout,
                    logger=self._logger,
                )
                tracker.increment(len(chunk))
        etag = None
        if last_response is not None:
            etag = last_response.headers.get("ETag")
        return {"mode": "single", "eTag": etag.strip('"') if isinstance(etag, str) else etag}

    def _upload_multipart(
        self,
        source: Path,
        upload_info: Mapping[str, Any],
        *,
        signature_id: str,
        content_type: str,
    ) -> Dict[str, Any]:
        parts = upload_info.get("parts")
        part_size = int(upload_info.get("partSize") or self._config.part_size_mib * 1024 * 1024)
        if not isinstance(parts, Sequence) or not parts:
            raise MCPClientError("multipart upload requires parts array")

        size = source.stat().st_size
        tracker = ProgressTracker(size, self._logger)
        etags: Dict[int, str] = {}
        headers = dict(upload_info.get("headers") or {})
        headers.setdefault("Content-Type", content_type)

        def upload_part(part_info: Mapping[str, Any]) -> None:
            part_number = int(part_info.get("partNumber"))
            put_url = part_info.get("putUrl")
            if not put_url:
                raise MCPClientError(f"part {part_number} missing putUrl")

            offset = (part_number - 1) * part_size
            length = min(part_size, size - offset)
            if length <= 0:
                return

            with source.open("rb") as handle:
                handle.seek(offset)
                chunk = handle.read(length)

            response = _put_with_retry(put_url, data=chunk, headers=headers, timeout=self._config.timeout, logger=self._logger)
            tracker.increment(len(chunk))
            etag = response.headers.get("ETag")
            if etag:
                etags[part_number] = etag.strip('"') if isinstance(etag, str) else etag

        threads: List[threading.Thread] = []
        errors: List[BaseException] = []
        lock = threading.Lock()

        def worker(part_list: Iterable[Mapping[str, Any]]) -> None:
            for part in part_list:
                if errors:
                    return
                try:
                    upload_part(part)
                except BaseException as exc:  # pragma: no cover - defensive
                    with lock:
                        errors.append(exc)
                    return

        # Chunk parts for workers
        parallel = max(1, min(self._config.upload_parallel, len(parts)))
        chunk_size = math.ceil(len(parts) / parallel)
        for i in range(parallel):
            chunk = parts[i * chunk_size : (i + 1) * chunk_size]
            if not chunk:
                continue
            thread = threading.Thread(target=worker, args=(chunk,), daemon=True)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        if errors:
            raise errors[0]

        complete_payload = {
            "signatureId": signature_id,
            "parts": [{"partNumber": k, "eTag": v} for k, v in sorted(etags.items())],
        }

        complete_url = upload_info.get("completeUrl")
        if complete_url:
            response = requests.post(
                complete_url,
                json=complete_payload,
                timeout=self._config.timeout,
            )
            if response.status_code >= 400:
                raise MCPClientError(
                    f"upload completion failed: {response.status_code} {response.text.strip()}",
                    status_code=response.status_code,
                )
            result = response.json() if response.content else {}
        else:
            result = self._client.call_tool("upload_complete", complete_payload)

        result.setdefault("mode", "multipart")
        result["parts"] = complete_payload["parts"]
        return result

    def _wait_for_dag(self, *, dag_id: str, run_id: str) -> None:
        self._logger.info("[WAIT] polling dag_status for %s (run %s)", dag_id, run_id)
        delay = 5
        consecutive_errors = 0
        while True:
            try:
                status = self._client.call_tool(
                    "dag_status",
                    {"dagId": dag_id, "runId": run_id},
                )
            except MCPClientError as exc:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    raise MCPClientError(
                        f"dag_status failed repeatedly: {exc}", status_code=exc.status_code
                    )
                self._logger.warning(
                    "[WARN] dag_status failed (%s), retrying in %ss", exc, delay
                )
                time.sleep(delay)
                continue

            consecutive_errors = 0
            state = status.get("state") or status.get("status")
            self._logger.info("[WAIT] dag run status: %s", state)
            if state in {"success", "failed", "cancelled"}:
                if state != "success":
                    raise MCPClientError(f"DAG finished with state {state}")
                return
            time.sleep(delay)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run managed MCP import pipeline")
    parser.add_argument("--source", required=True, help="Path to local CSV file")
    parser.add_argument("--sep", choices=[",", ";"], help="Override CSV delimiter")
    parser.add_argument("--encoding", help="Override file encoding")
    parser.add_argument(
        "--strategy",
        choices=["auto", "single", "multipart"],
        default="auto",
        help="Upload strategy override",
    )
    parser.add_argument("--wait", action="store_true", help="Wait for DAG completion")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without uploading")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logs and JSON traces")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = RunnerConfig.from_env(
        dry_run_override=args.dry_run or None,
        debug_override=args.debug or None,
    )
    logger = _setup_logging(config.debug)

    logger.info(
        "[CONFIG] MCP_URL=%s timeout=%ss parallel=%s dry_run=%s",
        config.mcp_url,
        config.timeout,
        config.upload_parallel,
        config.dry_run,
    )

    runner = PipelineRunner(config, logger)

    try:
        result = runner.run(
            source=Path(args.source),
            sep=args.sep,
            encoding=args.encoding,
            strategy=args.strategy,
            wait_for_dag=args.wait,
        )
    except MCPClientError as exc:
        logger.error("[ERROR] %s", exc)
        return 2

    logger.info("[DONE] import finished successfully")
    logger.info(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())

