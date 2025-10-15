"""Interact with Pyright to demonstrate language-server insights for DuckPlus."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


LSP_HEADER_ENCODING = "ascii"


def _default_command() -> list[str]:
    """Return the default pyright language server invocation."""

    return ["npx", "-y", "pyright-langserver", "--stdio"]


def _workspace_root(argument: str | None) -> Path:
    """Determine the workspace root passed to the language server."""

    if argument:
        return Path(argument).resolve()
    return Path.cwd()


def _demo_source() -> str:
    """Return the demo source file fed to the language server."""

    return (
        "from duckplus.typed import AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS\n\n"
        "scalar_expr = SCALAR_FUNCTIONS.Varchar.\n"
        "numeric_expr = SCALAR_FUNCTIONS.Numeric.\n"
        "aggregate_expr = AGGREGATE_FUNCTIONS.Numeric.\n\n"
        "hover_scalar = SCALAR_FUNCTIONS.Varchar.lower(\"customer_name\")\n"
        "hover_numeric = SCALAR_FUNCTIONS.Numeric.abs(42)\n"
        "hover_aggregate = AGGREGATE_FUNCTIONS.Numeric.sum(\"revenue\")\n"
    )


def _format_hover(result: dict[str, Any] | None) -> str:
    if not result:
        return "<no hover information>"
    contents = result.get("contents")
    if contents is None:
        return "<no hover information>"
    if isinstance(contents, dict):
        value = contents.get("value")
        return value or json.dumps(contents)
    if isinstance(contents, list):
        return "\n".join(_extract_hover_line(item) for item in contents)
    if isinstance(contents, str):
        return contents
    return json.dumps(contents)


def _extract_hover_line(item: Any) -> str:
    if isinstance(item, dict):
        return item.get("value", json.dumps(item))
    return str(item)


def _format_completion_items(items: Iterable[dict[str, Any]]) -> list[str]:
    formatted: list[str] = []
    for entry in items:
        label = entry.get("label")
        detail = entry.get("detail")
        if detail:
            formatted.append(f"{label} — {detail}")
        else:
            formatted.append(str(label))
    return formatted


@dataclass
class _LSPMessage:
    payload: dict[str, Any]

    def to_bytes(self) -> bytes:
        encoded = json.dumps(self.payload).encode("utf-8")
        header = f"Content-Length: {len(encoded)}\r\n\r\n".encode(LSP_HEADER_ENCODING)
        return header + encoded


class _LSPClient:
    """Minimal JSON-RPC client for interacting with an LSP server."""

    def __init__(self, command: list[str], *, cwd: Path | None = None) -> None:
        self._proc = subprocess.Popen(  # noqa: S603,S607 - tool invocation
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd) if cwd else None,
        )
        self._next_id = 0

    def close(self) -> None:
        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.terminate()
        self._proc.wait(timeout=10)

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._next_id += 1
        message_id = self._next_id
        message = _LSPMessage(
            {
                "jsonrpc": "2.0",
                "id": message_id,
                "method": method,
                "params": params or {},
            }
        )
        self._write(message)
        return self._read_response(message_id)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message = _LSPMessage(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params or {},
            }
        )
        self._write(message)

    def _write(self, message: _LSPMessage) -> None:
        if not self._proc.stdin:
            raise RuntimeError("language-server stdin closed")
        self._proc.stdin.write(message.to_bytes())
        self._proc.stdin.flush()

    def _read_response(self, expected_id: int) -> dict[str, Any]:
        while True:
            payload = self._read_message()
            if payload.get("id") == expected_id:
                return payload
            # Print asynchronous notifications for visibility and continue.
            method = payload.get("method")
            if method:
                print(f"[server] {method}")

    def _read_message(self) -> dict[str, Any]:
        if not self._proc.stdout:
            raise RuntimeError("language-server stdout closed")
        headers: dict[str, str] = {}
        while True:
            line = self._proc.stdout.readline()
            if not line:
                raise RuntimeError("language-server exited unexpectedly")
            decoded = line.decode(LSP_HEADER_ENCODING).strip()
            if not decoded:
                break
            if ":" in decoded:
                name, value = decoded.split(":", 1)
                headers[name.strip().lower()] = value.strip()
        length_text = headers.get("content-length")
        if length_text is None:
            raise RuntimeError(f"missing content-length header: {headers}")
        length = int(length_text)
        body = self._proc.stdout.read(length)
        if not body:
            raise RuntimeError("language-server returned empty body")
        return json.loads(body)


def _document_uri(path: Path) -> str:
    return path.as_uri()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace",
        help="Workspace root provided to the language server (defaults to CWD).",
    )
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        help="Override the command used to launch the language server.",
    )
    arguments = parser.parse_args(argv)

    workspace = _workspace_root(arguments.workspace)
    if not workspace.exists():
        msg = f"Workspace {workspace} does not exist"
        raise SystemExit(msg)

    command = arguments.command or _default_command()

    source = _demo_source()
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        dir=workspace,
        delete=False,
        encoding="utf-8",
    ) as handle:
        handle.write(source)
        document_path = Path(handle.name)

    client = _LSPClient(command, cwd=workspace)
    try:
        init_params = {
            "processId": os.getpid(),
            "rootUri": workspace.as_uri(),
            "capabilities": {},
            "workspaceFolders": [
                {"uri": workspace.as_uri(), "name": workspace.name}
            ],
        }
        initialize = client.request("initialize", init_params)
        capabilities = initialize.get("result", {}).get("capabilities", {})
        capability_names = sorted(capabilities)
        summary = (
            f"{len(capability_names)} capabilities"
            if capability_names
            else "<no capabilities reported>"
        )
        print("initialize:", summary)
        client.notify("initialized")
        text_document = {
            "uri": _document_uri(document_path),
            "languageId": "python",
            "version": 1,
            "text": source,
        }
        client.notify("textDocument/didOpen", {"textDocument": text_document})

        completion_queries = {
            "SCALAR_FUNCTIONS.Varchar": (2, len("scalar_expr = SCALAR_FUNCTIONS.Varchar.")),
            "SCALAR_FUNCTIONS.Numeric": (3, len("numeric_expr = SCALAR_FUNCTIONS.Numeric.")),
            "AGGREGATE_FUNCTIONS.Numeric": (4, len("aggregate_expr = AGGREGATE_FUNCTIONS.Numeric.")),
        }
        for namespace, (line, character) in completion_queries.items():
            response = client.request(
                "textDocument/completion",
                {
                    "textDocument": {"uri": _document_uri(document_path)},
                    "position": {"line": line, "character": character},
                },
            )
            result = response.get("result") or {}
            if isinstance(result, dict):
                items = result.get("items", [])
            else:
                items = result
            formatted = [
                entry
                for entry in _format_completion_items(items)
                if not entry.startswith("__")
            ][:5]
            print(f"\nCompletion for {namespace}:")
            for entry in formatted:
                print(f"  - {entry}")
            if not formatted:
                print("  <no completion items>")

        hover_queries = {
            "SCALAR_FUNCTIONS.Varchar.lower": (6, len("hover_scalar = SCALAR_FUNCTIONS.Varchar.low")),
            "SCALAR_FUNCTIONS.Numeric.abs": (7, len("hover_numeric = SCALAR_FUNCTIONS.Numeric.ab")),
            "AGGREGATE_FUNCTIONS.Numeric.sum": (8, len("hover_aggregate = AGGREGATE_FUNCTIONS.Numeric.su")),
        }
        for label, (line, character) in hover_queries.items():
            response = client.request(
                "textDocument/hover",
                {
                    "textDocument": {"uri": _document_uri(document_path)},
                    "position": {"line": line, "character": character},
                },
            )
            hover_result = response.get("result")
            hover = _format_hover(hover_result)
            print(f"\nHover for {label}:\n  {hover}")

        signature_response = client.request(
            "textDocument/signatureHelp",
            {
                "textDocument": {"uri": _document_uri(document_path)},
                "position": {"line": 6, "character": len("hover_scalar = SCALAR_FUNCTIONS.Varchar.lower(")},
            },
        )
        signature_result = signature_response.get("result", {})
        active_signature = signature_result.get("signatures", [{}])[0]
        label = active_signature.get("label", "<no signature>")
        print(f"\nSignature help for SCALAR_FUNCTIONS.Varchar.lower:\n  {label}")

        shutdown = client.request("shutdown")
        print("\nshutdown:", json.dumps(shutdown.get("result")))
        client.notify("exit")
    finally:
        client.close()
        try:
            document_path.unlink()
        except FileNotFoundError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
