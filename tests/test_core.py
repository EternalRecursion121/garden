from __future__ import annotations

import os
import tempfile
import textwrap
import time as _time
import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock

from core.dispatcher import Dispatcher
from core.gateways.discord import _reply_payloads
from core.manifest import AgentManifest
from core.registry import Registry
from core.scheduler import Scheduler
import core.sandbox as sandbox_mod


class FakeCarry:
    def assert_(self, *args, **kwargs):
        return "did:fake"


def write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body), encoding="utf-8")


class ManifestTests(unittest.TestCase):
    def test_commands_must_be_discord_app_command_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write(
                root / "agent.toml",
                """
                [agent]
                name = "a"

                [[function]]
                name = "f"
                impl = "f.py:run"
                commands = ["/Bad"]
                """,
            )

            with self.assertRaisesRegex(ValueError, "lowercase"):
                AgentManifest.load(root)


class RegistryTests(unittest.TestCase):
    def test_duplicate_agent_names_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for folder in ("one", "two"):
                write(
                    root / folder / "agent.toml",
                    """
                    [agent]
                    name = "same"
                    """,
                )

            with self.assertRaisesRegex(ValueError, "duplicate agent name"):
                Registry(root)


class DispatcherTests(unittest.TestCase):
    def test_python_impl_cache_invalidates_when_file_changes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            agents = Path(td) / "agents"
            agent = agents / "a"
            impl = agent / "functions" / "f.py"
            write(
                agent / "agent.toml",
                """
                [agent]
                name = "a"

                [agent.sandbox]
                enabled = false

                [[function]]
                name = "f"
                impl = "functions/f.py:run"
                """,
            )
            write(impl, "def run(params, ctx):\n    return {'version': 1}\n")

            dispatcher = Dispatcher(Registry(agents), FakeCarry(), log=False)
            self.assertEqual(dispatcher.call("a.f"), {"version": 1})

            write(impl, "def run(params, ctx):\n    return {'version': 2}\n")
            # Some filesystems (Docker overlay2) have sub-second mtime
            # resolution but may cache metadata. Force mtime to a different
            # value so the impl-fingerprint cache always invalidates.
            os.utime(impl, (_time.time(), _time.time() + 1))
            dispatcher.registry.refresh()

            self.assertEqual(dispatcher.call("a.f"), {"version": 2})


class DiscordFormattingTests(unittest.TestCase):
    def test_reply_payloads_split_long_messages(self) -> None:
        payloads = _reply_payloads("agent", ["x" * 4100], limit=100)

        self.assertGreater(len(payloads), 1)
        self.assertTrue(all(len(p) <= 100 for p in payloads))
        self.assertTrue(all(p.startswith("**[agent]** ") for p in payloads))


class SandboxSecurityTests(unittest.TestCase):
    """can_call_matches security boundary — must be tight by default."""

    def test_none_allows_all(self):
        self.assertTrue(sandbox_mod.can_call_matches(None, "iris.respond"))
        self.assertTrue(sandbox_mod.can_call_matches(None, "anything.goes"))
        self.assertTrue(sandbox_mod.can_call_matches(None, ""))

    def test_empty_denies_all(self):
        self.assertFalse(sandbox_mod.can_call_matches([], "iris.respond"))
        self.assertFalse(sandbox_mod.can_call_matches([], "anything.goes"))
        self.assertFalse(sandbox_mod.can_call_matches([], ""))

    def test_exact_match(self):
        allow = ["iris.respond", "kira.consult"]
        self.assertTrue(sandbox_mod.can_call_matches(allow, "iris.respond"))
        self.assertTrue(sandbox_mod.can_call_matches(allow, "kira.consult"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "iris.consult"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "loam.respond"))

    def test_wildcard_match(self):
        allow = ["kira.*", "tilth.*"]
        self.assertTrue(sandbox_mod.can_call_matches(allow, "kira.respond"))
        self.assertTrue(sandbox_mod.can_call_matches(allow, "kira.consult"))
        self.assertTrue(sandbox_mod.can_call_matches(allow, "tilth.sync"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "iris.respond"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "loam.anything"))

    def test_mixed_exact_and_wildcard(self):
        allow = ["iris.respond", "tilth.*"]
        self.assertTrue(sandbox_mod.can_call_matches(allow, "iris.respond"))
        self.assertTrue(sandbox_mod.can_call_matches(allow, "tilth.sync"))
        self.assertTrue(sandbox_mod.can_call_matches(allow, "tilth.synthesise"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "iris.consult"))
        self.assertFalse(sandbox_mod.can_call_matches(allow, "kira.respond"))


class SchedulerTrackingTests(unittest.TestCase):
    """Scheduler inflight tracking and overlap enforcement."""

    def test_inflight_tracks_and_clears_on_success(self):
        sched = Scheduler(dispatcher=MagicMock())
        fut = Future()
        sched._track("a.f", fut)
        self.assertTrue(sched._is_inflight("a.f"))
        fut.set_result(42)
        # Done-callback fires synchronously on set_result.
        self.assertFalse(sched._is_inflight("a.f"))

    def test_inflight_clears_on_exception(self):
        sched = Scheduler(dispatcher=MagicMock())
        fut = Future()
        sched._track("a.f", fut)
        self.assertTrue(sched._is_inflight("a.f"))
        fut.set_exception(RuntimeError("boom"))
        self.assertFalse(sched._is_inflight("a.f"))

    def test_inflight_independent_per_qualified_name(self):
        sched = Scheduler(dispatcher=MagicMock())
        f1, f2 = Future(), Future()
        sched._track("a.f", f1)
        sched._track("b.f", f2)
        self.assertTrue(sched._is_inflight("a.f"))
        self.assertTrue(sched._is_inflight("b.f"))
        f1.set_result(None)
        self.assertFalse(sched._is_inflight("a.f"))
        self.assertTrue(sched._is_inflight("b.f"))

    def test_inflight_empty_after_all_complete(self):
        sched = Scheduler(dispatcher=MagicMock())
        futs = [Future() for _ in range(3)]
        for f in futs:
            sched._track("a.f", f)
        self.assertTrue(sched._is_inflight("a.f"))
        for f in futs:
            f.set_result(None)
        self.assertFalse(sched._is_inflight("a.f"))


class SandboxParityTests(unittest.TestCase):
    def test_sandboxed_python_impl_passes_agent_root_to_harness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            agent = root / "agents" / "a"
            impl = agent / "functions" / "f.py"
            write(impl, "def run(params, ctx):\n    return None\n")

            # The sandbox impl now uses subprocess.Popen with streaming I/O
            # for RPC. Mock Popen with a fake that emits a single result
            # envelope and EOF, mirroring a no-op child.
            calls: list[list[str]] = []
            orig_build_bwrap = sandbox_mod.build_bwrap_argv
            orig_popen = sandbox_mod.subprocess.Popen

            def fake_build_bwrap(*args, **kwargs):
                return ["bwrap"]

            class _FakePipe:
                def __init__(self, lines: list[str]):
                    self._lines = list(lines)

                def readline(self) -> str:
                    return self._lines.pop(0) if self._lines else ""

                def __iter__(self):
                    return iter(self._lines)

                def write(self, data: str) -> int:
                    return len(data)

                def flush(self) -> None:
                    pass

                def close(self) -> None:
                    pass

            class _FakePopen:
                def __init__(self, cmd, **kwargs):
                    calls.append(cmd)
                    self.stdin = _FakePipe([])
                    self.stdout = _FakePipe(['{"op":"result","result":null}\n'])
                    self.stderr = _FakePipe([])
                    self.returncode = 0

                def wait(self, timeout=None):
                    return 0

                def kill(self):
                    pass

            try:
                sandbox_mod.build_bwrap_argv = fake_build_bwrap
                sandbox_mod.subprocess.Popen = _FakePopen
                fn = SimpleNamespace(impl="functions/f.py:run", timeout=None)
                manifest = SimpleNamespace(folder=agent, name="a")
                impl_fn = sandbox_mod.make_sandboxed_python_impl(
                    root, manifest, fn, sandbox_mod.SandboxConfig()
                )
                ctx = SimpleNamespace(
                    run_id="r", parent_run_id=None, scope=None, depth=0,
                    agent="a", call=lambda *a, **kw: None,
                    list_functions=lambda *_: [], list_agents=lambda: [],
                )
                impl_fn({}, ctx)
            finally:
                sandbox_mod.build_bwrap_argv = orig_build_bwrap
                sandbox_mod.subprocess.Popen = orig_popen

            self.assertIn("--agent-root", calls[0])
            self.assertIn(str(agent.resolve()), calls[0])


if __name__ == "__main__":
    unittest.main()
