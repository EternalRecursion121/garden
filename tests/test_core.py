from __future__ import annotations

import tempfile
import textwrap
import unittest
from types import SimpleNamespace
from pathlib import Path

from core.dispatcher import Dispatcher
from core.gateways.discord import _reply_payloads
from core.manifest import AgentManifest
from core.registry import Registry
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
            dispatcher.registry.refresh()

            self.assertEqual(dispatcher.call("a.f"), {"version": 2})


class DiscordFormattingTests(unittest.TestCase):
    def test_reply_payloads_split_long_messages(self) -> None:
        payloads = _reply_payloads("agent", ["x" * 4100], limit=100)

        self.assertGreater(len(payloads), 1)
        self.assertTrue(all(len(p) <= 100 for p in payloads))
        self.assertTrue(all(p.startswith("**[agent]** ") for p in payloads))


class SandboxParityTests(unittest.TestCase):
    def test_sandboxed_python_impl_passes_agent_root_to_harness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            agent = root / "agents" / "a"
            impl = agent / "functions" / "f.py"
            write(impl, "def run(params, ctx):\n    return None\n")

            calls: list[list[str]] = []
            orig_build_bwrap = sandbox_mod.build_bwrap_argv
            orig_run = sandbox_mod.subprocess.run

            def fake_build_bwrap(*args, **kwargs):
                return ["bwrap"]

            def fake_run(cmd, **kwargs):
                calls.append(cmd)
                return SimpleNamespace(returncode=0, stdout='{"result": null}', stderr="")

            try:
                sandbox_mod.build_bwrap_argv = fake_build_bwrap
                sandbox_mod.subprocess.run = fake_run
                fn = SimpleNamespace(impl="functions/f.py:run", timeout=None)
                manifest = SimpleNamespace(folder=agent, name="a")
                impl_fn = sandbox_mod.make_sandboxed_python_impl(
                    root, manifest, fn, sandbox_mod.SandboxConfig()
                )
                ctx = SimpleNamespace(run_id="r", parent_run_id=None, scope=None, depth=0)
                impl_fn({}, ctx)
            finally:
                sandbox_mod.build_bwrap_argv = orig_build_bwrap
                sandbox_mod.subprocess.run = orig_run

            self.assertIn("--agent-root", calls[0])
            self.assertIn(str(agent.resolve()), calls[0])


if __name__ == "__main__":
    unittest.main()
