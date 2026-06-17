import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch

# agent.py reads required env at import time and exits(2) otherwise; set before loading.
os.environ.setdefault("MANAGER_URL", "http://manager.test")
os.environ.setdefault("BUILD_AGENT_TOKEN", "agent-secret")

_AGENT_PATH = Path(__file__).resolve().parent.parent / "build-agent" / "agent.py"
_spec = importlib.util.spec_from_file_location("oneclick_build_agent", _AGENT_PATH)
agent = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(agent)


class BuildNetworkFailClosedTests(unittest.TestCase):
    """User Dockerfile builds must not get unrestricted host egress by default."""

    def _job(self):
        return {"id": 7, "tag": "reg/user-1:demo", "dockerfile": "FROM scratch\n"}

    def test_refuses_build_when_network_unset_and_required(self):
        logs = []
        results = []
        with patch.object(agent, "BUILD_NETWORK_REQUIRED", True), \
             patch.object(agent, "BUILD_NETWORK", ""), \
             patch.object(agent, "free_disk_gb", return_value=9999), \
             patch.object(agent, "push_log", lambda i, m: logs.append(m)), \
             patch.object(agent, "report_result", lambda i, s: results.append(s)), \
             patch.object(agent, "stream_command") as stream:
            agent.run_build(self._job())

        stream.assert_not_called()
        self.assertEqual(results, ["failed"])
        self.assertTrue(any("BUILD_NETWORK" in m for m in logs))

    def test_passes_network_flag_when_configured(self):
        captured = {}

        def fake_stream(image_id, cmd, env=None):
            captured.setdefault("cmds", []).append(cmd)
            return True

        with patch.object(agent, "BUILD_NETWORK_REQUIRED", True), \
             patch.object(agent, "BUILD_NETWORK", "egress-restricted"), \
             patch.object(agent, "free_disk_gb", return_value=9999), \
             patch.object(agent, "push_log", lambda i, m: None), \
             patch.object(agent, "report_result", lambda i, s: None), \
             patch.object(agent, "stream_command", side_effect=fake_stream), \
             patch.object(agent.subprocess, "run", return_value=None):
            agent.run_build(self._job())

        build_cmd = captured["cmds"][0]
        self.assertIn("--network", build_cmd)
        self.assertEqual(build_cmd[build_cmd.index("--network") + 1], "egress-restricted")

    def test_opt_out_allows_unset_network(self):
        results = []
        with patch.object(agent, "BUILD_NETWORK_REQUIRED", False), \
             patch.object(agent, "BUILD_NETWORK", ""), \
             patch.object(agent, "free_disk_gb", return_value=9999), \
             patch.object(agent, "push_log", lambda i, m: None), \
             patch.object(agent, "report_result", lambda i, s: results.append(s)), \
             patch.object(agent, "stream_command", return_value=True), \
             patch.object(agent.subprocess, "run", return_value=None):
            agent.run_build(self._job())

        self.assertEqual(results, ["ready"])


class BuildWatchdogTests(unittest.TestCase):
    """BUILD_TIMEOUT must fire even when the build produces no output."""

    def test_silent_hang_is_killed_by_watchdog(self):
        # A command that emits nothing and sleeps far longer than the timeout. Without the
        # watchdog the `for line in proc.stdout` loop would block until the sleep finished.
        with patch.object(agent, "BUILD_TIMEOUT", 1), \
             patch.object(agent, "push_log", lambda i, m: None):
            ok = agent.stream_command(99, ["python3", "-c", "import time; time.sleep(60)"])
        self.assertFalse(ok)

    def test_fast_command_succeeds(self):
        with patch.object(agent, "BUILD_TIMEOUT", 30), \
             patch.object(agent, "push_log", lambda i, m: None):
            ok = agent.stream_command(99, ["python3", "-c", "print('hi')"])
        self.assertTrue(ok)

    def test_success_wins_over_late_watchdog_trip(self):
        # Race regression: if the watchdog Timer fires in the window between the process
        # finishing successfully and cancel(), returncode==0 must still win (not a timeout).
        real_timer = agent.threading.Timer

        class RaceTimer:
            """Fires the watchdog at cancel() time, i.e. in the finally block AFTER
            proc.wait() has reaped an rc==0 process -- the exact race the fix guards."""

            def __init__(self, interval, fn):
                self._fn = fn

            def start(self):
                pass

            def cancel(self):
                self._fn()

        with patch.object(agent, "BUILD_TIMEOUT", 30), \
             patch.object(agent, "push_log", lambda i, m: None), \
             patch.object(agent.threading, "Timer", RaceTimer):
            ok = agent.stream_command(99, ["python3", "-c", "print('done')"])
        # Even though the watchdog set timed_out and killed (a no-op on the reaped proc),
        # the successful exit code must be honored.
        self.assertTrue(ok)
        # Restore (defensive; patch already does, but make intent explicit).
        agent.threading.Timer = real_timer


if __name__ == "__main__":
    unittest.main()
