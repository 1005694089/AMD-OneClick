import re
import subprocess
import unittest
from pathlib import Path

SETUP = Path(__file__).resolve().parent.parent / "build-agent" / "setup.sh"


class SetupBuildNetworkDefaultTests(unittest.TestCase):
    """setup.sh must not write an env file the agent rejects: the agent fails closed unless
    BUILD_NETWORK is set, so setup must emit a safe non-empty default ('none')."""

    def test_default_is_none_when_unset(self):
        text = SETUP.read_text()
        # The safe default assignment exists.
        self.assertIn('BUILD_NETWORK="${BUILD_NETWORK:-none}"', text)
        # The env-file heredoc emits BUILD_NETWORK (not a commented example).
        self.assertTrue(
            re.search(r"^BUILD_NETWORK=\$\{BUILD_NETWORK\}", text, re.M),
            "setup.sh env heredoc must emit an uncommented BUILD_NETWORK line",
        )
        self.assertNotIn("# BUILD_NETWORK=oneclick-build-egress", text)

    def test_expands_to_nonempty_value(self):
        # Evaluate the default + the env line exactly as the script would, with BUILD_NETWORK
        # unset, and confirm the resulting env line carries a non-empty value.
        script = 'BUILD_NETWORK="${BUILD_NETWORK:-none}"\n' 'printf "BUILD_NETWORK=%s\\n" "${BUILD_NETWORK}"\n'
        out = subprocess.run(
            ["bash", "-c", script], capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"}
        ).stdout.strip()
        self.assertEqual(out, "BUILD_NETWORK=none")

    def test_operator_override_is_respected(self):
        script = 'BUILD_NETWORK="${BUILD_NETWORK:-none}"\n' 'printf "BUILD_NETWORK=%s\\n" "${BUILD_NETWORK}"\n'
        out = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={"PATH": "/usr/bin:/bin", "BUILD_NETWORK": "egress-restricted"},
        ).stdout.strip()
        self.assertEqual(out, "BUILD_NETWORK=egress-restricted")


class SetupSystemdUserTests(unittest.TestCase):
    """setup.sh must run the systemd service as the operator-selected AGENT_USER, not the
    hard-coded 'buildagent', so an overridden user can actually read its DOCKER_CONFIG dir."""

    def test_setup_templates_user_into_unit(self):
        text = SETUP.read_text()
        # setup.sh rewrites User=/Group= rather than installing the unit verbatim.
        self.assertIn("s/^User=.*/User=${AGENT_USER}/", text)
        self.assertIn("s/^Group=.*/Group=${AGENT_USER}/", text)
        # And it must not blindly `install` the unit (which would keep User=buildagent).
        self.assertNotIn(
            'install -m 0644 "${SRC_DIR}/systemd/build-agent.service" /etc/systemd/system/build-agent.service',
            text,
        )

    def test_sed_substitution_produces_selected_user(self):
        # Reproduce the exact sed transform on the shipped unit and confirm both lines change.
        unit = SERVICE.read_text()
        script = (
            'sed -e "s/^User=.*/User=${AGENT_USER}/" '
            '-e "s/^Group=.*/Group=${AGENT_USER}/"'
        )
        out = subprocess.run(
            ["bash", "-c", f'{script} "$1"', "_", str(SERVICE)],
            capture_output=True,
            text=True,
            env={"PATH": "/usr/bin:/bin", "AGENT_USER": "custombuilder"},
        ).stdout
        self.assertIn("User=custombuilder", out)
        self.assertIn("Group=custombuilder", out)
        self.assertNotIn("User=buildagent", out)
        self.assertNotIn("Group=buildagent", out)

    def test_setup_validates_agent_user_charset(self):
        # AGENT_USER feeds useradd, chown, AND a sed replacement; a value with sed metachars
        # (& / \) must be rejected before it can silently corrupt the rewritten unit.
        text = SETUP.read_text()
        self.assertIn("AGENT_USER", text)
        self.assertTrue(
            re.search(r"grep -Eq '\^\[a-z_\]\[a-z0-9_-\]\*\\\$\?\$'", text)
            or "[a-z_][a-z0-9_-]*" in text,
            "setup.sh must validate AGENT_USER against a safe username charset",
        )

    def test_validation_rejects_sed_metachar_users(self):
        # Reproduce just the validation guard and confirm it rejects dangerous values and
        # accepts ordinary ones.
        guard = (
            "if ! printf '%s' \"${AGENT_USER}\" | grep -Eq "
            "'^[a-z_][a-z0-9_-]*\\$?$'; then echo bad; exit 1; fi; echo ok"
        )
        def run(val):
            return subprocess.run(
                ["bash", "-c", guard],
                capture_output=True, text=True,
                env={"PATH": "/usr/bin:/bin", "AGENT_USER": val},
            )
        for bad in ("r&d", "a/b", "a\\b", "Build Agent", "UPPER"):
            self.assertEqual(run(bad).stdout.strip(), "bad", f"should reject {bad!r}")
        for good in ("buildagent", "custom-builder", "_svc", "agent1"):
            self.assertEqual(run(good).stdout.strip(), "ok", f"should accept {good!r}")


SERVICE = Path(__file__).resolve().parent.parent / "build-agent" / "systemd" / "build-agent.service"


if __name__ == "__main__":
    unittest.main()
