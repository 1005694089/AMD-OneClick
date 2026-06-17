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


if __name__ == "__main__":
    unittest.main()
