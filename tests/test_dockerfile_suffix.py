import os
import unittest

os.environ.setdefault("ADMIN_PASSWORD", "testpass")

from app.config import DOCKERFILE_SUFFIX  # noqa: E402


class DockerfileSuffixJupyterMandatoryTests(unittest.TestCase):
    """A build must fail (not be pushed 'ready') if Jupyter cannot be installed, since the
    workspace launches `jupyter lab` and is unusable without it."""

    def _lines(self):
        return [l.strip() for l in DOCKERFILE_SUFFIX.splitlines() if l.strip().startswith("RUN ")]

    def test_jupyter_install_does_not_swallow_failure(self):
        install = [l for l in self._lines() if "jupyterlab" in l]
        self.assertEqual(len(install), 1, "expected exactly one jupyterlab install RUN")
        # pip-variant fallbacks are fine, but the line must NOT end in `|| true`.
        self.assertFalse(
            install[0].rstrip().endswith("|| true"),
            "jupyter install must propagate failure so a broken build is not marked ready",
        )

    def test_jupyter_verification_gate_present(self):
        # A hard gate that the binary actually runs (converts a silently-incomplete base into
        # a build failure).
        self.assertTrue(
            any("jupyter lab --version" in l for l in self._lines()),
            "expected a `jupyter lab --version` verification RUN",
        )
        gate = next(l for l in self._lines() if "jupyter lab --version" in l)
        self.assertFalse(gate.rstrip().endswith("|| true"))

    def test_optional_services_stay_best_effort(self):
        # OpenCode is an optional side-service; it may keep `|| true`.
        text = DOCKERFILE_SUFFIX
        self.assertIn("opencode.ai/install", text)


if __name__ == "__main__":
    unittest.main()
