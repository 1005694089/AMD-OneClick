import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BETA_MANIFESTS = [
    ROOT / "k8s-radeon-beta.yaml",
    ROOT / "k8s-pr1-edge-shared-router.yaml",
]
PRODUCTION_V2_TOKENS = [
    "amd-oneclick-manager-v2",
    "amd-oneclick-config-v2",
    "amd-oneclick-secrets-v2",
    "amd-oneclick-postgres",
]


class ManifestScopeTests(unittest.TestCase):
    def test_beta_manifests_do_not_reference_production_v2_resources(self):
        combined = "\n".join(path.read_text(encoding="utf-8") for path in BETA_MANIFESTS)

        for token in PRODUCTION_V2_TOKENS:
            with self.subTest(token=token):
                self.assertNotIn(token, combined)

    def test_beta_manager_uses_beta_dns_override(self):
        manifest = (ROOT / "k8s-radeon-beta.yaml").read_text(encoding="utf-8")

        self.assertIn("dnsPolicy: None", manifest)
        self.assertIn("10.233.0.10", manifest)
        self.assertNotIn("8.8.8.8", manifest)

    def test_beta_notebooks_are_scoped_to_beta_nodes(self):
        manifest = (ROOT / "k8s-radeon-beta.yaml").read_text(encoding="utf-8")

        # Notebooks are no longer pinned to a single node: with the image-service enabled,
        # NOTEBOOK_NODE_NAME is empty so notebooks auto-spread across all eligible beta GPU
        # nodes (0043 + 0044) by free GPU capacity. Scoping is enforced by the beta taint
        # toleration, not a hard node pin.
        self.assertIn('NOTEBOOK_NODE_NAME: ""', manifest)
        self.assertIn('NOTEBOOK_TOLERATION_KEY: "amd-oneclick/beta"', manifest)
        self.assertIn("key: amd-oneclick/beta", manifest)
        self.assertIn('IMAGE_PULL_SECRET_NAME: "amd-oneclick-radeon-beta-regcred"', manifest)
        self.assertIn("imagePullSecrets:", manifest)

    def test_edge_router_uses_stable_service_dns_and_valid_beta_hostname(self):
        manifest = (ROOT / "k8s-pr1-edge-shared-router.yaml").read_text(encoding="utf-8")

        self.assertIn("server 10.233.51.189:80;", manifest)
        self.assertIn("server 10.233.98.233:80;", manifest)
        self.assertIn("listen 36.150.116.220:80 default_server;", manifest)
        self.assertIn("listen 36.150.116.220:443 ssl http2 default_server;", manifest)
        self.assertIn("server_name radeon-beta.anruicloud.com;", manifest)
        self.assertNotIn("radeon_beta.anruicloud.com", manifest)
        self.assertNotIn(".svc.cluster.local", manifest)


if __name__ == "__main__":
    unittest.main()
