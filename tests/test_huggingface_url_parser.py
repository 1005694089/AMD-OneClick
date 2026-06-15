import unittest

from app.notebook_sources import parse_github_path, parse_huggingface_demo_notebook_path


class HuggingFaceNotebookUrlTests(unittest.TestCase):

    def test_github_path_parses_blob_notebook(self):
        parsed = parse_github_path("org/repo/blob/main/notebooks/example.ipynb")

        self.assertEqual(parsed["org"], "org")
        self.assertEqual(parsed["repo"], "repo")
        self.assertEqual(parsed["branch"], "main")
        self.assertEqual(parsed["path"], "notebooks/example.ipynb")
        self.assertEqual(
            parsed["raw_url"],
            "https://raw.githubusercontent.com/org/repo/main/notebooks/example.ipynb",
        )

    def test_github_path_requires_blob_notebook(self):
        with self.assertRaises(ValueError):
            parse_github_path("org/repo/tree/main/notebooks/example.ipynb")

        with self.assertRaises(ValueError):
            parse_github_path("org/repo/blob/main/README.md")


    def test_huggingface_repo_root_ipynb_url_uses_main_branch(self):
        parsed = parse_huggingface_demo_notebook_path(
            "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb"
        )

        self.assertEqual(parsed["org"], "huggingface")
        self.assertEqual(parsed["repo"], "Qwen")
        self.assertEqual(parsed["branch"], "main")
        self.assertEqual(parsed["path"], "Qwen3.6-27B.ipynb")
        self.assertEqual(
            parsed["raw_url"],
            "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb",
        )

    def test_huggingface_blob_url_converts_to_resolve_url(self):
        parsed = parse_huggingface_demo_notebook_path(
            "https://huggingface.co/org/model/blob/demo/notebooks/example.ipynb"
        )

        self.assertEqual(parsed["repo"], "org/model")
        self.assertEqual(parsed["branch"], "demo")
        self.assertEqual(parsed["path"], "example.ipynb")
        self.assertEqual(
            parsed["raw_url"],
            "https://huggingface.co/org/model/resolve/demo/notebooks/example.ipynb",
        )

    def test_non_ipynb_huggingface_url_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_huggingface_demo_notebook_path(
                "https://huggingface.co/Qwen/README.md"
            )


if __name__ == "__main__":
    unittest.main()
