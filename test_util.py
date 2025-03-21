import unittest


class TestUtil(unittest.TestCase):
    def test_nice_dump(self):
        res = (
            " 1: 3 files (146)\n"
            "cleanup count:\n"
            " 1: 4 files (223)\n"
            " 0: 2 files (76)\n")

        self.assertEqual(
            "' 1: 3 files (146)\\n'\n"
            "'cleanup count:\\n'\n"
            "' 1: 4 files (223)\\n'\n"
            "' 0: 2 files (76)\\n'", nice_dump(res))


def nice_dump(result: str) -> str:
    return "\n".join(f.__repr__() for f in result.splitlines(keepends=True))
