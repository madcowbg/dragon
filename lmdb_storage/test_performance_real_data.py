import os
import unittest
from unittest.async_case import IsolatedAsyncioTestCase

from dragon import TotalCommand


@unittest.skipUnless(os.getenv('RUN_LENGTHY_TESTS'), reason="Lengthy test")
class TestPerformance(IsolatedAsyncioTestCase):

    async def test_load_all_and_get_size(self):
        path = r"C:\Users\Bono\hoard"

        hoard_cmd = TotalCommand(path=path).hoard

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: e10e064c040fbe9395a889b089a84abc9d51027c\n'
            '|Num Files                |total     |available |\n'
            '|GoPro@NAS                |      1579|      1579|\n'
            '|Insta360@NAS             |      3820|      3820|\n'
            '|Videos@NAS               |      5515|      5515|\n'
            '|cloud-drive@laptop       |    156010|    156010|\n'
            '\n'
            '|Size                     |total     |available |\n'
            '|GoPro@NAS                |     1.3TB|     1.3TB|\n'
            '|Insta360@NAS             |     8.5TB|     8.5TB|\n'
            '|Videos@NAS               |     1.7TB|     1.7TB|\n'
            '|cloud-drive@laptop       |    64.9GB|    64.9GB|\n'), res)
