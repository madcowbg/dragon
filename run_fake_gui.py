import os
import shutil
from pathlib import Path

import fire

from command.backups.test_backups_maintenance import populate_random_data, add_remotes
from dragon import TotalCommand
from lmdb_storage.test_performance_with_fake_data import vocabulary_short, populate_index

fake_data_path = Path("./tests/fake_hoard").absolute()


async def test_create_random_hoard_data():
    custom_path = "./tests/fake_hoard"
    shutil.rmtree(Path(custom_path).resolve(), ignore_errors=True)
    Path(custom_path).resolve().mkdir(parents=True, exist_ok=True)

    partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(custom_path)

    await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)

    await hoard_cmd.contents.pull(all=True)

    await hoard_cmd.files.push(all=True)

    backup_fake_repo_path = Path(custom_path).joinpath("backup-fake")
    backup_fake_repo_path.mkdir(parents=True, exist_ok=False)

    for fpath, rnddata, size in populate_index(100 + 99, 7, vocabulary=vocabulary_short, chance_pct=85):
        file_path = backup_fake_repo_path.joinpath(fpath)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path.with_suffix(".file").write_text(rnddata * size)

    backup_fake_cmd = TotalCommand(path=backup_fake_repo_path.as_posix()).cave
    backup_fake_cmd.init()
    await backup_fake_cmd.refresh()

    hoard_cmd.add_remote(remote_path=backup_fake_cmd.repo.path, name="backup-fake", mount_point="/", type="backup")

    res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
    assert (
        'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
        '|Num Files           |total |availa|\n'
        '|backup-fake         |      |      |\n'
        '|repo-full           |    13|    13|\n'
        '|work-repo-0         |     5|     5|\n'
        '|work-repo-1         |     4|     4|\n'
        '|work-repo-2         |     5|     5|\n'
        '\n'
        '|Size                |total |availa|\n'
        '|backup-fake         |      |      |\n'
        '|repo-full           |16.8KB|16.8KB|\n'
        '|work-repo-0         | 6.2KB| 6.2KB|\n'
        '|work-repo-1         | 5.9KB| 5.9KB|\n'
        '|work-repo-2         | 6.6KB| 6.6KB|\n') == res

async def create():
    shutil.rmtree(fake_data_path)
    await test_create_random_hoard_data()

    fake_data_path.with_suffix(".zip").unlink()
    shutil.make_archive(fake_data_path.as_posix(), "zip", fake_data_path)

async def reset():
    assert fake_data_path.with_suffix(".zip").exists()

    shutil.rmtree(fake_data_path)
    fake_data_path.mkdir(parents=True, exist_ok=True)

    shutil.unpack_archive(fake_data_path.with_suffix(".zip"), fake_data_path)


def run():
    assert fake_data_path.exists(), f"Need to create fake data at {fake_data_path} first!"
    os.chdir(fake_data_path.joinpath("hoard"))
    TotalCommand().hoard.gui()


if __name__ == '__main__':
    fire.Fire()
