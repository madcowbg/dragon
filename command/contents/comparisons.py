import logging
from pathlib import PurePosixPath
from typing import Generator, Dict

from alive_progress import alive_bar, alive_it

from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps, HoardDirProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus, RepoDirProps
from contents_diff import Diff, FileOnlyInLocal, FileIsSame, FileContentsDiffer, \
    DirMissingInHoard, DirIsSame, FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalMoved, \
    DirMissingInLocal


def is_same_file(current: RepoFileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def compare_local_to_hoard(local: RepoContents, hoard: HoardContents, pathing: HoardPathing) \
        -> Generator[Diff, None, None]:
    #
    # logging.warning("creating temp table")
    # hoard.conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS tmp_local_objects (fullpath TEXT NOT NULL)")
    # hoard.conn.execute("DELETE FROM tmp_local_objects")
    #
    # mounted_at = pathing.mounted_at(local.uuid)
    # logging.warning("getting all paths")
    # all_paths = list(local.conn.execute("SELECT ? || '/' || fullpath FROM fsobject", (mounted_at.as_posix(),)))
    # logging.warning(f"inserting {len(all_paths)} paths to temp table")
    #
    # hoard.conn.executemany("INSERT INTO tmp_local_objects (fullpath) VALUES (?)", all_paths)
    # logging.warning("all paths inserted")
    #
    # for line in hoard.conn.execute("SELECT * FROM tmp_local_objects LIMIT 10"):
    #     print(line)
    #
    # for line in hoard.conn.execute("SELECT * FROM fsobject LIMIT 10"):
    #     print(line)
    #
    # logging.warning("finding all missing in hoard")
    # all_missing_in_hoard = list(hoard.conn.execute(
    #     "SELECT fsobject_id, fsobject.fullpath "
    #     "FROM fsobject LEFT OUTER JOIN tmp_local_objects ON fsobject.fullpath = tmp_local_objects.fullpath "
    #     "WHERE tmp_local_objects.fullpath IS NULL"))
    # logging.warning(f"found {len(all_missing_in_hoard)} are missing!")
    #
    # logging.warning("finding all missing in repo")
    # all_missing_in_repo = list(hoard.conn.execute(
    #     "SELECT fsobject_id, tmp_local_objects.fullpath "
    #     "FROM tmp_local_objects LEFT OUTER JOIN fsobject ON fsobject.fullpath = tmp_local_objects.fullpath "
    #     "WHERE fsobject.fullpath IS NULL"))
    # logging.warning(f"found {len(all_missing_in_repo)} are missing!")
    #
    logging.warning(f"loading all local existing objects")
    all_local_existing: Dict[PurePosixPath, RepoFileProps | RepoDirProps] = dict(local.fsobjects.existing())
    all_local_with_any_status: Dict[PurePosixPath, RepoFileProps | RepoDirProps] = dict(local.fsobjects.all_status())
    logging.warning(f"loaded {len(all_local_existing)} local existing objects")

    logging.warning(f"loading all hoard existing objects")
    all_hoard_in_folder: Dict[PurePosixPath, HoardFileProps | HoardDirProps] = dict(hoard.fsobjects.in_folder(pathing.mounted_at(local.config.uuid)))
    logging.warning(f"loaded {len(all_hoard_in_folder)} local existing objects")
    #
    # raise NotImplementedError()


    with alive_bar(local.fsobjects.len_existing(), title="Current files vs. Hoard") as bar:
        for current_path, props in all_local_existing.items():
            bar()
            if isinstance(props, RepoFileProps):
                current_file = current_path
                curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
                if curr_file_hoard_path.as_pure_path not in all_hoard_in_folder:
                    logging.info(f"local file not in hoard: {curr_file_hoard_path}")
                    yield FileOnlyInLocal(
                        current_file, curr_file_hoard_path.as_pure_path, props,
                        props.last_status == RepoFileStatus.ADDED)
                elif is_same_file(
                        all_local_existing[current_file],
                        all_hoard_in_folder[curr_file_hoard_path.as_pure_path]):
                    logging.info(f"same in hoard {current_file}!")
                    yield FileIsSame(current_file, curr_file_hoard_path.as_pure_path, props, all_hoard_in_folder[
                        curr_file_hoard_path.as_pure_path])
                else:
                    logging.info(f"file changes {current_file}")
                    yield FileContentsDiffer(
                        current_file,
                        curr_file_hoard_path.as_pure_path, props, all_hoard_in_folder[
                            curr_file_hoard_path.as_pure_path])

            elif isinstance(props, RepoDirProps):
                current_dir = current_path
                curr_dir_hoard_path = pathing.in_local(current_dir, local.config.uuid).at_hoard()
                if curr_dir_hoard_path.as_pure_path not in all_hoard_in_folder:
                    logging.info(f"new dir found: {current_dir}")
                    yield DirMissingInHoard(current_dir, curr_dir_hoard_path.as_pure_path)
                else:
                    yield DirIsSame(current_dir, curr_dir_hoard_path.as_pure_path)
            else:
                raise ValueError(f"unknown props type: {type(props)}")

    hoard_file: PurePosixPath
    for hoard_file, props in alive_it(
            all_hoard_in_folder.items(),
            title="Hoard vs. Current files"):
        if isinstance(props, HoardFileProps):
            curr_file_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
            assert curr_file_path_in_local is not None  # hoard file is not in the mounted location

            local_props: RepoFileProps | None = all_local_with_any_status.get(
                curr_file_path_in_local.as_pure_path, None)
            if local_props is None:
                yield FileOnlyInHoardLocalUnknown(curr_file_path_in_local.as_pure_path, hoard_file, props)
            elif local_props.last_status == RepoFileStatus.DELETED:
                yield FileOnlyInHoardLocalDeleted(
                    curr_file_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status == RepoFileStatus.MOVED_FROM:
                yield FileOnlyInHoardLocalMoved(
                    curr_file_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status in (RepoFileStatus.ADDED, RepoFileStatus.PRESENT, RepoFileStatus.MODIFIED):
                pass  # file is there, which is handled above
            else:
                raise ValueError(f"Unrecognized state: {local_props.last_status}")
        elif isinstance(props, HoardDirProps):
            hoard_dir = hoard_file
            curr_dir_path_in_local = pathing.in_hoard(hoard_dir).at_local(local.config.uuid)
            assert curr_dir_path_in_local is not None  # hoard dir is not in the mounted location

            if curr_dir_path_in_local.as_pure_path not in all_local_existing:
                logging.info(f"missing dir found in hoard: {hoard_dir}")
                yield DirMissingInLocal(curr_dir_path_in_local.as_pure_path, hoard_dir)
            else:
                pass  # existing dirs are handled above
        else:
            raise ValueError(f"unknown props type: {type(props)}")
