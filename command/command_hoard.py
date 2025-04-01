import datetime
import logging
import os
import pathlib
import shutil
from io import StringIO
from typing import Dict, List, Tuple

from alive_progress import alive_bar, alive_it

from command.backups.command import HoardCommandBackups
from command.command_repo import RepoCommand
from command.contents.command import HoardCommandContents
from command.files.command import HoardCommandFiles
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.repo import ProspectiveRepo
from config import HoardRemote, CavePath, CaveType, ConnectionSpeed, ConnectionLatency
from contents.hoard import HoardContents
from contents.hoard_props import HoardDirProps, HoardFileProps
from contents.repo_props import RepoFileStatus
from exceptions import MissingRepo
from gui.hoard_explorer import start_hoard_explorer_gui
from hashing import fast_hash
from resolve_uuid import resolve_remote_uuid
from util import group_to_dict


def path_in_local(hoard_file: str, mounted_at: str) -> str:
    return pathlib.Path(hoard_file).relative_to(mounted_at).as_posix()


class HoardCommand(object):
    def __init__(self, path: str):
        self.hoard = Hoard(path)

        self.contents = HoardCommandContents(self.hoard)
        self.files = HoardCommandFiles(self.hoard)
        self.backups = HoardCommandBackups(self.hoard)

    def gui(self):
        start_hoard_explorer_gui(self.hoard.hoardpath)

    def init(self):
        logging.info(f"Reading or creating config...")
        self.hoard.config(True)
        return "DONE"

    def add_remote(
            self, remote_path: str, name: str, mount_point: str,
            type: str = "partial", fetch_new: bool = False,
            speed: ConnectionSpeed = ConnectionSpeed.INTERNAL_DRIVE,
            latency: ConnectionLatency = ConnectionLatency.ALWAYS):
        repo_type = CaveType(type)
        config = self.hoard.config()
        paths = self.hoard.paths()

        remote_abs_path = pathlib.Path(remote_path).absolute().as_posix()
        logging.info(f"Adding remote {remote_abs_path} to config...")

        logging.info("Loading remote from remote_path")

        try:
            repo = ProspectiveRepo(remote_abs_path)
            remote_uuid = repo.current_uuid
        except MissingRepo:
            return f"Repo not initialized at {remote_path}!"

        resolved_uuid = resolve_remote_uuid(self.hoard.config(), name)
        if resolved_uuid is not None and resolved_uuid != remote_uuid and resolved_uuid != name:  # fixme ugly AF
            raise ValueError(f"Remote uuid {name} already resolves to {resolved_uuid} and does not match {remote_uuid}")

        config.remotes.declare(remote_uuid, name, repo_type, mount_point, fetch_new)
        config.write()

        paths[remote_uuid] = CavePath.exact(remote_abs_path, speed, latency)
        paths.write()

        with self.hoard.open_contents(create_missing=True, is_readonly=False) as hoard:  # fixme remove when unit tests are updated
            hoard.config.set_max_size_fallback(remote_uuid, shutil.disk_usage(remote_path).total)

        return f"Added {name}[{remote_uuid}] at {remote_path}!"

    def mount_remote(self, remote: str, mount_point: str, force: bool = False):
        remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)
        logging.info(f"Reading config in {self.hoard.hoardpath}...")
        config = self.hoard.config()

        remote_doc = config.remotes[remote_uuid]
        if remote_doc is None:
            raise ValueError(f"remote {remote_uuid} does not exist")

        if remote_doc.mounted_at is not None and not force:
            return f"Remote {remote_uuid} already mounted in {remote_doc.mounted_at}, use --force to set.!"

        mount_path = pathlib.Path(mount_point)

        if not mount_path.is_relative_to("/"):
            return f"Mount point {mount_point} is absolute, must use relative!"

        logging.info(f"setting path to {mount_path.as_posix()}")

        remote_doc.mount_at(mount_path.as_posix())
        config.write()

        return f"set path of {remote} to {mount_path.as_posix()}\n"

    def remotes(self, hide_paths: bool = False):
        logging.info(f"Reading config in {self.hoard.hoardpath}...")
        config = self.hoard.config()

        with StringIO() as out:
            out.write(f"{len(config.remotes)} total remotes.\n")
            for remote in config.remotes.all():
                name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""
                path = self.hoard.paths()[remote.uuid]
                exact_path = f" in {path.find()} [{path.speed.value}: {path.latency.value}]" if not hide_paths else ""

                out.write(f"  {name_prefix}{remote.uuid} ({remote.type.value}){exact_path}\n")
            out.write("Mounts:\n")

            mount_point_to_mount = group_to_dict(config.remotes.all(), key=lambda r: r.mounted_at)
            for mount, remotes in sorted(mount_point_to_mount.items()):
                out.write(f"  {mount} -> {', '.join(r.name for r in remotes)}\n")
            out.write("DONE\n")
            return out.getvalue()

    def health(self):
        logging.info("Loading config")
        config = self.hoard.config()

        logging.info(f"Loading hoard TOML...")
        with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            logging.info(f"Loaded hoard TOML!")

            repo_health: Dict[str, Dict[int, int]] = dict()
            health_files: Dict[int, List[str]] = dict()
            for file, props in hoard.fsobjects:
                if not isinstance(props, HoardFileProps):
                    continue  # fixme what about folders?

                num_copies = len(props.available_at)
                if num_copies not in health_files:
                    health_files[num_copies] = []
                health_files[num_copies].append(file)

                # count how many files are uniquely stored here
                for repo in props.available_at:
                    if repo not in repo_health:
                        repo_health[repo] = dict()
                    if num_copies not in repo_health[repo]:
                        repo_health[repo][num_copies] = 0
                    repo_health[repo][num_copies] += 1

            with StringIO() as out:
                out.write("Health stats:\n")
                out.write(f"{len(config.remotes)} total remotes.\n")
                for remote in config.remotes.all():
                    name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""
                    out.write(
                        f"  {name_prefix}{remote.uuid}: {repo_health.get(remote.uuid, {}).get(1, 0)} with no other copy\n")

                out.write("Hoard health stats:\n")
                for num, files in sorted(health_files.items()):
                    out.write(f"  {num} copies: {len(files)} files\n")
                out.write("DONE")
                return out.getvalue()

    def clone(self, to_path: str, mount_at: str, name: str, fetch_new: bool = False):
        _ = self.hoard.config()  # validate hoard is available

        if not os.path.isdir(to_path):
            return f"Cave dir {to_path} to create does not exist!"

        cave_cmd = RepoCommand(path=to_path)
        cave_cmd.init()
        cave_cmd.refresh(show_details=False)

        self.add_remote(to_path, name=name, mount_point=mount_at, fetch_new=fetch_new)
        return f"DONE"

    def move_mounts(self, from_path: str, to_path: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        from_path_in_hoard = pathing.in_hoard(from_path)
        to_path_in_hoard = pathing.in_hoard(to_path)

        repos_to_move: List[HoardRemote] = []
        for remote in config.remotes.all():
            if pathlib.Path(remote.mounted_at).is_relative_to(from_path):
                # mounted_at is a subfolder of from_path
                logging.info(f"{remote.name} will be moved as {remote.mounted_at} is subfolder of {from_path}")
                repos_to_move.append(remote)
                continue

            path_in_remote = from_path_in_hoard.at_local(remote.uuid)
            if path_in_remote is None:
                logging.info(f"Remote {remote.uuid} does not map path {from_path_in_hoard.as_pure_path.as_posix()} ... skipping")
                continue

            assert path_in_remote.as_pure_path.as_posix() != ".", f'{path_in_remote.as_pure_path.as_posix()} should be local folder "."'

            logging.warning(
                f"Remote {remote.uuid} contains path {from_path_in_hoard.as_pure_path.as_posix()}"
                f" as inner {path_in_remote}, which requires moving files.")
            return f"Can't move {from_path} to {to_path}, requires moving files in {remote.name}:{path_in_remote.as_pure_path.as_posix()}.\n"

        if len(repos_to_move) == 0:
            return f"No repos to move!"

        logging.info(f"Loading hoard...")
        with self.hoard.open_contents(create_missing=False, is_readonly=False) as hoard:
            logging.info(f"Loaded hoard.")

            with StringIO() as out:
                out.write("Moving files and folders:\n")
                for orig_path, props in list(hoard.fsobjects):
                    assert isinstance(props, HoardFileProps) or isinstance(props, HoardDirProps), \
                        f"Unsupported props type: {type(props)}"
                    current_path = pathlib.Path(orig_path)
                    if current_path.is_relative_to(from_path):
                        rel_path = current_path.relative_to(from_path)
                        logging.info(f"Relative file path to move: {rel_path}")
                        new_path = pathlib.Path(to_path).joinpath(rel_path).as_posix()

                        out.write(f"{orig_path}=>{new_path}\n")
                        hoard.fsobjects.move_via_mounts(orig_path, new_path, props)

                logging.info(f"Moving {', '.join(r.name for r in repos_to_move)}.")
                out.write(f"Moving {len(repos_to_move)} repos:\n")
                for remote in repos_to_move:
                    relative_repo_mounted_at = pathlib.Path(remote.mounted_at).relative_to(from_path)
                    logging.info(
                        f"[{remote.name} is mounted {relative_repo_mounted_at.as_posix()} rel. to {from_path}]")
                    final_mount_path = pathlib.Path(
                        to_path_in_hoard.as_pure_path.as_posix()).joinpath(relative_repo_mounted_at)
                    logging.info(f"re-mounting it to {final_mount_path}")

                    out.write(f"[{remote.name}] {remote.mounted_at} => {final_mount_path.as_posix()}\n")
                    remote.mount_at(final_mount_path.as_posix())

                logging.info("Writing config...")
                config.write()

                out.write("DONE")
                return out.getvalue()

    def export_contents_to_repo(self, remote: str):
        remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)

        logging.info(f"Loading hoard TOML...")
        with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            logging.info(f"Removing old contents...")
            self.hoard.connect_to_repo(remote_uuid, require_contents=False).remove_contents()

            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())
            logging.info(f"Opening new contents of {remote_uuid}...")
            with self.hoard.connect_to_repo(remote_uuid, require_contents=False) \
                    .create_contents(remote_uuid) as current_contents:
                current_contents.config.start_updating()

                logging.info("Restoring config...")
                hoard.config.restore_remote_config(current_contents.config)

                with StringIO() as out:
                    logging.info(f"Iterating over files marked available in {remote}...")
                    for hoard_file, hoard_props in alive_it(
                            hoard.fsobjects.available_in_repo(remote_uuid), title="Recreating index"):
                        local_path_obj = pathing.in_hoard(hoard_file).at_local(remote_uuid)
                        assert local_path_obj is not None, \
                            f"Path {hoard_file} needs to be available in local, but isn't???"
                        local_path = local_path_obj.as_pure_path.as_posix()

                        if isinstance(hoard_props, HoardFileProps):
                            logging.info(f"Restoring description of file {hoard_file} to {local_path}...")
                            current_contents.fsobjects.add_file(
                                local_path,
                                size=hoard_props.size,
                                mtime=datetime.datetime.now(),
                                fasthash=hoard_props.fasthash,
                                status=RepoFileStatus.PRESENT)
                            out.write(f"PRESENT {local_path}\n")
                        elif isinstance(hoard_props, HoardDirProps):
                            logging.info(f"Restoring description of dir {hoard_file} to {local_path}...")
                            current_contents.fsobjects.add_dir(local_path, RepoFileStatus.PRESENT)
                            out.write(f"PRESENT DIR {local_path}\n")
                        else:
                            raise ValueError(f"Unsupported hoard props type: {type(hoard_props)}")

                    current_contents.config.end_updating()
                    out.write("DONE")
                    return out.getvalue()

    def meld(
            self, source: str, dest: str, move: bool = False, junk_folder: str = "_JUNK_",
            skip_empty_files: bool = True):
        if not os.path.isdir(source):
            return f"Source path {source} does not exist!"
        if not os.path.isdir(dest):
            return f"Dest path {dest} does not exist!"
        if len(os.listdir(dest)) != 0:
            return f"Dest path {dest} must be empty!"

        if move:
            print("Moving files to proper locations!")
        else:
            print("Copying files to proper locations!")

        logging.info(f"Loading hoard...")
        with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            logging.info(f"Loaded hoard.")
            junk_path = pathlib.Path(dest).joinpath(junk_folder)
            if move:
                junk_path.mkdir()

            copied, copied_dest, mismatched, errors, skipped = 0, 0, 0, 0, 0
            with StringIO() as out:
                with alive_bar() as bar:
                    for dirpath, _, filenames in os.walk(source):
                        for filename in filenames:
                            bar()

                            fullpath = os.path.join(dirpath, filename)
                            logging.info(f"Full path: {fullpath}")
                            rel_to_source = pathlib.Path(fullpath).relative_to(source)
                            logging.info(f"Rel path: {rel_to_source}")

                            fasthash = fast_hash(fullpath)
                            size = os.stat(fullpath).st_size
                            if size == 0 and skip_empty_files:
                                logging.warning(f"Skipping empty file{fullpath}")
                                skipped += 1
                                continue

                            places: List[Tuple[str, HoardFileProps]] = list(hoard.fsobjects.by_fasthash(fasthash))
                            if len(places) == 0:
                                mismatched += 1

                                dest_junk_path = junk_path.joinpath(rel_to_source)
                                rel_junk_path = dest_junk_path.relative_to(dest).as_posix()
                                if move:
                                    logging.info(f"Copying {fullpath} to {dest_junk_path}")
                                    dest_junk_path.parent.mkdir(parents=True, exist_ok=True)
                                    try:
                                        logging.info(f"m+{rel_junk_path}\n")
                                        shutil.move(fullpath, dest_junk_path)
                                        out.write(f"+{rel_junk_path}\n")
                                    except shutil.Error as e:
                                        logging.error(e)
                                        errors += 0
                                        out.write(f"E{rel_junk_path}\n")
                                else:  # do nothing, as we are preserving the input
                                    out.write(f"s{rel_junk_path}\n")
                            else:
                                copied += 1
                                while len(places) > 1:  # all but the last file...
                                    hoard_filepath, hoard_props = places.pop()

                                    # use + because hoard paths are absolute!
                                    end_place = pathlib.Path(dest + hoard_filepath)
                                    logging.info(f"Creating {end_place} from {fullpath}")
                                    end_place.parent.mkdir(parents=True, exist_ok=True)

                                    try:
                                        # copy (as we will need it for the last)
                                        shutil.copy2(fullpath, end_place)
                                        out.write(f"A{hoard_filepath}\n")

                                        copied_dest += 1
                                    except shutil.Error as e:
                                        logging.error(e)
                                        errors += 0
                                        out.write(f"E{end_place}\n")

                                assert len(places) == 1
                                hoard_filepath, hoard_props = places.pop()

                                # use + because hoard paths are absolute!
                                end_place = pathlib.Path(dest + hoard_filepath)
                                logging.info(f"Creating {end_place} from {fullpath}")
                                end_place.parent.mkdir(parents=True, exist_ok=True)

                                try:
                                    if move:
                                        shutil.move(fullpath, end_place)
                                        out.write(f"M{hoard_filepath}\n")
                                    else:
                                        shutil.copy2(fullpath, end_place)
                                        out.write(f"A{hoard_filepath}\n")

                                    copied_dest += 1
                                except shutil.Error as e:
                                    logging.error(e)
                                    errors += 0
                                    out.write(f"E{end_place}\n")

                out.write(
                    f"Copied: {copied} to Dest: {copied_dest}, Mismatched: {mismatched},"
                    f" Errors: {errors} and Skipped: {skipped}\n")
                return out.getvalue()
