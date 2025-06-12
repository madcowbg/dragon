import logging
import os
import pathlib
import shutil
from io import StringIO
from typing import Dict, List, Tuple, TextIO, Any

from alive_progress import alive_bar, alive_it

from command.backups.command import HoardCommandBackups
from command.command_repo import RepoCommand
from command.content_prefs import Presence
from command.contents.command import HoardCommandContents
from command.fast_path import FastPosixPath
from command.files.command import HoardCommandFiles
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.repo import ProspectiveRepo
from config import HoardRemote, CavePath, CaveType, ConnectionSpeed, ConnectionLatency
from contents.hoard import HoardContents, MovesAndCopies
from contents.repo import RepoContents
from exceptions import MissingRepo
from gui.hoard_explorer import start_hoard_explorer_gui
from hashing import fast_hash
from lmdb_storage.file_object import FileObject
from lmdb_storage.lookup_tables_paths import fast_compressed_path_dfs
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_object import ObjectType, MaybeObjectID
from lmdb_storage.tree_operations import get_child, remove_child
from lmdb_storage.tree_structure import ObjectID, add_object, Objects
from resolve_uuid import resolve_remote_uuid
from util import group_to_dict, run_in_separate_loop, safe_hex, format_size


def path_in_local(hoard_file: str, mounted_at: str) -> str:
    return pathlib.Path(hoard_file).relative_to(mounted_at).as_posix()


def fasthash_len_distribution(existing_fasthashes: Dict[str, List[Any]]) -> list[tuple[int, int]]:
    fasthashes_by_fasthash_length = group_to_dict(existing_fasthashes.items(), key=lambda g: len(g[0]))
    fasthashes_len_distrib = list(sorted(
        (length, sum(len(fl) for fl in file_lists))
        for length, file_lists in fasthashes_by_fasthash_length.items()))
    return fasthashes_len_distrib


class HoardCommand(object):
    def __init__(self, path: str):
        self.hoard = Hoard(path)

        self.contents = HoardCommandContents(self.hoard)
        self.files = HoardCommandFiles(self.hoard)
        self.backups = HoardCommandBackups(self.hoard)

    def gui(self):
        start_hoard_explorer_gui(self.hoard.hoardpath)

    async def init(self):
        logging.info(f"Reading or creating config...")
        self.hoard.config(True)

        logging.info(f"Opening or creating contents...")
        async with self.hoard.open_contents(create_missing=True).writeable():
            logging.info(f"Opened contents!")
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

        async def hack():  # fixme remove when unit tests are updated
            async with self.hoard.open_contents(create_missing=True).writeable() as hoard:
                hoard.config.set_max_size_fallback(remote_uuid, shutil.disk_usage(remote_path).total)

                remote_root = hoard.env.roots(write=True)[remote_uuid]
                remote_root.desired = None
                remote_root.current = None

        run_in_separate_loop(hack())

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

            mount_point_to_mount: Dict[FastPosixPath, List[HoardRemote]] = \
                group_to_dict(config.remotes.all(), key=lambda r: r.mounted_at)
            for mount, remotes in sorted(mount_point_to_mount.items()):
                out.write(f"  {mount.as_posix()} -> {', '.join(r.name for r in remotes)}\n")
            out.write("DONE\n")
            return out.getvalue()

    async def health(self):
        logging.info("Loading config")
        config = self.hoard.config()

        logging.info(f"Loading hoard TOML...")
        async with self.hoard.open_contents(create_missing=False) as hoard:
            logging.info(f"Loaded hoard TOML!")

            repo_health: Dict[str, Dict[int, int]] = dict()
            health_files: Dict[int, List[FastPosixPath]] = dict()
            presence = Presence(hoard)
            with alive_bar(title="Iterating hoard...") as bar:
                for hoard_file, file_obj in hoard.fsobjects.hoard_files():
                    assert isinstance(file_obj, FileObject)

                    uuid_current = list(presence.in_current(hoard_file, file_obj))

                    num_copies = len(uuid_current)
                    if num_copies not in health_files:
                        health_files[num_copies] = []
                    health_files[num_copies].append(hoard_file)

                    # count how many files are uniquely stored here
                    for repo in uuid_current:
                        if repo not in repo_health:
                            repo_health[repo] = dict()
                        if num_copies not in repo_health[repo]:
                            repo_health[repo][num_copies] = 0
                        repo_health[repo][num_copies] += 1
                    bar()

            existing_fast_hashes = read_all_current_hashes(hoard)

            hoard_fasthashes: Dict[str, List[Tuple[FastPosixPath, FileObject]]] = dict()
            for path, file in hoard.fsobjects.desired_hoard():
                if file.fasthash not in hoard_fasthashes:
                    hoard_fasthashes[file.fasthash] = []

                hoard_fasthashes[file.fasthash].append((path, file))

            with StringIO() as out:
                out.write("Health stats:\n")
                out.write(f"{len(config.remotes)} total remotes.\n")
                for remote in config.remotes.all():
                    name_prefix = f"[{remote.name}]" if remote.name != "INVALID" else ""
                    out.write(
                        f"  {name_prefix}: {repo_health.get(remote.uuid, {}).get(1, 0)} with no other copy\n")

                out.write("Hoard health stats:\n")
                for num, files in sorted(health_files.items()):
                    out.write(f"  {num} copies: {len(files)} files\n")

                out.write("Fasthash health stats:\n")
                out.write(f" #existing fasthashes = {len(existing_fast_hashes)}\n")
                for l, c in fasthash_len_distribution(existing_fast_hashes):
                    out.write(f"  len {l} -> {c}\n")

                out.write(f" #hoard fasthashes = {len(hoard_fasthashes)}\n")
                for l, c in fasthash_len_distribution(hoard_fasthashes):
                    out.write(f"  len {l} -> {c}\n")

                existing_but_not_in_hoard = set(existing_fast_hashes.keys()) - set(hoard_fasthashes.keys())
                out.write(f" #existing but not in hoard: {len(existing_but_not_in_hoard)}\n")
                hoard_but_not_existing = set(hoard_fasthashes.keys()) - set(existing_fast_hashes.keys())
                out.write(
                    f" #hoard but not existing: {len(hoard_but_not_existing)}"
                    f"{" BAD!" if len(hoard_but_not_existing) > 0 else ""}\n")

                num_copies_of_hoard_filehashes = dict(
                    (fasthash, len(existing_fast_hashes[fasthash])) for fasthash in hoard_fasthashes)
                count_to_lfc = group_to_dict(num_copies_of_hoard_filehashes.items(), lambda fc: fc[1])
                for count, lfc in sorted(count_to_lfc.items()):
                    existing_sizes = [existing_fast_hashes[fasthash][0][1].size for fasthash, _ in lfc]
                    total_sizes = [
                        sum(file_copy[1].size for file_copy in existing_fast_hashes[fasthash]) for fasthash, _ in lfc]

                    set_sizes = set(existing_sizes)
                    assert len(set_sizes) > 0

                    if len(set_sizes) == 1:
                        size_est = f"{format_size(sum(total_sizes))} = {len(lfc)} x {count} x {format_size(min(set_sizes))}"
                    else:
                        size_est = (
                            f"{format_size(sum(total_sizes))} = {len(lfc)} x {count} x ({format_size(min(set_sizes))} ~ {format_size(max(set_sizes))})")
                    out.write(f"  {count} copies - {len(lfc)} hashes, space est: {size_est}\n")

                out.write("DONE")
                return out.getvalue()

    async def clone(self, to_path: str, mount_at: str, name: str, fetch_new: bool = False):
        _ = self.hoard.config()  # validate hoard is available

        if not os.path.isdir(to_path):
            return f"Cave dir {to_path} to create does not exist!"

        cave_cmd = RepoCommand(path=to_path)
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        self.add_remote(to_path, name=name, mount_point=mount_at, fetch_new=fetch_new)
        return f"DONE"

    async def move_mounts(self, from_path: str, to_path: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        from_path_in_hoard = pathing.in_hoard(FastPosixPath(from_path))
        to_path_in_hoard = pathing.in_hoard(FastPosixPath(to_path))

        repos_to_move: List[HoardRemote] = []
        for remote in config.remotes.all():
            if remote.mounted_at.is_relative_to(from_path):
                # mounted_at is a subfolder of from_path
                logging.info(f"{remote.name} will be moved as {remote.mounted_at} is subfolder of {from_path}")
                repos_to_move.append(remote)
                continue

            path_in_remote = from_path_in_hoard.at_local(remote.uuid)
            if path_in_remote is None:
                logging.info(f"Remote {remote.uuid} does not map path {from_path_in_hoard} ... skipping")
                continue

            assert path_in_remote.as_pure_path.as_posix() != ".", f'{path_in_remote} should be local folder "."'

            logging.warning(
                f"Remote {remote.uuid} contains path {from_path_in_hoard}"
                f" as inner {path_in_remote}, which requires moving files.")
            return f"Can't move {from_path} to {to_path}, requires moving files in {remote.name}:{path_in_remote}.\n"

        if len(repos_to_move) == 0:
            return f"No repos to move!"

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            logging.info(f"Loaded hoard.")

            with StringIO() as out:
                out.write("Moving files and folders:\n")
                roots = hoard.env.roots(True)

                for remote in sorted(config.remotes.all(), key=lambda remote: remote.name):
                    rname = remote.name if remote else r.name
                    r = roots[remote.uuid]

                    r.current = move_paths(
                        hoard, FastPosixPath(from_path), FastPosixPath(to_path), r.current,
                        rname, "current", out)
                    r.staging = move_paths(
                        hoard, FastPosixPath(from_path), FastPosixPath(to_path), r.staging,
                        rname, "staging", out)
                    r.desired = move_paths(
                        hoard, FastPosixPath(from_path), FastPosixPath(to_path), r.desired,
                        rname, "desired", out)

                hoard_root = roots["HOARD"]
                hoard_root.desired = move_paths(
                    hoard, FastPosixPath(from_path), FastPosixPath(to_path), hoard_root.desired,
                    "HOARD", "desired", out, dump_changes=True)

                logging.info(f"Moving {', '.join(r.name for r in repos_to_move)}.")
                out.write(f"Moving {len(repos_to_move)} repos:\n")
                for remote in repos_to_move:
                    relative_repo_mounted_at = remote.mounted_at.relative_to(from_path)
                    logging.info(
                        f"[{remote.name} is mounted {relative_repo_mounted_at.as_posix()} rel. to {from_path}]")
                    final_mount_path = to_path_in_hoard.as_pure_path.joinpath(relative_repo_mounted_at)
                    logging.info(f"re-mounting it to {final_mount_path}")

                    out.write(f"[{remote.name}] {remote.mounted_at.as_posix()} => {final_mount_path.as_posix()}\n")
                    remote.mount_at(final_mount_path)

                logging.info("Writing config...")
                config.write()

                out.write("DONE")
                return out.getvalue()

    async def export_contents_to_repo(self, remote: str):
        remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)

        logging.info(f"Loading hoard TOML...")
        async with self.hoard.open_contents(create_missing=False) as hoard:
            logging.info(f"Removing old contents...")
            self.hoard.connect_to_repo(remote_uuid, require_contents=False).remove_contents()

            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())
            logging.info(f"Opening new contents of {remote_uuid}...")
            with self.hoard.connect_to_repo(remote_uuid, require_contents=False) \
                    .create_contents(remote_uuid) as current_contents:
                logging.info("Restoring config...")
                hoard.config.restore_remote_config(current_contents.config)

                with StringIO() as out:
                    self._export_contents_to_cave(hoard, current_contents, pathing, remote_uuid, out)

                    current_contents.config.end_updating()
                    out.write("DONE")
                    return out.getvalue()

    def _export_contents_to_cave(
            self, hoard: HoardContents, current_contents: RepoContents, pathing: HoardPathing, remote_uuid: str,
            out: TextIO) -> None:
        logging.info(f"Iterating over files marked available in {remote_uuid}...")
        hoard_file: FastPosixPath
        presence = Presence(hoard)
        for hoard_file, file_obj in alive_it(
                hoard.fsobjects.desired_in_repo(remote_uuid), title="Recreating index"):
            if not presence.is_current(remote_uuid, hoard_file, file_obj):
                continue

            local_path_obj = pathing.in_hoard(hoard_file).at_local(remote_uuid)
            assert local_path_obj is not None, \
                f"Path {hoard_file} needs to be available in local, but isn't???"
            assert isinstance(file_obj, FileObject)

            logging.info(
                f"Restoring description of file {hoard_file} to {local_path_obj}...")
            current_contents.fsobjects.add_file(
                local_path_obj.as_pure_path,
                FileObject.create(size=file_obj.size, fasthash=file_obj.fasthash))
            out.write(f"PRESENT {local_path_obj}\n")

    async def meld(
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
        async with self.hoard.open_contents(create_missing=False) as hoard:
            logging.info(f"Loaded hoard.")
            junk_path = pathlib.Path(dest).joinpath(junk_folder)
            if move:
                junk_path.mkdir()

            moves_and_copies = MovesAndCopies(hoard)
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

                            file_obj = FileObject.create(fasthash=fasthash, size=size, md5=None)
                            places: List[FastPosixPath] = list(
                                moves_and_copies.get_paths_in_hoard_expanded(file_obj.file_id))

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
                                    hoard_filepath = places.pop()

                                    # use + because hoard paths are absolute!
                                    end_place = pathlib.Path(dest + hoard_filepath.as_posix())
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
                                hoard_filepath = places.pop()

                                # use + because hoard paths are absolute!
                                end_place = pathlib.Path(dest + hoard_filepath.as_posix())
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


def move_paths(
        hoard: HoardContents, from_path: FastPosixPath, to_path: FastPosixPath, root_id: ObjectID | None, rname: str,
        rtype: str, out: TextIO, dump_changes: bool = False):
    with hoard.env.objects(write=True) as objects:
        # get and remove old subpath
        old_subpath_id = get_child(objects, from_path._rem, root_id)
        new_root_id = remove_child(objects, from_path._rem, root_id)

        # graft into the new subpath
        new_root_id = add_object(objects, new_root_id, to_path._rem, old_subpath_id)

        if new_root_id != root_id:
            out.write(f"{rname}.{rtype}: {safe_hex(root_id)[:6]} => {safe_hex(new_root_id)[:6]}\n")

        if dump_changes:
            for current_path, obj_type, obj_id, obj, skip_children in dfs(objects, "", old_subpath_id):
                if obj_type == ObjectType.BLOB:
                    current_path = FastPosixPath(current_path).relative_to("/")
                    out.write(
                        f"{from_path.joinpath(current_path).as_posix()}=>{to_path.joinpath(current_path).as_posix()}\n")

    return new_root_id


type Hashes = Dict[str, List[Tuple[str, FileObject, bytearray, MaybeObjectID]]] | None


class CachedObjectReader:
    def __init__(self, objects: Objects):
        self.objects = objects
        self.cache = dict()
        assert self.objects.txn is not None, "Use with an opened transaction only."

    def __getitem__(self, obj_id):
        if obj_id not in self.cache:
            self.cache[obj_id] = self.objects[obj_id]
        return self.cache[obj_id]


def read_all_current_hashes(hoard: HoardContents) -> Hashes:
    roots = hoard.env.roots(write=False)

    hashes: Hashes = dict()
    roots_per_uuid = dict((remote.uuid, roots[remote.uuid].current) for remote in hoard.hoard_config.remotes.all())

    with alive_bar(title="Reading all hashes") as bar:
        with hoard.env.objects(write=False) as objects:
            for uuid, root_id in roots_per_uuid.items():
                for path, obj_type, obj_id, obj, _ in fast_compressed_path_dfs(
                        CachedObjectReader(objects), bytearray(), root_id):
                    bar()
                    if obj_type == ObjectType.TREE:
                        continue
                    obj: FileObject
                    if obj.fasthash not in hashes:
                        hashes[obj.fasthash] = [(uuid, obj, path.copy(), root_id)]
                    else:
                        hashes[obj.fasthash].append((uuid, obj, path.copy(), root_id))
    return hashes
