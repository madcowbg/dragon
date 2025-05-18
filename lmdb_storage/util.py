from lmdb_storage.tree_iteration import zip_dfs


def dump_tree_diffs(env, root, remote_current_id) -> str:
    with (env.objects(write=False) as objects):
        diffs = [
            f"{path}: {diff_type.value}"
            for path, diff_type, left_id, right_id, should_skip
            in zip_dfs(objects, "root", root.current, remote_current_id)]
        return "\n############ dumping diffs of trees ############\n" + \
            "\n".join(diffs) + \
            "\n############# end dumping tree ############\n\n"
