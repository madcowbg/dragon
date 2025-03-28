class RepoOpeningFailed(Exception):
    pass


class MissingRepo(RepoOpeningFailed):
    pass


class WrongRepo(RepoOpeningFailed):
    pass


class MissingRepoContents(RepoOpeningFailed):
    pass
