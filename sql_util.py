from dataclasses import dataclass

from command.fast_path import FastPosixPath


def format_for_subfolder(folder_name: FastPosixPath):
    return folder_name.simple.rstrip("/")


@dataclass
class SubfolderFilter:
    _sql_field: str
    _param_value: FastPosixPath

    @property
    def where_clause(self): return f" ? || '/' < {self._sql_field} AND {self._sql_field} < ? || '0' "

    @property
    def params(self): return format_for_subfolder(self._param_value), format_for_subfolder(self._param_value)


class NoFilter:
    @property
    def where_clause(self): return " TRUE "

    @property
    def params(self): return []
