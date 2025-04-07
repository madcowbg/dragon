import sqlite3
from dataclasses import dataclass

from command.fast_path import FastPosixPath


def sqlite3_standard(*args, **kwargs) -> sqlite3.Connection:
    """ The standard way to open a connection, with sane defaults:
     - enabled foreign keys
     - enabled recursive triggers
     """
    conn = sqlite3.connect(*args, **kwargs)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA recursive_triggers=ON;")
    return conn


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
