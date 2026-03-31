"""
Storage 模块 - 内存存储引擎（角色B）

提供 Database、Table 的内存数据结构和基础操作，
支持简单持久化（pickle/json），并预留与索引模块（角色C）的对接接口。
"""

from .database import Database, Table, Row, SchemaMode
from .persistence import Persistence

__all__ = ['Database', 'Table', 'Row', 'SchemaMode', 'Persistence']
