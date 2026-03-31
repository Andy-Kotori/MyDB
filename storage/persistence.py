"""
persistence.py - 简单持久化实现

提供 Database 的保存和加载功能：
- 格式: pickle（默认）或 json
- 策略: 全量序列化（整个 Database 对象一次性保存/加载）
- 适用场景: 小型数据库，学习演示

注意: 这是简化实现，不考虑:
- 并发控制
- 增量更新
- 崩溃恢复
- 大文件分块

这些高级特性由角色C后续实现（二进制分页存储）
"""

import pickle
import json
import os
from pathlib import Path
from typing import Union, Optional

from .database import Database


class Persistence:
    """
    持久化管理器
    
    使用方式:
        db = Database()
        # ... 操作数据库 ...
        
        # 保存
        Persistence.save(db, 'mydb.db')
        
        # 加载
        db = Persistence.load('mydb.db')
    
    格式说明:
        - pickle: Python 专用，性能好，支持所有 Python 类型
        - json: 通用格式，可读，但只支持基本类型（int/float/str/list/dict）
    """
    
    DEFAULT_FORMAT = 'pickle'
    
    @staticmethod
    def save(db: Database, filepath: Union[str, Path], fmt: Optional[str] = None) -> None:
        """
        保存数据库到文件
        
        Args:
            db: Database 对象
            filepath: 文件路径
            fmt: 格式，'pickle' 或 'json'，默认从文件扩展名推断
        
        Raises:
            ValueError: 格式不支持
            IOError: 文件写入失败
        """
        filepath = Path(filepath)
        
        # 自动推断格式
        if fmt is None:
            if filepath.suffix == '.json':
                fmt = 'json'
            else:
                fmt = Persistence.DEFAULT_FORMAT
        
        # 确保目录存在
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # 序列化
        data = db.to_dict()
        
        if fmt == 'pickle':
            with open(filepath, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        elif fmt == 'json':
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            raise ValueError(f"不支持的格式: {fmt}，请使用 'pickle' 或 'json'")
    
    @staticmethod
    def load(filepath: Union[str, Path], fmt: Optional[str] = None) -> Database:
        """
        从文件加载数据库
        
        Args:
            filepath: 文件路径
            fmt: 格式，'pickle' 或 'json'，默认从文件扩展名推断
        
        Returns:
            Database 对象
        
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 格式不支持或文件损坏
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"数据库文件不存在: {filepath}")
        
        # 自动推断格式
        if fmt is None:
            if filepath.suffix == '.json':
                fmt = 'json'
            else:
                fmt = Persistence.DEFAULT_FORMAT
        
        # 反序列化
        if fmt == 'pickle':
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
        elif fmt == 'json':
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            raise ValueError(f"不支持的格式: {fmt}")
        
        return Database.from_dict(data)
    
    @staticmethod
    def exists(filepath: Union[str, Path]) -> bool:
        """
        检查数据库文件是否存在
        """
        return Path(filepath).exists()
    
    @staticmethod
    def delete(filepath: Union[str, Path]) -> bool:
        """
        删除数据库文件
        
        Returns:
            是否成功删除
        """
        filepath = Path(filepath)
        if filepath.exists():
            filepath.unlink()
            return True
        return False
