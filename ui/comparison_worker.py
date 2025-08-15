#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比较工作线程 - 在后台执行文件比较操作
"""

import time
import sys
import os
from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtWidgets import QApplication

# 添加父目录到路径以导入hybrid_compare
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hybrid_compare import HybridComparator


class ComparisonWorker(QObject):
    """比较工作线程"""
    
    # 信号定义
    progress_updated = pyqtSignal(int)  # 进度更新
    status_updated = pyqtSignal(str)    # 状态更新
    log_updated = pyqtSignal(str)      # 日志更新
    comparison_finished = pyqtSignal(dict)  # 比较完成
    error_occurred = pyqtSignal(str)    # 错误发生
    
    def __init__(self, file1, file2, key_columns, settings):
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.key_columns = key_columns
        self.settings = settings
        self.is_running = False
        self.progress = 0
        
    def run(self):
        """执行比较操作"""
        try:
            self.is_running = True
            self.progress = 0
            self.status_updated.emit("正在初始化...")
            self.log_updated.emit("开始文件比较...")
            
            # 创建比较器
            comparator = HybridComparator(
                chunk_size=self.settings.get('chunk_size', 500000),
                hash_algorithm=self.settings.get('hash_algorithm', 'md5'),
                use_dask=self.settings.get('use_dask', False),
                memory_limit=self.settings.get('memory_limit', '2GB'),
                max_compare_rows=self.settings.get('max_compare_rows')
            )
            
            self.progress = 10
            self.progress_updated.emit(self.progress)
            self.status_updated.emit("正在执行Hash对比...")
            self.log_updated.emit("开始Hash对比...")
            
            # 执行比较
            result = comparator.compare_files_hybrid(
                self.file1, 
                self.file2, 
                self.key_columns
            )
            
            self.progress = 100
            self.progress_updated.emit(self.progress)
            self.status_updated.emit("比较完成")
            self.log_updated.emit("文件比较完成！")
            
            # 添加时间戳
            result['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # 发送完成信号
            self.comparison_finished.emit(result)
            
        except Exception as e:
            self.error_occurred.emit(str(e))
            self.log_updated.emit(f"比较过程中发生错误: {str(e)}")
            
        finally:
            self.is_running = False
            
    def stop(self):
        """停止比较操作"""
        self.is_running = False
        self.status_updated.emit("正在停止...")
        
    def get_progress(self):
        """获取当前进度"""
        return self.progress
