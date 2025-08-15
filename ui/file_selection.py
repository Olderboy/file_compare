#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件选择组件 - 包含文件路径选择和关键列设置
"""

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
                             QLabel, QLineEdit, QPushButton, QGroupBox,
                             QFileDialog, QMessageBox, QListWidget, QListWidgetItem)
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QFont


class FileSelectionWidget(QGroupBox):
    """文件选择组件"""
    
    def __init__(self):
        super().__init__("文件选择")
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # 文件1选择
        file1_group = QWidget()
        file1_layout = QHBoxLayout(file1_group)
        file1_layout.setContentsMargins(0, 0, 0, 0)
        
        file1_label = QLabel("文件1:")
        file1_label.setMinimumWidth(60)
        self.file1_edit = QLineEdit()
        self.file1_edit.setPlaceholderText("选择第一个CSV文件")
        self.file1_edit.setReadOnly(True)
        
        self.file1_button = QPushButton("浏览...")
        self.file1_button.clicked.connect(lambda: self.select_file(self.file1_edit))
        
        file1_layout.addWidget(file1_label)
        file1_layout.addWidget(self.file1_edit)
        file1_layout.addWidget(self.file1_button)
        
        layout.addWidget(file1_group)
        
        # 文件2选择
        file2_group = QWidget()
        file2_layout = QHBoxLayout(file2_group)
        file2_layout.setContentsMargins(0, 0, 0, 0)
        
        file2_label = QLabel("文件2:")
        file2_label.setMinimumWidth(60)
        self.file2_edit = QLineEdit()
        self.file2_edit.setPlaceholderText("选择第二个CSV文件")
        self.file2_edit.setReadOnly(True)
        
        self.file2_button = QPushButton("浏览...")
        self.file2_button.clicked.connect(lambda: self.select_file(self.file2_edit))
        
        file2_layout.addWidget(file2_label)
        file2_layout.addWidget(self.file2_edit)
        file2_layout.addWidget(self.file2_button)
        
        layout.addWidget(file2_group)
        
        # 关键列设置
        key_columns_group = QWidget()
        key_columns_layout = QVBoxLayout(key_columns_group)
        key_columns_layout.setContentsMargins(0, 0, 0, 0)
        
        key_columns_label = QLabel("关键列 (用逗号分隔):")
        self.key_columns_edit = QLineEdit()
        self.key_columns_edit.setPlaceholderText("例如: id,name")
        
        key_columns_layout.addWidget(key_columns_label)
        key_columns_layout.addWidget(self.key_columns_edit)
        
        layout.addWidget(key_columns_group)
        
        # 文件信息显示
        self.file_info_label = QLabel("请选择CSV文件")
        self.file_info_label.setStyleSheet("color: #7f8c8d; font-style: italic;")
        self.file_info_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.file_info_label)
        
    def select_file(self, line_edit):
        """选择文件"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择CSV文件",
            "",
            "CSV文件 (*.csv);;所有文件 (*)"
        )
        
        if file_path:
            line_edit.setText(file_path)
            self.update_file_info()
            
    def update_file_info(self):
        """更新文件信息显示"""
        file1 = self.file1_edit.text()
        file2 = self.file2_edit.text()
        
        if file1 and file2:
            try:
                import pandas as pd
                df1 = pd.read_csv(file1, nrows=0)  # 只读取列名
                df2 = pd.read_csv(file2, nrows=0)
                
                info_text = f"文件1列数: {len(df1.columns)}, 文件2列数: {len(df2.columns)}"
                self.file_info_label.setText(info_text)
                self.file_info_label.setStyleSheet("color: #27ae60; font-weight: bold;")
                
            except Exception as e:
                self.file_info_label.setText(f"文件读取错误: {str(e)}")
                self.file_info_label.setStyleSheet("color: #e74c3c; font-weight: bold;")
        else:
            self.file_info_label.setText("请选择CSV文件")
            self.file_info_label.setStyleSheet("color: #7f8c8d; font-style: italic;")
            
    def get_file1_path(self):
        """获取文件1路径"""
        return self.file1_edit.text().strip()
        
    def get_file2_path(self):
        """获取文件2路径"""
        return self.file2_edit.text().strip()
        
    def get_key_columns(self):
        """获取关键列列表"""
        key_columns_text = self.key_columns_edit.text().strip()
        if not key_columns_text:
            return []
        
        # 分割并清理关键列
        key_columns = [col.strip() for col in key_columns_text.split(',') if col.strip()]
        return key_columns
