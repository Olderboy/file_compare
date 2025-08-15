#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比较设置组件 - 包含各种比较参数设置
"""

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
                             QLabel, QLineEdit, QPushButton, QGroupBox,
                             QSpinBox, QComboBox, QCheckBox)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont


class ComparisonSettingsWidget(QGroupBox):
    """比较设置组件"""
    
    def __init__(self):
        super().__init__("比较设置")
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # 创建网格布局
        grid_layout = QGridLayout()
        grid_layout.setSpacing(10)
        
        # 哈希算法设置
        hash_label = QLabel("哈希算法:")
        self.hash_combo = QComboBox()
        self.hash_combo.addItems(["md5", "sha1", "sha256"])
        self.hash_combo.setCurrentText("md5")
        grid_layout.addWidget(hash_label, 0, 0)
        grid_layout.addWidget(self.hash_combo, 0, 1)
        
        # 分块大小设置
        chunk_label = QLabel("分块大小:")
        self.chunk_spin = QSpinBox()
        self.chunk_spin.setRange(10000, 1000000)
        self.chunk_spin.setValue(500000)
        self.chunk_spin.setSuffix(" 行")
        self.chunk_spin.setSingleStep(50000)
        grid_layout.addWidget(chunk_label, 1, 0)
        grid_layout.addWidget(self.chunk_spin, 1, 1)
        
        # 最大对比行数设置
        max_rows_label = QLabel("最大对比行数:")
        self.max_rows_spin = QSpinBox()
        self.max_rows_spin.setRange(0, 10000000)
        self.max_rows_spin.setValue(0)
        self.max_rows_spin.setSuffix(" 行")
        self.max_rows_spin.setSpecialValueText("全部")
        self.max_rows_spin.setSingleStep(10000)
        grid_layout.addWidget(max_rows_label, 2, 0)
        grid_layout.addWidget(self.max_rows_spin, 2, 1)
        
        # 内存限制设置
        memory_label = QLabel("内存限制:")
        self.memory_combo = QComboBox()
        self.memory_combo.addItems(["1GB", "2GB", "4GB", "8GB", "16GB"])
        self.memory_combo.setCurrentText("2GB")
        grid_layout.addWidget(memory_label, 3, 0)
        grid_layout.addWidget(self.memory_combo, 3, 1)
        
        # Dask设置
        self.dask_checkbox = QCheckBox("使用Dask进行分布式处理")
        self.dask_checkbox.setToolTip("适用于超大文件（150万行以上）")
        grid_layout.addWidget(self.dask_checkbox, 4, 0, 1, 2)
        
        # 高级设置
        advanced_group = QGroupBox("高级设置")
        advanced_layout = QVBoxLayout(advanced_group)
        
        # 输出报告设置
        report_layout = QHBoxLayout()
        report_label = QLabel("输出报告:")
        self.report_edit = QLineEdit()
        self.report_edit.setPlaceholderText("选择报告保存路径（可选）")
        self.report_edit.setReadOnly(True)
        
        self.report_button = QPushButton("浏览...")
        self.report_button.clicked.connect(self.select_report_path)
        
        report_layout.addWidget(report_label)
        report_layout.addWidget(self.report_edit)
        report_layout.addWidget(self.report_button)
        
        advanced_layout.addLayout(report_layout)
        
        # 详细输出设置
        self.verbose_checkbox = QCheckBox("详细输出")
        self.verbose_checkbox.setToolTip("显示详细的比较过程信息")
        advanced_layout.addWidget(self.verbose_checkbox)
        
        # 添加所有布局
        layout.addLayout(grid_layout)
        layout.addWidget(advanced_group)
        
        # 添加弹性空间
        layout.addStretch()
        
    def select_report_path(self):
        """选择报告保存路径"""
        from PyQt5.QtWidgets import QFileDialog
        
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "选择报告保存路径",
            "",
            "文本文件 (*.txt);;所有文件 (*)"
        )
        
        if file_path:
            self.report_edit.setText(file_path)
            
    def get_settings(self):
        """获取比较设置"""
        settings = {
            'hash_algorithm': self.hash_combo.currentText(),
            'chunk_size': self.chunk_spin.value(),
            'max_compare_rows': self.max_rows_spin.value() if self.max_rows_spin.value() > 0 else None,
            'memory_limit': self.memory_combo.currentText(),
            'use_dask': self.dask_checkbox.isChecked(),
            'output_report': self.report_edit.text().strip() if self.report_edit.text().strip() else None,
            'verbose': self.verbose_checkbox.isChecked()
        }
        return settings
        
    def reset_to_defaults(self):
        """重置为默认设置"""
        self.hash_combo.setCurrentText("md5")
        self.chunk_spin.setValue(500000)
        self.max_rows_spin.setValue(0)
        self.memory_combo.setCurrentText("2GB")
        self.dask_checkbox.setChecked(False)
        self.report_edit.clear()
        self.verbose_checkbox.setChecked(False)
