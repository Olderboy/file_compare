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
        self.report_edit.setMinimumWidth(180)
        
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
        
        # 设置样式
        self.setup_styles()
        
    def setup_styles(self):
        """设置现代化样式"""
        style_sheet = """
        QGroupBox {
            font-weight: 600;
            font-size: 10pt;
            border: none;
            border-radius: 12px;
            margin-top: 8px;
            padding: 15px;
            padding-top: 30px;
            background: rgba(255, 255, 255, 0.95);
            color: #2c3e50;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        }
        
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 20px;
            top: 12px;
            padding: 0 15px;
            color: #667eea;
            font-weight: 700;
            font-size: 12pt;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 8px;
            border: 2px solid rgba(102, 126, 234, 0.2);
        }
        
        QLabel {
            color: #2c3e50;
            font-weight: 500;
            font-size: 9pt;
            margin: 5px 0px;
        }
        
        QLineEdit, QSpinBox, QComboBox {
            border: 2px solid #e8e8e8;
            border-radius: 8px;
            padding: 10px 12px;
            background: white;
            color: #2c3e50;
            font-size: 9pt;
            selection-background-color: #667eea;
        }
        
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        QLineEdit:hover, QSpinBox:hover, QComboBox:hover {
            border-color: #bdc3c7;
        }
        
        QComboBox::drop-down {
            border: none;
            width: 20px;
        }
        
        QComboBox::down-arrow {
            image: none;
            border-left: 5px solid transparent;
            border-right: 5px solid transparent;
            border-top: 5px solid #667eea;
            margin-right: 8px;
        }
        
        QComboBox QAbstractItemView {
            border: 2px solid #e8e8e8;
            border-radius: 8px;
            background: white;
            selection-background-color: #667eea;
            selection-color: white;
        }
        
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #667eea, stop:1 #764ba2);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 10px 20px;
            font-weight: 600;
            font-size: 9pt;
            min-height: 18px;
        }
        
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #5a6fd8, stop:1 #6a4190);
        }
        
        QPushButton:pressed {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4a5fc8, stop:1 #5a3190);
        }
        
        QCheckBox {
            spacing: 8px;
            color: #2c3e50;
            font-weight: 500;
        }
        
        QCheckBox::indicator {
            width: 18px;
            height: 18px;
            border: 2px solid #bdc3c7;
            border-radius: 4px;
            background: white;
        }
        
        QCheckBox::indicator:checked {
            background: #667eea;
            border-color: #667eea;
            image: url(data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIiIGhlaWdodD0iOSIgdmlld0JveD0iMCAwIDEyIDkiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+CjxwYXRoIGQ9Ik0xIDQuNUw0LjUgOEwxMSAxIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgo8L3N2Zz4K);
        }
        
        QCheckBox::indicator:hover {
            border-color: #667eea;
        }
        """
        
        self.setStyleSheet(style_sheet)
        
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
