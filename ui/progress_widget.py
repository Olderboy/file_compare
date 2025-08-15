#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
进度显示组件 - 包含进度条和状态显示
"""

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QProgressBar, QGroupBox)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont


class ProgressWidget(QGroupBox):
    """进度显示组件"""
    
    def __init__(self):
        super().__init__("进度状态")
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # 状态标签
        self.status_label = QLabel("就绪")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("""
            QLabel {
                color: #7f8c8d;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #ecf0f1;
                border-radius: 6px;
            }
        """)
        layout.addWidget(self.status_label)
        
        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("进度: %p%")
        self.progress_bar.setMinimumHeight(30)
        layout.addWidget(self.progress_bar)
        
        # 详细信息标签
        self.detail_label = QLabel("等待开始...")
        self.detail_label.setAlignment(Qt.AlignCenter)
        self.detail_label.setStyleSheet("color: #95a5a6; font-size: 12px;")
        layout.addWidget(self.detail_label)
        
    def set_progress(self, value):
        """设置进度值"""
        self.progress_bar.setValue(value)
        
    def set_status(self, status):
        """设置状态文本"""
        self.status_label.setText(status)
        
        # 根据状态设置不同的样式
        if "完成" in status:
            self.status_label.setStyleSheet("""
                QLabel {
                    color: #27ae60;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #d5f4e6;
                    border-radius: 6px;
                }
            """)
        elif "失败" in status or "错误" in status:
            self.status_label.setStyleSheet("""
                QLabel {
                    color: #e74c3c;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #fadbd8;
                    border-radius: 6px;
                }
            """)
        elif "正在" in status:
            self.status_label.setStyleSheet("""
                QLabel {
                    color: #f39c12;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #fdeaa7;
                    border-radius: 6px;
                }
            """)
        else:
            self.status_label.setStyleSheet("""
                QLabel {
                    color: #7f8c8d;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #ecf0f1;
                    border-radius: 6px;
                }
            """)
            
    def set_detail(self, detail):
        """设置详细信息"""
        self.detail_label.setText(detail)
        
    def reset(self):
        """重置进度显示"""
        self.progress_bar.setValue(0)
        self.status_label.setText("就绪")
        self.detail_label.setText("等待开始...")
        self.status_label.setStyleSheet("""
            QLabel {
                color: #7f8c8d;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #ecf0f1;
                border-radius: 6px;
            }
        """)
