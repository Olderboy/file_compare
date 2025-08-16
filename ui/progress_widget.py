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
        self.setObjectName("progress_widget")  # 设置对象名称
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(5)  # 减少组件间距
        layout.setContentsMargins(15, 20, 15, 15)  # 减少顶部边距
        
        # 状态标签
        self.status_label = QLabel("就绪")
        self.status_label.setObjectName("status_label")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setMinimumHeight(20)  # 减少高度
        layout.addWidget(self.status_label)
        
        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("进度: %p%")
        self.progress_bar.setMinimumHeight(20)  # 减少高度
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar)
        
        # 设置样式
        self.setup_styles()
        
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
        # This method is no longer needed as the detail label is removed.
        pass
        
    def reset(self):
        """重置进度显示"""
        self.progress_bar.setValue(0)
        self.status_label.setText("就绪")
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

    def setup_styles(self):
        """设置现代化样式"""
        style_sheet = """
        QGroupBox#progress_widget {
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
        
        QGroupBox#progress_widget::title {
            subcontrol-origin: margin;
            left: 15px;
            top: 15px;
            padding: 0 12px;
            color: #667eea;
            font-weight: 700;
            font-size: 11pt;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 8px;
            border: 2px solid rgba(102, 126, 234, 0.2);
        }
        
        QProgressBar {
            border: none;
            border-radius: 10px;
            text-align: center;
            background: #ecf0f1;
            color: #2c3e50;
            font-weight: 600;
            height: 18px;
            margin: 3px 0px;
            font-size: 9pt;
        }
        
        QProgressBar::chunk {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #667eea, stop:1 #764ba2);
            border-radius: 10px;
            margin: 2px;
            min-width: 20px;
        }
        
        QLabel {
            color: #2c3e50;
            font-weight: 500;
            font-size: 9pt;
            margin: 5px 0px;
        }
        
        QLabel#status_label {
            color: #667eea;
            font-weight: 600;
            font-size: 9pt;
            padding: 5px;
            background: rgba(102, 126, 234, 0.1);
            border-radius: 6px;
            border: 1px solid rgba(102, 126, 234, 0.2);
        }
        """
        
        self.setStyleSheet(style_sheet)
