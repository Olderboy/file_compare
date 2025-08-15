#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主窗口类 - 包含主要的UI布局和逻辑
"""

import sys
import os
import threading
from pathlib import Path
from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QGridLayout, QLabel, QLineEdit, QPushButton, 
                             QTextEdit, QProgressBar, QGroupBox, QFileDialog,
                             QMessageBox, QSpinBox, QComboBox, QCheckBox,
                             QSplitter, QTabWidget, QFrame, QScrollArea)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QIcon, QPixmap, QPalette, QColor

from .file_selection import FileSelectionWidget
from .comparison_settings import ComparisonSettingsWidget
from .progress_widget import ProgressWidget
from .results_display import ResultsDisplayWidget
from .comparison_worker import ComparisonWorker


class MainWindow(QMainWindow):
    """主窗口类"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV文件混合对比工具")
        self.setGeometry(100, 100, 1200, 800)
        
        # 设置应用图标
        self.setWindowIcon(self.create_icon())
        
        # 初始化UI
        self.init_ui()
        self.setup_styles()
        
        # 比较器工作线程
        self.comparison_worker = None
        self.comparison_thread = None
        
        # 定时器用于更新进度
        self.progress_timer = QTimer()
        self.progress_timer.timeout.connect(self.update_progress)
        
    def init_ui(self):
        """初始化UI"""
        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # 创建标题
        title_label = QLabel("CSV文件混合对比工具")
        title_label.setAlignment(Qt.AlignCenter)
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #2c3e50; margin: 10px;")
        main_layout.addWidget(title_label)
        
        # 创建分割器
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)
        
        # 左侧控制面板
        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)
        
        # 右侧结果显示面板
        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)
        
        # 设置分割器比例
        splitter.setSizes([400, 800])
        
        # 底部状态栏
        self.statusBar().showMessage("就绪")
        
    def create_left_panel(self):
        """创建左侧控制面板"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setSpacing(15)
        
        # 文件选择区域
        self.file_selection = FileSelectionWidget()
        left_layout.addWidget(self.file_selection)
        
        # 比较设置区域
        self.comparison_settings = ComparisonSettingsWidget()
        left_layout.addWidget(self.comparison_settings)
        
        # 进度显示区域
        self.progress_widget = ProgressWidget()
        left_layout.addWidget(self.progress_widget)
        
        # 控制按钮区域
        control_group = QGroupBox("操作控制")
        control_layout = QVBoxLayout(control_group)
        
        # 开始比较按钮
        self.start_button = QPushButton("开始比较")
        self.start_button.setMinimumHeight(50)
        self.start_button.clicked.connect(self.start_comparison)
        control_layout.addWidget(self.start_button)
        
        # 停止比较按钮
        self.stop_button = QPushButton("停止比较")
        self.stop_button.setMinimumHeight(40)
        self.stop_button.clicked.connect(self.stop_comparison)
        self.stop_button.setEnabled(False)
        control_layout.addWidget(self.stop_button)
        
        # 清空结果按钮
        self.clear_button = QPushButton("清空结果")
        self.clear_button.setMinimumHeight(40)
        self.clear_button.clicked.connect(self.clear_results)
        control_layout.addWidget(self.clear_button)
        
        left_layout.addWidget(control_group)
        
        # 添加弹性空间
        left_layout.addStretch()
        
        return left_widget
        
    def create_right_panel(self):
        """创建右侧结果显示面板"""
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        
        # 创建标签页
        self.tab_widget = QTabWidget()
        
        # 实时日志标签页
        self.log_tab = QWidget()
        log_layout = QVBoxLayout(self.log_tab)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        log_layout.addWidget(self.log_text)
        self.tab_widget.addTab(self.log_tab, "实时日志")
        
        # 结果报告标签页
        self.results_tab = QWidget()
        results_layout = QVBoxLayout(self.results_tab)
        self.results_display = ResultsDisplayWidget()
        results_layout.addWidget(self.results_display)
        self.tab_widget.addTab(self.results_tab, "对比结果")
        
        right_layout.addWidget(self.tab_widget)
        
        return right_widget
        
    def setup_styles(self):
        """设置样式"""
        # 设置应用样式表
        style_sheet = """
        QMainWindow {
            background-color: #f5f6fa;
        }
        
        QGroupBox {
            font-weight: bold;
            border: 2px solid #dcdde1;
            border-radius: 8px;
            margin-top: 10px;
            padding-top: 10px;
            background-color: white;
        }
        
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px 0 5px;
            color: #2c3e50;
        }
        
        QPushButton {
            background-color: #3498db;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 8px 16px;
            font-weight: bold;
        }
        
        QPushButton:hover {
            background-color: #2980b9;
        }
        
        QPushButton:pressed {
            background-color: #21618c;
        }
        
        QPushButton:disabled {
            background-color: #bdc3c7;
            color: #7f8c8d;
        }
        
        QLineEdit, QSpinBox, QComboBox {
            border: 2px solid #dcdde1;
            border-radius: 6px;
            padding: 6px;
            background-color: white;
        }
        
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
            border-color: #3498db;
        }
        
        QTextEdit {
            border: 2px solid #dcdde1;
            border-radius: 6px;
            background-color: white;
        }
        
        QProgressBar {
            border: 2px solid #dcdde1;
            border-radius: 6px;
            text-align: center;
            background-color: #ecf0f1;
        }
        
        QProgressBar::chunk {
            background-color: #2ecc71;
            border-radius: 4px;
        }
        
        QTabWidget::pane {
            border: 2px solid #dcdde1;
            border-radius: 6px;
            background-color: white;
        }
        
        QTabBar::tab {
            background-color: #ecf0f1;
            border: 2px solid #dcdde1;
            border-bottom: none;
            border-radius: 6px 6px 0 0;
            padding: 8px 16px;
            margin-right: 2px;
        }
        
        QTabBar::tab:selected {
            background-color: white;
            border-bottom: 2px solid white;
        }
        """
        
        self.setStyleSheet(style_sheet)
        
    def create_icon(self):
        """创建应用图标"""
        # 这里可以设置一个简单的图标
        return QIcon()
        
    def start_comparison(self):
        """开始比较"""
        # 验证输入
        if not self.validate_inputs():
            return
            
        # 获取文件路径
        file1 = self.file_selection.get_file1_path()
        file2 = self.file_selection.get_file2_path()
        key_columns = self.file_selection.get_key_columns()
        
        # 获取比较设置
        settings = self.comparison_settings.get_settings()
        
        # 清空日志
        self.log_text.clear()
        self.log_text.append("开始比较...")
        
        # 更新UI状态
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.progress_widget.set_progress(0)
        self.progress_widget.set_status("正在比较...")
        
        # 创建比较工作线程
        self.comparison_worker = ComparisonWorker(
            file1, file2, key_columns, settings
        )
        self.comparison_thread = QThread()
        self.comparison_worker.moveToThread(self.comparison_thread)
        
        # 连接信号
        self.comparison_worker.progress_updated.connect(self.progress_widget.set_progress)
        self.comparison_worker.status_updated.connect(self.progress_widget.set_status)
        self.comparison_worker.log_updated.connect(self.append_log)
        self.comparison_worker.comparison_finished.connect(self.on_comparison_finished)
        self.comparison_worker.error_occurred.connect(self.on_error_occurred)
        
        # 启动线程
        self.comparison_thread.started.connect(self.comparison_worker.run)
        self.comparison_thread.start()
        
        # 启动进度更新定时器
        self.progress_timer.start(100)
        
    def stop_comparison(self):
        """停止比较"""
        if self.comparison_worker:
            self.comparison_worker.stop()
            self.progress_widget.set_status("正在停止...")
            
    def clear_results(self):
        """清空结果"""
        self.log_text.clear()
        self.results_display.clear()
        self.progress_widget.set_progress(0)
        self.progress_widget.set_status("就绪")
        self.statusBar().showMessage("结果已清空")
        
    def validate_inputs(self):
        """验证输入"""
        # 检查文件路径
        file1 = self.file_selection.get_file1_path()
        file2 = self.file_selection.get_file2_path()
        
        if not file1 or not file2:
            QMessageBox.warning(self, "输入错误", "请选择要比较的两个CSV文件")
            return False
            
        if not os.path.exists(file1):
            QMessageBox.warning(self, "文件错误", f"文件1不存在: {file1}")
            return False
            
        if not os.path.exists(file2):
            QMessageBox.warning(self, "文件错误", f"文件2不存在: {file2}")
            return False
            
        # 检查关键列
        key_columns = self.file_selection.get_key_columns()
        if not key_columns:
            QMessageBox.warning(self, "输入错误", "请输入关键列名")
            return False
            
        return True
        
    def append_log(self, message):
        """添加日志消息"""
        self.log_text.append(message)
        # 滚动到底部
        self.log_text.verticalScrollBar().setValue(
            self.log_text.verticalScrollBar().maximum()
        )
        
    def update_progress(self):
        """更新进度"""
        if self.comparison_worker:
            progress = self.comparison_worker.get_progress()
            self.progress_widget.set_progress(progress)
            
    def on_comparison_finished(self, result):
        """比较完成回调"""
        # 更新UI状态
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.progress_widget.set_progress(100)
        self.progress_widget.set_status("比较完成")
        
        # 显示结果
        self.results_display.display_results(result)
        
        # 切换到结果标签页
        self.tab_widget.setCurrentIndex(1)
        
        # 停止定时器
        self.progress_timer.stop()
        
        # 显示完成消息
        self.statusBar().showMessage("比较完成")
        
        # 清理线程
        if self.comparison_thread:
            self.comparison_thread.quit()
            self.comparison_thread.wait()
            self.comparison_thread = None
            self.comparison_worker = None
            
    def on_error_occurred(self, error_message):
        """错误处理回调"""
        QMessageBox.critical(self, "比较错误", f"比较过程中发生错误:\n{error_message}")
        
        # 更新UI状态
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.progress_widget.set_status("比较失败")
        
        # 停止定时器
        self.progress_timer.stop()
        
        # 清理线程
        if self.comparison_thread:
            self.comparison_thread.quit()
            self.comparison_thread.wait()
            self.comparison_thread = None
            self.comparison_worker = None
            
    def closeEvent(self, event):
        """窗口关闭事件"""
        if self.comparison_thread and self.comparison_thread.isRunning():
            self.comparison_worker.stop()
            self.comparison_thread.quit()
            self.comparison_thread.wait()
        event.accept()
