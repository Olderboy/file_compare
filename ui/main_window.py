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
from PyQt5.QtGui import QFont, QIcon, QPixmap, QPalette, QColor, QPainter, QLinearGradient, QPen, QLinearGradient

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
        self.setGeometry(100, 100, 1150, 650)
        
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
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 10, 15, 10)
        
        # 创建标题
        title_label = QLabel("CSV文件混合对比工具")
        title_label.setObjectName("title")
        title_label.setAlignment(Qt.AlignCenter)
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setWeight(QFont.Light)
        title_label.setFont(title_font)
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
        splitter.setSizes([380, 720])
        
        # 底部状态栏
        self.statusBar().showMessage("就绪")
        
    def create_left_panel(self):
        """创建左侧控制面板"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setSpacing(6)  # 减少面板间距
        
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
        self.start_button.setObjectName("start_button")
        self.start_button.setMinimumHeight(40)
        self.start_button.clicked.connect(self.start_comparison)
        control_layout.addWidget(self.start_button)
        
        # 停止比较按钮
        self.stop_button = QPushButton("停止比较")
        self.stop_button.setObjectName("stop_button")
        self.stop_button.setMinimumHeight(35)
        self.stop_button.clicked.connect(self.stop_comparison)
        self.stop_button.setEnabled(False)
        control_layout.addWidget(self.stop_button)
        
        # 清空结果按钮
        self.clear_button = QPushButton("清空结果")
        self.clear_button.setMinimumHeight(35)
        self.clear_button.clicked.connect(self.clear_results)
        control_layout.addWidget(self.clear_button)
        
        left_layout.addWidget(control_group)
        
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
        """设置现代化样式"""
        # 设置应用样式表 - Material Design风格
        style_sheet = """
        /* 全局样式 */
        QMainWindow {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #667eea, stop:1 #764ba2);
            color: #2c3e50;
        }
        
        QWidget {
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
            font-size: 9pt;
        }
        
        /* 标题样式 */
        QLabel#title {
            color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #ffffff, stop:0.5 #f0f8ff, stop:1 #ffffff);
            font-size: 26pt;
            font-weight: 300;
            padding: 15px;
            background: rgba(255, 255, 255, 0.15);
            border-radius: 18px;
            margin: 8px;
            text-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
            letter-spacing: 2px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            backdrop-filter: blur(10px);
        }
        
        /* 分组框样式 - 现代化卡片设计 */
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
            left: 15px;
            top: 10px;
            padding: 0 12px;
            color: #667eea;
            font-weight: 700;
            font-size: 11pt;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 8px;
            border: 2px solid rgba(102, 126, 234, 0.2);
        }
        
        /* 按钮样式 - Material Design风格 */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #667eea, stop:1 #764ba2);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 8px 20px;
            font-weight: 600;
            font-size: 10pt;
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
        
        QPushButton:disabled {
            background: #bdc3c7;
            color: #7f8c8d;
        }
        
        /* 开始比较按钮特殊样式 */
        QPushButton#start_button {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2ecc71, stop:1 #27ae60);
            font-size: 11pt;
            padding: 12px 28px;
        }
        
        QPushButton#start_button:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #27ae60, stop:1 #229954);
        }
        
        /* 停止按钮样式 */
        QPushButton#stop_button {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #e74c3c, stop:1 #c0392b);
        }
        
        QPushButton#stop_button:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #c0392b, stop:1 #a93226);
        }
        
        /* 输入控件样式 */
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
        }
        
        QLineEdit:hover, QSpinBox:hover, QComboBox:hover {
            border-color: #bdc3c7;
        }
        
        /* 下拉框样式 */
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
        
        /* 文本编辑框样式 */
        QTextEdit {
            border: 2px solid #e8e8e8;
            border-radius: 8px;
            background: white;
            padding: 8px;
            color: #2c3e50;
            selection-background-color: #667eea;
        }
        
        QTextEdit:focus {
            border-color: #667eea;
        }
        
        /* 进度条样式 */
        QProgressBar {
            border: none;
            border-radius: 10px;
            text-align: center;
            background: #ecf0f1;
            color: #2c3e50;
            font-weight: 600;
            height: 20px;
        }
        
        QProgressBar::chunk {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #667eea, stop:1 #764ba2);
            border-radius: 10px;
            margin: 2px;
        }
        
        /* 标签页样式 */
        QTabWidget::pane {
            border: none;
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.95);
            margin-top: 5px;
        }
        
        QTabBar::tab {
            background: rgba(255, 255, 255, 0.7);
            border: none;
            border-radius: 8px 8px 0 0;
            padding: 12px 20px;
            margin-right: 3px;
            color: #7f8c8d;
            font-weight: 600;
            min-width: 100px;
        }
        
        QTabBar::tab:selected {
            background: rgba(255, 255, 255, 0.95);
            color: #667eea;
            border-bottom: 3px solid #667eea;
        }
        
        QTabBar::tab:hover:!selected {
            background: rgba(255, 255, 255, 0.8);
            color: #5a6fd8;
        }
        
        /* 分割器样式 */
        QSplitter::handle {
            background: rgba(102, 126, 234, 0.3);
            border-radius: 2px;
        }
        
        QSplitter::handle:hover {
            background: rgba(102, 126, 234, 0.5);
        }
        
        /* 滚动条样式 */
        QScrollBar:vertical {
            background: #f8f9fa;
            width: 12px;
            border-radius: 6px;
            margin: 0px;
        }
        
        QScrollBar::handle:vertical {
            background: #c1c1c1;
            border-radius: 6px;
            min-height: 20px;
        }
        
        QScrollBar::handle:vertical:hover {
            background: #a8a8a8;
        }
        
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0px;
        }
        
        /* 复选框样式 */
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
        
        /* 状态栏样式 */
        QStatusBar {
            background: rgba(255, 255, 255, 0.9);
            color: #2c3e50;
            border-top: 1px solid rgba(102, 126, 234, 0.2);
        }
        
        /* 工具提示样式 */
        QToolTip {
            background: #2c3e50;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 8px 12px;
            font-size: 9pt;
        }
        
        /* 特殊标签样式 */
        QLabel#report_path {
            color: #7f8c8d;
            font-size: 9pt;
            padding: 8px;
            background: rgba(236, 240, 241, 0.8);
            border-radius: 6px;
            border: 1px solid #e8e8e8;
        }
        
        QLabel#report_path[status="set"] {
            color: #27ae60;
            background: rgba(46, 204, 113, 0.1);
            border-color: #27ae60;
        }
        """
        
        self.setStyleSheet(style_sheet)
        
    def create_icon(self):
        """创建现代化应用图标"""
        # 创建一个简单的现代化图标
        icon = QIcon()
        
        # 这里可以设置一个SVG图标或者使用系统图标
        # 为了演示，我们使用一个简单的颜色块作为图标
        pixmap = QPixmap(32, 32)
        pixmap.fill(Qt.transparent)
        
        # 绘制一个现代化的图标
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # 绘制渐变背景
        gradient = QLinearGradient(0, 0, 32, 32)
        gradient.setColorAt(0, QColor(102, 126, 234))  # #667eea
        gradient.setColorAt(1, QColor(118, 75, 162))   # #764ba2
        
        painter.setBrush(gradient)
        painter.setPen(Qt.NoPen)
        painter.drawRoundedRect(2, 2, 28, 28, 6, 6)
        
        # 绘制CSV图标
        painter.setPen(QPen(Qt.white, 2))
        painter.setFont(QFont("Arial", 12, QFont.Bold))
        painter.drawText(pixmap.rect(), Qt.AlignCenter, "CSV")
        
        painter.end()
        
        icon.addPixmap(pixmap)
        return icon
        
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
        self.results_display.clear_results()
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
        
        # 获取报告输出路径
        report_output_path = self.comparison_settings.get_settings().get('output_report')
        
        # 显示结果
        self.results_display.display_results(result, report_output_path)
        
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
