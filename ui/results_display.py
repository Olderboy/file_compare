#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结果显示组件 - 用于显示比较结果和报告
"""

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTextEdit, 
                             QPushButton, QGroupBox, QLabel, QTabWidget)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont


class ResultsDisplayWidget(QWidget):
    """结果显示组件"""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # 创建标签页
        self.tab_widget = QTabWidget()
        
        # 摘要标签页
        self.summary_tab = self.create_summary_tab()
        self.tab_widget.addTab(self.summary_tab, "结果摘要")
        
        # 详细报告标签页
        self.report_tab = self.create_report_tab()
        self.tab_widget.addTab(self.report_tab, "详细报告")
        
        layout.addWidget(self.tab_widget)
        
        # 操作按钮
        button_layout = QHBoxLayout()
        
        self.save_report_button = QPushButton("保存报告")
        self.save_report_button.clicked.connect(self.save_report)
        self.save_report_button.setEnabled(False)
        
        # 添加报告路径显示标签
        self.report_path_label = QLabel("报告路径: 未设置")
        self.report_path_label.setObjectName("report_path")
        self.report_path_label.setWordWrap(True)
        
        self.clear_button = QPushButton("清空结果")
        self.clear_button.clicked.connect(self.clear_results)
        
        button_layout.addWidget(self.save_report_button)
        button_layout.addWidget(self.report_path_label)
        button_layout.addStretch()
        button_layout.addWidget(self.clear_button)
        
        layout.addLayout(button_layout)
        
        # 存储结果数据
        self.current_result = None
        self.report_output_path = None
        
        # 设置样式
        self.setup_styles()
        
    def setup_styles(self):
        """设置现代化样式"""
        style_sheet = """
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
        
        QTextEdit {
            border: 2px solid #e8e8e8;
            border-radius: 8px;
            background: white;
            padding: 8px;
            color: #2c3e50;
            selection-background-color: #667eea;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
        }
        
        QTextEdit:focus {
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #667eea, stop:1 #764ba2);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 12px 24px;
            font-weight: 600;
            font-size: 10pt;
            min-height: 20px;
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
        
    def create_summary_tab(self):
        """创建结果摘要标签页"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # 摘要信息显示
        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setFont(QFont("Consolas", 10))
        self.summary_text.setPlaceholderText("比较完成后将显示结果摘要...")
        
        layout.addWidget(self.summary_text)
        
        return widget
        
    def create_report_tab(self):
        """创建详细报告标签页"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # 报告内容显示
        self.report_text = QTextEdit()
        self.report_text.setReadOnly(True)
        self.report_text.setFont(QFont("Consolas", 9))
        self.report_text.setPlaceholderText("比较完成后将显示详细报告...")
        
        layout.addWidget(self.report_text)
        
        return widget
        
    def display_results(self, result, report_output_path=None):
        """显示比较结果"""
        self.current_result = result
        self.report_output_path = report_output_path
        
        # 显示结果摘要
        self.display_summary(result)
        
        # 显示详细报告
        self.display_report(result)
        
        # 更新报告路径显示
        if report_output_path and report_output_path.strip():
            self.report_path_label.setText(f"报告路径: {report_output_path}")
            self.report_path_label.setProperty("status", "set")
            self.report_path_label.style().unpolish(self.report_path_label)
            self.report_path_label.style().polish(self.report_path_label)
        else:
            self.report_path_label.setText("报告路径: 未设置")
            self.report_path_label.setProperty("status", "unset")
            self.report_path_label.style().unpolish(self.report_path_label)
            self.report_path_label.style().polish(self.report_path_label)
        
        # 启用按钮
        self.save_report_button.setEnabled(True)
        
    def display_summary(self, result):
        """显示结果摘要"""
        summary = []
        summary.append("=" * 60)
        summary.append("比较结果摘要")
        summary.append("=" * 60)
        
        # 基本信息
        summary.append(f"比较时间: {result.get('timestamp', '未知')}")
        summary.append(f"总耗时: {result.get('total_processing_time', 0):.2f}秒")
        summary.append(f"文件是否相同: {'是' if result.get('is_identical', False) else '否'}")
        
        # Hash对比信息
        hash_result = result.get('hash_result', {})
        if hash_result:
            summary.append("")
            summary.append("Hash对比结果:")
            summary.append(f"  文件1行数: {hash_result.get('file1_lines', 0):,}")
            summary.append(f"  文件2行数: {hash_result.get('file2_lines', 0):,}")
            summary.append(f"  差异数量: {hash_result.get('total_differences', 0):,}")
            
        self.summary_text.setPlainText("\n".join(summary))
        
    def display_report(self, result):
        """显示详细报告"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CSV文件混合对比详细报告")
        report_lines.append("=" * 80)
        
        # 基本信息
        report_lines.append(f"比较时间: {result.get('timestamp', '未知')}")
        report_lines.append(f"总耗时: {result.get('total_processing_time', 0):.2f}秒")
        
        # 关键列信息
        key_columns = result.get('key_columns', [])
        report_lines.append(f"关键列: {', '.join(key_columns) if key_columns else '未指定'}")
        
        # Hash对比结果
        hash_result = result.get('hash_result', {})
        if hash_result:
            report_lines.append("")
            report_lines.append("Hash对比结果:")
            report_lines.append(f"  文件1行数: {hash_result.get('file1_lines', 0):,}")
            report_lines.append(f"  文件2行数: {hash_result.get('file2_lines', 0):,}")
            report_lines.append(f"  差异数量: {hash_result.get('total_differences', 0):,}")
            
        self.report_text.setPlainText("\n".join(report_lines))
        
    def save_report(self):
        """保存报告"""
        if not self.current_result:
            return
            
        from PyQt5.QtWidgets import QFileDialog
        
        # 使用已设置的报告路径作为默认文件名
        default_name = "comparison_report.txt"
        if self.report_output_path and self.report_output_path.strip():
            # 如果已有输出路径，使用该路径作为默认位置
            default_path = self.report_output_path
        else:
            # 否则使用默认文件名
            default_path = default_name
        
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "保存报告",
            default_path,
            "文本文件 (*.txt);;所有文件 (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.report_text.toPlainText())
                from PyQt5.QtWidgets import QMessageBox
                QMessageBox.information(self, "保存成功", f"报告已保存到: {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "保存失败", f"保存报告时出错: {str(e)}")
                
    def clear_results(self):
        """清空结果"""
        self.current_result = None
        self.report_output_path = None
        self.summary_text.clear()
        self.report_text.clear()
        
        # 重置报告路径显示
        self.report_path_label.setText("报告路径: 未设置")
        self.report_path_label.setProperty("status", "unset")
        self.report_path_label.style().unpolish(self.report_path_label)
        self.report_path_label.style().polish(self.report_path_label)
        
        # 禁用按钮
        self.save_report_button.setEnabled(False)
