#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件混合对比工具 - UI主程序入口
"""

import sys
import os
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import Qt

# 添加父目录到路径以导入hybrid_compare
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .main_window import MainWindow


def main():
    """主函数"""
    # 创建应用
    app = QApplication(sys.argv)
    app.setApplicationName("CSV文件混合对比工具")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("CSV Compare Tool")
    
    # 设置应用样式
    app.setStyle('Fusion')
    
    try:
        # 创建主窗口
        window = MainWindow()
        window.show()
        
        # 运行应用
        sys.exit(app.exec_())
        
    except Exception as e:
        QMessageBox.critical(None, "启动错误", f"程序启动失败:\n{str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
