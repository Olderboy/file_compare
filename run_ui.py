#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件混合对比工具 - UI启动脚本
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入并运行UI
from ui.main import main

if __name__ == "__main__":
    main()
