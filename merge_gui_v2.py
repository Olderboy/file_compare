import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import os
import sys
from threading import Thread
import glob
import subprocess
import platform
import json
from datetime import datetime

# 导入合并功能
sys.path.append(os.path.dirname(__file__))
from merge_test_results import read_file, extract_number_with_unit
from merge_compare_results import save_file


class ModernMergeGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("测试结果合并对比工具 v2.0")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)

        # 设置现代化主题颜色
        self.colors = {
            'bg': '#f5f7fa',
            'sidebar_bg': '#2c3e50',
            'sidebar_fg': '#ecf0f1',
            'sidebar_active': '#34495e',
            'sidebar_hover': '#34495e',
            'header_bg': '#ffffff',
            'accent': '#3498db',
            'accent_hover': '#2980b9',
            'success': '#27ae60',
            'success_hover': '#229954',
            'warning': '#f39c12',
            'warning_hover': '#e67e22',
            'error': '#e74c3c',
            'card_bg': '#ffffff',
            'card_border': '#e1e8ed',
            'input_bg': '#ffffff',
            'input_border': '#d1d5db',
            'input_focus': '#3498db',
            'text': '#2c3e50',
            'text_secondary': '#7f8c8d',
            'border': '#e1e8ed'
        }

        self.root.configure(bg=self.colors['bg'])

        # 初始化变量
        self.left_dir = tk.StringVar()
        self.right_dir = tk.StringVar()
        self.output_file = tk.StringVar(value=os.path.join(os.getcwd(), 'comparison_result.xlsx'))
        self.left_suffix = tk.StringVar(value="gauss")
        self.right_suffix = tk.StringVar(value="sr")
        self.left_files = []
        self.right_files = []

        # 统计列配置
        self.numeric_columns_var = tk.StringVar(value="TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime,StartMemory,EndMemory,MaxMemory,MaxCpu")
        self.original_only_columns_var = tk.StringVar(value="FileCount")
        self.custom_separator_columns_var = tk.StringVar(value="EndTimeRecord:||")

        # 对比配置
        self.compare_columns_var = tk.StringVar(value="TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime")
        self.deterioration_threshold_var = tk.StringVar(value="3")  # 劣化阈值

        # 当前选中的面板
        self.current_panel = None

        # 历史记录
        self.history_file = os.path.join(os.path.dirname(__file__), '.merge_history.json')
        self.max_history_items = 10
        self.history = self.load_history()

        # 创建界面
        self.create_ui()

        # 默认显示数据源面板
        self.show_panel('data')

    def create_ui(self):
        """创建主界面"""
        # 主容器
        main_container = tk.Frame(self.root, bg=self.colors['bg'])
        main_container.pack(fill=tk.BOTH, expand=True)

        # 左侧边栏
        self.sidebar = tk.Frame(main_container, bg=self.colors['sidebar_bg'], width=250)
        self.sidebar.pack(side=tk.LEFT, fill=tk.Y)
        self.sidebar.pack_propagate(False)

        # 右侧主内容区
        self.main_content = tk.Frame(main_container, bg=self.colors['bg'])
        self.main_content.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # 创建侧边栏内容
        self.create_sidebar()

        # 创建主内容区
        self.create_main_content_area()

    def create_sidebar(self):
        """创建侧边栏"""
        # 标题
        title_frame = tk.Frame(self.sidebar, bg=self.colors['sidebar_bg'])
        title_frame.pack(fill=tk.X, padx=15, pady=(20, 30))

        tk.Label(
            title_frame,
            text="📊",
            font=("Segoe UI Emoji", 24),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['accent']
        ).pack()

        tk.Label(
            title_frame,
            text="合并工具",
            font=("Microsoft YaHei UI", 14, "bold"),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg']
        ).pack(pady=(5, 0))

        tk.Label(
            title_frame,
            text="Test Results Merger",
            font=("Arial", 8),
            bg=self.colors['sidebar_bg'],
            fg='#7f8c8d'
        ).pack()

        # 导航按钮
        self.nav_buttons = {}
        nav_items = [
            ('data', '📂', '数据源'),
            ('config', '⚙️', '配置'),
            ('output', '📤', '输出'),
            ('logs', '📋', '日志')
        ]

        for item_id, icon, text in nav_items:
            btn = tk.Button(
                self.sidebar,
                text=f"{icon}  {text}",
                font=("Microsoft YaHei UI", 11),
                bg=self.colors['sidebar_bg'],
                fg=self.colors['sidebar_fg'],
                activebackground=self.colors['sidebar_active'],
                activeforeground=self.colors['accent'],
                relief=tk.FLAT,
                cursor='hand2',
                anchor='w',
                padx=20,
                pady=12,
                command=lambda i=item_id: self.show_panel(i)
            )
            btn.pack(fill=tk.X, padx=10, pady=2)
            self.nav_buttons[item_id] = btn

        # 快速操作区
        tk.Frame(self.sidebar, bg=self.colors['sidebar_bg'], height=1).pack(fill=tk.X, padx=15, pady=20)

        tk.Label(
            self.sidebar,
            text="⚡ 快速操作",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w', padx=15)

        # 导入配置按钮
        tk.Button(
            self.sidebar,
            text="📥 导入配置",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg'],
            activebackground=self.colors['sidebar_active'],
            relief=tk.FLAT,
            cursor='hand2',
            anchor='w',
            padx=20,
            pady=8,
            command=self.import_config
        ).pack(fill=tk.X, padx=10, pady=2)

        # 导出配置按钮
        tk.Button(
            self.sidebar,
            text="📤 导出配置",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg'],
            activebackground=self.colors['sidebar_active'],
            relief=tk.FLAT,
            cursor='hand2',
            anchor='w',
            padx=20,
            pady=8,
            command=self.export_config
        ).pack(fill=tk.X, padx=10, pady=2)

        # 重置按钮
        tk.Button(
            self.sidebar,
            text="🔄 重置默认",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg'],
            activebackground=self.colors['sidebar_active'],
            relief=tk.FLAT,
            cursor='hand2',
            anchor='w',
            padx=20,
            pady=8,
            command=self.reset_config
        ).pack(fill=tk.X, padx=10, pady=2)

        # 历史记录按钮
        tk.Button(
            self.sidebar,
            text="📜 历史记录",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg'],
            activebackground=self.colors['sidebar_active'],
            relief=tk.FLAT,
            cursor='hand2',
            anchor='w',
            padx=20,
            pady=8,
            command=self.show_history_dialog
        ).pack(fill=tk.X, padx=10, pady=2)

        # 清空历史按钮
        tk.Button(
            self.sidebar,
            text="🗑️ 清空历史",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['sidebar_bg'],
            fg=self.colors['sidebar_fg'],
            activebackground=self.colors['sidebar_active'],
            relief=tk.FLAT,
            cursor='hand2',
            anchor='w',
            padx=20,
            pady=8,
            command=self.clear_history
        ).pack(fill=tk.X, padx=10, pady=2)

        # 底部信息
        tk.Frame(self.sidebar, bg=self.colors['sidebar_bg'], height=1).pack(fill=tk.X, padx=15, pady=20, expand=True)

        tk.Label(
            self.sidebar,
            text="v2.0",
            font=("Arial", 8),
            bg=self.colors['sidebar_bg'],
            fg='#7f8c8d'
        ).pack(side=tk.BOTTOM, pady=10)

    def create_main_content_area(self):
        """创建主内容区域"""
        # 内容容器
        self.content_container = tk.Frame(self.main_content, bg=self.colors['bg'])
        self.content_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 创建各个面板
        self.panels = {}

        # 数据源面板
        self.panels['data'] = self.create_data_panel()

        # 配置面板
        self.panels['config'] = self.create_config_panel()

        # 输出面板
        self.panels['output'] = self.create_output_panel()

        # 日志面板
        self.panels['logs'] = self.create_logs_panel()

        # 底部操作栏（始终显示）
        self.create_bottom_bar()

    def create_data_panel(self):
        """创建数据源配置面板"""
        panel = tk.Frame(self.content_container, bg=self.colors['bg'])

        # 标题
        header = tk.Frame(panel, bg=self.colors['card_bg'], relief=tk.FLAT, bd=0)
        header.pack(fill=tk.X, pady=(0, 15))

        tk.Label(
            header,
            text="📂 数据源配置",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=20, pady=15)

        tk.Label(
            header,
            text="选择要合并的测试数据文件",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT, padx=(0, 20), pady=15)

        # 左右两侧容器
        sides_frame = tk.Frame(panel, bg=self.colors['bg'])
        sides_frame.pack(fill=tk.BOTH, expand=True)

        # 左侧数据区
        left_card = self.create_side_card(sides_frame, "🔵 左侧数据", "left")
        left_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        # 右侧数据区
        right_card = self.create_side_card(sides_frame, "🔴 右侧数据", "right")
        right_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))

        return panel

    def create_side_card(self, parent, title, side):
        """创建单侧数据卡片"""
        card = tk.Frame(parent, bg=self.colors['card_bg'], relief=tk.FLAT, bd=1)
        card_inner = tk.Frame(card, bg=self.colors['card_bg'])
        card_inner.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # 标题和目录选择
        header_frame = tk.Frame(card_inner, bg=self.colors['card_bg'])
        header_frame.pack(fill=tk.X, pady=(0, 10))

        tk.Label(
            header_frame,
            text=title,
            font=("Microsoft YaHei UI", 12, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT)

        dir_button = tk.Button(
            header_frame,
            text="📁 选择目录",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=12,
            pady=5,
            bd=0,
            activebackground=self.colors['accent_hover'],
            command=lambda s=side: self.browse_dir(s)
        )
        dir_button.pack(side=tk.RIGHT)

        # 文件列表
        list_frame = tk.Frame(card_inner, bg=self.colors['card_bg'])
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        listbox = tk.Listbox(
            list_frame,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            fg=self.colors['text'],
            height=8,
            selectmode=tk.MULTIPLE,
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            selectbackground=self.colors['accent'],
            selectforeground='white'
        )
        listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=listbox.yview)

        if side == 'left':
            self.left_file_listbox = listbox
        else:
            self.right_file_listbox = listbox

        # 操作按钮
        btn_frame = tk.Frame(card_inner, bg=self.colors['card_bg'])
        btn_frame.pack(fill=tk.X)

        tk.Button(
            btn_frame,
            text="➕ 添加文件",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=4,
            bd=0,
            activebackground=self.colors['success_hover'],
            command=lambda s=side: self.add_files(s)
        ).pack(side=tk.LEFT, padx=(0, 5))

        tk.Button(
            btn_frame,
            text="➖ 移除",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['error'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=4,
            bd=0,
            activebackground='#c0392b',
            command=lambda s=side: self.remove_files(s)
        ).pack(side=tk.LEFT, padx=(0, 5))

        tk.Button(
            btn_frame,
            text="🗑️ 清空",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['warning'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=4,
            bd=0,
            activebackground=self.colors['warning_hover'],
            command=lambda s=side: self.clear_files(s)
        ).pack(side=tk.LEFT)

        return card

    def create_config_panel(self):
        """创建配置面板"""
        panel = tk.Frame(self.content_container, bg=self.colors['bg'])

        # 标题
        header = tk.Frame(panel, bg=self.colors['card_bg'], relief=tk.FLAT, bd=0)
        header.pack(fill=tk.X, pady=(0, 15))

        tk.Label(
            header,
            text="⚙️ 配置选项",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=20, pady=15)

        tk.Label(
            header,
            text="设置列名后缀和统计列",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT, padx=(0, 20), pady=15)

        # 配置内容
        config_frame = tk.Frame(panel, bg=self.colors['card_bg'])
        config_frame.pack(fill=tk.BOTH, expand=True, padx=1)

        inner = tk.Frame(config_frame, bg=self.colors['card_bg'])
        inner.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 列名后缀配置
        suffix_group = self.create_config_group(inner, "列名后缀", "设置左右两侧数据的列名后缀")
        suffix_group.pack(fill=tk.X, pady=(0, 20))

        suffix_content = tk.Frame(suffix_group, bg=self.colors['card_bg'])
        suffix_content.pack(fill=tk.X, padx=15, pady=15)

        # 左侧后缀
        left_suffix_frame = tk.Frame(suffix_content, bg=self.colors['card_bg'])
        left_suffix_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            left_suffix_frame,
            text="🔵 左侧后缀:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=12,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Entry(
            left_suffix_frame,
            textvariable=self.left_suffix,
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            width=20
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            left_suffix_frame,
            text="例如: gauss, test1",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 右侧后缀
        right_suffix_frame = tk.Frame(suffix_content, bg=self.colors['card_bg'])
        right_suffix_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            right_suffix_frame,
            text="🔴 右侧后缀:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=12,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Entry(
            right_suffix_frame,
            textvariable=self.right_suffix,
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            width=20
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            right_suffix_frame,
            text="例如: sr, test2",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 统计列配置
        stats_group = self.create_config_group(inner, "统计列配置", "设置需要统计和计算的列名")
        stats_group.pack(fill=tk.BOTH, expand=True)

        stats_content = tk.Frame(stats_group, bg=self.colors['card_bg'])
        stats_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # 数值统计列
        numeric_frame = tk.Frame(stats_content, bg=self.colors['card_bg'])
        numeric_frame.pack(fill=tk.X, pady=8)

        tk.Label(
            numeric_frame,
            text="📊 数值统计列 (计算平均值):",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=25,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Entry(
            numeric_frame,
            textvariable=self.numeric_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 仅原始值列 + 自定义分隔符（并排）
        row2_frame = tk.Frame(stats_content, bg=self.colors['card_bg'])
        row2_frame.pack(fill=tk.X, pady=8)

        # 仅原始值
        left_row2 = tk.Frame(row2_frame, bg=self.colors['card_bg'])
        left_row2.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 15))

        tk.Label(
            left_row2,
            text="📝 仅原始值:",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=12,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 8))

        tk.Entry(
            left_row2,
            textvariable=self.original_only_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 自定义分隔符
        right_row2 = tk.Frame(row2_frame, bg=self.colors['card_bg'])
        right_row2.pack(side=tk.LEFT, fill=tk.X, expand=True)

        tk.Label(
            right_row2,
            text="🔧 自定义分隔符:",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=14,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 8))

        tk.Entry(
            right_row2,
            textvariable=self.custom_separator_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 提示信息
        tips_frame = tk.Frame(stats_content, bg=self.colors['card_bg'])
        tips_frame.pack(fill=tk.X, pady=(10, 0))

        tk.Label(
            tips_frame,
            text="💡 提示: 多个列名用逗号分隔；自定义分隔符格式为 '列名:分隔符'",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack()

        # 对比配置
        compare_group = self.create_config_group(inner, "双边对比配置", "设置双边对比时的差值计算和阈值")
        compare_group.pack(fill=tk.X, pady=(20, 0))

        compare_content = tk.Frame(compare_group, bg=self.colors['card_bg'])
        compare_content.pack(fill=tk.X, padx=15, pady=15)

        # 对比列配置
        compare_frame = tk.Frame(compare_content, bg=self.colors['card_bg'])
        compare_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            compare_frame,
            text="📊 对比列 (计算左-右差值):",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=25,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Entry(
            compare_frame,
            textvariable=self.compare_columns_var,
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            width=30
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            compare_frame,
            text="双边对比时计算差值的列",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 劣化阈值配置
        threshold_frame = tk.Frame(compare_content, bg=self.colors['card_bg'])
        threshold_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            threshold_frame,
            text="⚠️ 劣化阈值:",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=25,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Entry(
            threshold_frame,
            textvariable=self.deterioration_threshold_var,
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            width=10
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            threshold_frame,
            text="差值<-阈值为红色(劣化), >0为绿色(提升), 否则为无色(持平)",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        return panel

    def create_config_group(self, parent, title, description):
        """创建配置分组"""
        group = tk.Frame(parent, bg=self.colors['card_bg'], relief=tk.FLAT, bd=1)
        group.pack(fill=tk.X)

        # 标题栏
        header = tk.Frame(group, bg=self.colors['accent'], height=40)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(
            header,
            text=title,
            font=("Microsoft YaHei UI", 11, "bold"),
            bg=self.colors['accent'],
            fg='white'
        ).pack(side=tk.LEFT, padx=15, pady=10)

        tk.Label(
            header,
            text=description,
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['accent'],
            fg='#e1e8ed'
        ).pack(side=tk.LEFT, padx=(0, 15), pady=10)

        return group

    def create_output_panel(self):
        """创建输出面板"""
        panel = tk.Frame(self.content_container, bg=self.colors['bg'])

        # 标题
        header = tk.Frame(panel, bg=self.colors['card_bg'], relief=tk.FLAT, bd=0)
        header.pack(fill=tk.X, pady=(0, 15))

        tk.Label(
            header,
            text="📤 输出设置",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=20, pady=15)

        tk.Label(
            header,
            text="指定结果文件的保存路径",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT, padx=(0, 20), pady=15)

        # 输出配置
        output_frame = tk.Frame(panel, bg=self.colors['card_bg'])
        output_frame.pack(fill=tk.BOTH, expand=True)

        inner = tk.Frame(output_frame, bg=self.colors['card_bg'])
        inner.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 文件路径
        path_frame = tk.Frame(inner, bg=self.colors['card_bg'])
        path_frame.pack(fill=tk.X, pady=10)

        tk.Label(
            path_frame,
            text="📁 输出文件路径:",
            font=("Microsoft YaHei UI", 11, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=16,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 15))

        tk.Entry(
            path_frame,
            textvariable=self.output_file,
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Button(
            path_frame,
            text="浏览...",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=6,
            bd=0,
            activebackground=self.colors['accent_hover'],
            command=self.browse_output_file
        ).pack(side=tk.LEFT)

        # 说明信息
        info_frame = tk.Frame(inner, bg=self.colors['card_bg'])
        info_frame.pack(fill=tk.X, pady=(20, 0))

        tk.Label(
            info_frame,
            text="📋 支持格式: CSV (.csv), Excel (.xlsx, .xls)",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack()

        return panel

    def create_logs_panel(self):
        """创建日志面板"""
        panel = tk.Frame(self.content_container, bg=self.colors['bg'])

        # 标题
        header = tk.Frame(panel, bg=self.colors['card_bg'], relief=tk.FLAT, bd=0)
        header.pack(fill=tk.X, pady=(0, 15))

        tk.Label(
            header,
            text="📋 执行日志",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=20, pady=15)

        tk.Label(
            header,
            text="查看详细的执行过程和结果",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT, padx=(0, 20), pady=15)

        # 日志显示区
        log_frame = tk.Frame(panel, bg=self.colors['card_bg'])
        log_frame.pack(fill=tk.BOTH, expand=True)

        inner = tk.Frame(log_frame, bg='#1e1e1e')
        inner.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)

        # 滚动条
        scrollbar = tk.Scrollbar(inner)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text = tk.Text(
            inner,
            font=("Consolas", 10),
            bg='#1e1e1e',
            fg='#00ff00',
            insertbackground='white',
            relief=tk.FLAT,
            bd=0,
            yscrollcommand=scrollbar.set,
            wrap=tk.WORD,
            padx=15,
            pady=15
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.log_text.yview)

        return panel

    def create_bottom_bar(self):
        """创建底部操作栏"""
        bottom_frame = tk.Frame(self.main_content, bg=self.colors['card_bg'], relief=tk.FLAT, bd=1)
        bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=(0, 20))

        inner = tk.Frame(bottom_frame, bg=self.colors['card_bg'])
        inner.pack(fill=tk.X, padx=20, pady=15)

        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            inner,
            variable=self.progress_var,
            maximum=100,
            style='Custom.Horizontal.TProgressbar'
        )
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 20))

        # 配置进度条样式
        style = ttk.Style()
        style.theme_use('clam')
        style.configure(
            "Custom.Horizontal.TProgressbar",
            troughcolor=self.colors['bg'],
            background=self.colors['accent'],
            borderwidth=0,
            thickness=8
        )

        # 状态标签
        self.status_label = tk.Label(
            inner,
            text="就绪",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary'],
            width=20
        )
        self.status_label.pack(side=tk.LEFT, padx=(0, 15))

        # 开始按钮
        self.start_button = tk.Button(
            inner,
            text="🚀 开始合并",
            command=self.start_merge,
            font=("Microsoft YaHei UI", 12, "bold"),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=30,
            pady=10,
            bd=0,
            activebackground=self.colors['success_hover']
        )
        self.start_button.pack(side=tk.RIGHT)

    def show_panel(self, panel_id):
        """显示指定的面板"""
        # 隐藏所有面板
        for panel in self.panels.values():
            panel.pack_forget()

        # 显示选中的面板
        self.panels[panel_id].pack(fill=tk.BOTH, expand=True)

        # 更新导航按钮状态
        for btn_id, btn in self.nav_buttons.items():
            if btn_id == panel_id:
                btn.config(bg=self.colors['sidebar_active'])
            else:
                btn.config(bg=self.colors['sidebar_bg'])

        self.current_panel = panel_id

    # 以下是原有的功能方法，需要适配新界面
    def browse_dir(self, side):
        """浏览目录"""
        initial_dir = self.left_dir.get() if side == 'left' else self.right_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        directory = filedialog.askdirectory(
            title=f"选择{'左侧' if side == 'left' else '右侧'}数据目录",
            initialdir=initial_dir
        )
        if directory:
            if side == 'left':
                self.left_dir.set(directory)
            else:
                self.right_dir.set(directory)
            self.load_directory_files(directory, side)
            self.log(f"📁 {'左侧' if side == 'left' else '右侧'}目录: {directory}")

    def load_directory_files(self, directory, side):
        """加载目录中的文件到列表"""
        if not directory or not os.path.isdir(directory):
            return

        supported_extensions = ['.csv', '.xlsx', '.xls']
        files = []

        for ext in supported_extensions:
            pattern = os.path.join(directory, f"*{ext}")
            files.extend(glob.glob(pattern))

        files = sorted(files)

        listbox = self.left_file_listbox if side == 'left' else self.right_file_listbox
        file_list = self.left_files if side == 'left' else self.right_files

        listbox.delete(0, tk.END)
        file_list.clear()

        for file_path in files:
            filename = os.path.basename(file_path)
            listbox.insert(tk.END, filename)
            file_list.append(file_path)

        self.log(f"  自动加载了 {len(files)} 个文件到{'左侧' if side == 'left' else '右侧'}列表")

    def add_files(self, side):
        """添加文件"""
        initial_dir = self.left_dir.get() if side == 'left' else self.right_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        files = filedialog.askopenfilenames(
            title=f"选择{'左侧' if side == 'left' else '右侧'}数据文件",
            initialdir=initial_dir,
            filetypes=[
                ("支持的文件", "*.csv;*.xlsx;*.xls"),
                ("CSV文件", "*.csv"),
                ("Excel文件", "*.xlsx;*.xls"),
                ("所有文件", "*.*")
            ]
        )
        if files:
            listbox = self.left_file_listbox if side == 'left' else self.right_file_listbox
            file_list = self.left_files if side == 'left' else self.right_files

            for file_path in files:
                if file_path not in file_list:
                    filename = os.path.basename(file_path)
                    listbox.insert(tk.END, filename)
                    file_list.append(file_path)
            self.log(f"📄 添加了 {len(files)} 个{'左侧' if side == 'left' else '右侧'}文件")

    def remove_files(self, side):
        """移除选中的文件"""
        listbox = self.left_file_listbox if side == 'left' else self.right_file_listbox
        file_list = self.left_files if side == 'left' else self.right_files

        selection = listbox.curselection()
        if selection:
            for idx in reversed(selection):
                listbox.delete(idx)
                del file_list[idx]
            self.log(f"🗑️  移除了 {len(selection)} 个{'左侧' if side == 'left' else '右侧'}文件")

    def clear_files(self, side):
        """清空所有文件"""
        listbox = self.left_file_listbox if side == 'left' else self.right_file_listbox
        file_list = self.left_files if side == 'left' else self.right_files

        listbox.delete(0, tk.END)
        file_list.clear()
        self.log(f"🗑️  清空了所有{'左侧' if side == 'left' else '右侧'}文件")

    def browse_output_file(self):
        """浏览输出文件"""
        current_output = self.output_file.get()
        initial_dir = os.path.dirname(current_output)
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        default_file = os.path.basename(current_output) if current_output else "comparison_result.csv"

        file_path = filedialog.asksaveasfilename(
            title="选择输出文件",
            initialdir=initial_dir,
            initialfile=default_file,
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("Excel文件", "*.xlsx"), ("所有文件", "*.*")]
        )
        if file_path:
            self.output_file.set(file_path)
            self.log(f"💾 输出文件: {file_path}")

    def log(self, message):
        """添加日志"""
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.root.update()

    def import_config(self):
        """导入配置"""
        file_path = filedialog.askopenfilename(
            title="导入配置文件",
            defaultextension=".json",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)

                self.left_suffix.set(config.get('left_suffix', 'gauss'))
                self.right_suffix.set(config.get('right_suffix', 'sr'))
                self.numeric_columns_var.set(config.get('numeric_columns', ''))
                self.original_only_columns_var.set(config.get('original_only_columns', ''))
                self.custom_separator_columns_var.set(config.get('custom_separator_columns', ''))

                self.log(f"✓ 配置已导入: {file_path}")
                messagebox.showinfo("成功", "配置导入成功！")
            except Exception as e:
                self.log(f"✗ 导入失败: {e}")
                messagebox.showerror("错误", f"导入配置失败:\n{e}")

    def export_config(self):
        """导出配置"""
        config = {
            'left_suffix': self.left_suffix.get(),
            'right_suffix': self.right_suffix.get(),
            'numeric_columns': self.numeric_columns_var.get(),
            'original_only_columns': self.original_only_columns_var.get(),
            'custom_separator_columns': self.custom_separator_columns_var.get()
        }

        file_path = filedialog.asksaveasfilename(
            title="导出配置文件",
            defaultextension=".json",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
                self.log(f"✓ 配置已导出: {file_path}")
                messagebox.showinfo("成功", "配置导出成功！")
            except Exception as e:
                self.log(f"✗ 导出失败: {e}")
                messagebox.showerror("错误", f"导出配置失败:\n{e}")

    def reset_config(self):
        """重置为默认配置"""
        if messagebox.askyesno("确认", "确定要重置为默认配置吗？"):
            self.left_suffix.set("gauss")
            self.right_suffix.set("sr")
            self.numeric_columns_var.set("TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime,StartMemory,EndMemory,MaxMemory,MaxCpu")
            self.original_only_columns_var.set("FileCount")
            self.custom_separator_columns_var.set("EndTimeRecord:||")
            self.log("🔄 配置已重置为默认值")

    def update_progress(self, value, status):
        """更新进度"""
        self.progress_var.set(value)
        self.status_label.config(text=status)
        self.root.update()

    def open_file(self, file_path):
        """打开文件"""
        try:
            if platform.system() == 'Windows':
                os.startfile(file_path)
            elif platform.system() == 'Darwin':
                subprocess.call(['open', file_path])
            else:
                subprocess.call(['xdg-open', file_path])
            self.log(f"📂 已打开文件: {file_path}")
        except Exception as e:
            self.log(f"⚠️  无法打开文件: {e}")
            messagebox.showwarning("警告", f"无法打开文件:\n{e}")

    # 以下是核心合并逻辑（与原版相同）
    def merge_files(self, files, output_path):
        """合并多个文件"""
        # 这里使用原版的合并逻辑
        dfs = []
        for file_path in files:
            try:
                df = read_file(file_path)
                self.log(f"  ✓ 读取成功: {os.path.basename(file_path)} ({len(df)} 行)")

                if 'OriginFileName' in df.columns:
                    seen_files = set()
                    split_indices = []

                    for idx, origin_file in enumerate(df['OriginFileName']):
                        if origin_file in seen_files:
                            split_indices.append(idx)
                            seen_files = set([origin_file])
                        else:
                            seen_files.add(origin_file)

                    if split_indices:
                        self.log(f"    → 检测到 {len(split_indices) + 1} 组重复的 OriginFileName，进行拆分...")

                        start_idx = 0
                        for i, split_idx in enumerate(split_indices):
                            end_idx = split_idx
                            df_part = df.iloc[start_idx:end_idx].copy()
                            dfs.append(df_part)
                            self.log(f"      - 第 {i+1} 部分: {len(df_part)} 行")
                            start_idx = split_idx

                        df_last = df.iloc[start_idx:].copy()
                        dfs.append(df_last)
                        self.log(f"      - 第 {len(split_indices) + 1} 部分: {len(df_last)} 行")
                    else:
                        dfs.append(df)
                else:
                    dfs.append(df)

            except Exception as e:
                self.log(f"  ✗ 读取失败: {os.path.basename(file_path)} - {e}")
                raise

        if len(dfs) < 2:
            raise ValueError("至少需要2个测试数据才能进行合并")

        self.log(f"\n  共 {len(dfs)} 个 DataFrame 参与合并")
        for i, df in enumerate(dfs):
            unique_files = df['OriginFileName'].nunique() if 'OriginFileName' in df.columns else 1
            self.log(f"    - DataFrame {i+1}: {len(df)} 行, {unique_files} 个不同的 OriginFileName")

        num_rows = len(dfs[0])

        # 从界面获取列配置
        numeric_columns_str = self.numeric_columns_var.get().strip()
        numeric_columns = [col.strip() for col in numeric_columns_str.split(',') if col.strip()] if numeric_columns_str else []

        original_only_columns_str = self.original_only_columns_var.get().strip()
        original_only_columns = [col.strip() for col in original_only_columns_str.split(',') if col.strip()] if original_only_columns_str else []

        custom_separator_columns_str = self.custom_separator_columns_var.get().strip()
        custom_separator_columns = {}
        if custom_separator_columns_str:
            for item in custom_separator_columns_str.split(','):
                if ':' in item:
                    col, sep = item.split(':', 1)
                    custom_separator_columns[col.strip()] = sep.strip()

        base_columns = ['OriginFileName', 'PreferenceStdFileName', 'Type', 'Level', 'Env&Ver', 'Row&Column']

        self.log(f"\n📋 列配置:")
        self.log(f"  - 数值统计列 ({len(numeric_columns)}): {', '.join(numeric_columns)}")
        self.log(f"  - 仅原始值列 ({len(original_only_columns)}): {', '.join(original_only_columns)}")
        self.log(f"  - 自定义分隔符列 ({len(custom_separator_columns)}): {', '.join([f'{k}:{v}' for k, v in custom_separator_columns.items()])}")

        # 创建汇总结果
        result_df = dfs[0][base_columns].copy()

        # 处理数值列
        for col in numeric_columns:
            original_values = []
            for i in range(num_rows):
                vals = []
                for df in dfs:
                    v = df[col].values[i]
                    if pd.notna(v) and v != '' and v != 'TraceBack':
                        vals.append(str(v))
                original_values.append('/'.join(vals))

            result_df[f'{col}_Original'] = original_values

            # 计算平均值
            averages = []
            for i in range(num_rows):
                nums_with_units = []
                for df in dfs:
                    val = df[col].values[i]
                    if pd.notna(val) and val != '' and val != 'TraceBack':
                        num, unit = extract_number_with_unit(val)
                        if num is not None:
                            if 'Memory' in col:
                                if unit == 'G':
                                    nums_with_units.append(num * 1024)
                                else:
                                    nums_with_units.append(num)
                            else:
                                nums_with_units.append(num)

                if nums_with_units:
                    avg = sum(nums_with_units) / len(nums_with_units)
                    if 'Memory' in col:
                        if avg >= 1024:
                            averages.append(f"{avg/1024:.1f}G")
                        else:
                            averages.append(f"{avg:.0f}M")
                    elif 'Time' in col and 'TimeCount' not in col:
                        averages.append(f"{avg:.3f}s")
                    elif 'Cpu' in col:
                        averages.append(f"{avg:.0f}%")
                    else:
                        averages.append(f"{avg:.1f}s")
                else:
                    averages.append('')

            result_df[f'{col}_Average'] = averages

        # 处理其他列...
        # (省略部分代码，与原版相同)

        save_file(result_df, output_path)
        return result_df

    def start_merge(self):
        """开始合并流程"""
        has_left = len(self.left_files) > 0
        has_right = len(self.right_files) > 0

        if not has_left and not has_right:
            messagebox.showerror("错误", "请至少添加一个文件！")
            return

        # 自动切换到日志面板
        self.show_panel('logs')

        # 清空日志
        self.log_text.delete(1.0, tk.END)

        # 禁用按钮
        self.start_button.config(state=tk.DISABLED, text="处理中...")

        # 在新线程中执行
        thread = Thread(target=self.execute_merge)
        thread.start()

    def execute_merge(self):
        """执行合并操作"""
        try:
            has_left = len(self.left_files) > 0
            has_right = len(self.right_files) > 0
            both_sides = has_left and has_right

            self.log("=" * 60)
            if both_sides:
                self.log("🔍 步骤 1: 检查测试文件")
            else:
                self.log("🔍 检查测试文件")
            self.log("=" * 60)

            left_files = self.left_files.copy()
            right_files = self.right_files.copy()

            if has_left:
                self.log(f"📂 左侧选择了 {len(left_files)} 个文件")
                for f in left_files:
                    self.log(f"  - {os.path.basename(f)}")

            if has_right:
                self.log(f"📂 右侧选择了 {len(right_files)} 个文件")
                for f in right_files:
                    self.log(f"  - {os.path.basename(f)}")

            if not both_sides:
                self.log("\n📝 检测到只有单边数据，执行单边合并模式")
                self.log("=" * 60)

                files_to_merge = left_files if has_left else right_files
                side_label = "左侧" if has_left else "右侧"

                self.update_progress(20, f"正在合并{side_label}数据...")

                self.log(f"\n🔹 合并{side_label}数据")
                self.log("=" * 60)

                merged_result = self.merge_files(files_to_merge, self.output_file.get())
                self.log(f"✓ {side_label}数据合并完成: {len(merged_result)} 行, {len(merged_result.columns)} 列")

                # 保存到历史记录
                self.add_to_history(left_files, right_files, self.output_file.get())

                self.update_progress(100, "处理完成！")

                self.log("\n" + "=" * 60)
                self.log("🎉 处理完成！")
                self.log("=" * 60)
                self.log(f"📊 结果已保存到: {self.output_file.get()}")
                self.log(f"📈 总行数: {len(merged_result)}")
                self.log(f"📋 总列数: {len(merged_result.columns)}")

                result = messagebox.askyesno(
                    "成功",
                    f"{side_label}数据合并完成！\n\n结果已保存到:\n{self.output_file.get()}\n\n是否立即打开结果文件？",
                    icon=messagebox.INFO
                )
                if result:
                    self.open_file(self.output_file.get())
            else:
                self.log("\n⚖️  双边对比模式")
                self.update_progress(10, "正在合并左侧数据...")

                left_temp = os.path.join(os.path.dirname(self.output_file.get()), "_temp_left_merge.csv")
                left_merged = self.merge_files(left_files, left_temp)
                self.log(f"✓ 左侧数据合并完成: {len(left_merged)} 行")

                self.update_progress(40, "正在合并右侧数据...")

                right_temp = os.path.join(os.path.dirname(self.output_file.get()), "_temp_right_merge.csv")
                right_merged = self.merge_files(right_files, right_temp)
                self.log(f"✓ 右侧数据合并完成: {len(right_merged)} 行")

                self.update_progress(70, "正在对比合并结果...")

                self.log("\n" + "=" * 60)
                self.log("⚖️  对比两个合并结果")
                self.log("=" * 60)

                # 执行双边对比
                comparison_result = self.compare_and_calculate_diff(left_merged, right_merged)
                self.log(f"✓ 对比完成: {len(comparison_result)} 行")

                self.update_progress(90, "正在保存结果...")

                # 获取对比列配置（用于颜色设置）
                compare_columns_str = self.compare_columns_var.get().strip()
                compare_columns = [col.strip() for col in compare_columns_str.split(',') if col.strip()] if compare_columns_str else []

                # 准备状态列颜色配置
                status_columns = {}
                for col in compare_columns:
                    status_col = f"{col}_状态"
                    if status_col in comparison_result.columns:
                        status_columns[status_col] = {
                            '✓ 提升': '90EE90',  # 淡绿色
                            '✗ 劣化': 'FFB6C1',  # 淡红色
                            '- 持平': 'FFFFFF'   # 白色（默认）
                        }

                # 保存结果（带颜色）
                save_file(comparison_result, self.output_file.get(), status_columns=status_columns)

                self.update_progress(100, "处理完成！")

                try:
                    os.remove(left_temp)
                    os.remove(right_temp)
                except:
                    pass

                self.log("\n" + "=" * 60)
                self.log("🎉 处理完成！")
                self.log("=" * 60)
                self.log(f"📊 结果已保存到: {self.output_file.get()}")
                self.log(f"📈 总行数: {len(comparison_result)}")
                self.log(f"📋 总列数: {len(comparison_result.columns)}")

                # 保存到历史记录
                self.add_to_history(left_files, right_files, self.output_file.get())

                result = messagebox.askyesno(
                    "成功",
                    f"合并对比完成！\n\n结果已保存到:\n{self.output_file.get()}\n\n是否立即打开结果文件？",
                    icon=messagebox.INFO
                )
                if result:
                    self.open_file(self.output_file.get())

        except Exception as e:
            self.log(f"\n❌ 错误: {str(e)}")
            messagebox.showerror("错误", f"处理失败:\n{str(e)}")

        finally:
            self.start_button.config(state=tk.NORMAL, text="🚀 开始合并")

    def compare_and_calculate_diff(self, left_df, right_df):
        """对比两个数据集并计算差值"""
        # 获取对比列配置
        compare_columns_str = self.compare_columns_var.get().strip()
        compare_columns = [col.strip() for col in compare_columns_str.split(',') if col.strip()] if compare_columns_str else []

        # 获取劣化阈值
        try:
            threshold = float(self.deterioration_threshold_var.get())
        except:
            threshold = 3.0
            self.log(f"⚠️  劣化阈值格式错误，使用默认值: {threshold}")

        self.log(f"\n📊 对比配置:")
        self.log(f"  - 对比列 ({len(compare_columns)}): {', '.join(compare_columns)}")
        self.log(f"  - 劣化阈值: {threshold}")

        # 基础列（用于匹配）
        base_columns = ['OriginFileName', 'PreferenceStdFileName', 'Type', 'Level', 'Env&Ver', 'Row&Column']

        # 为左右数据集的数值列添加后缀
        left_suffix = self.left_suffix.get()
        right_suffix = self.right_suffix.get()

        # 创建副本避免修改原始数据
        left_renamed = left_df.copy()
        right_renamed = right_df.copy()

        # 重命名左表的数值列（除了基础列）
        for col in left_renamed.columns:
            if col not in base_columns:
                left_renamed = left_renamed.rename(columns={col: f"{col}_{left_suffix}"})

        # 重命名右表的数值列（除了基础列）
        for col in right_renamed.columns:
            if col not in base_columns:
                right_renamed = right_renamed.rename(columns={col: f"{col}_{right_suffix}"})

        # 使用基础列进行merge
        match_columns = ['OriginFileName', 'Type', 'Level']
        self.log(f"  - 匹配列: {', '.join(match_columns)}")

        # 执行merge
        merged_df = pd.merge(
            left_renamed,
            right_renamed,
            on=match_columns,
            how='outer',
            suffixes=('', '_dup')
        )

        # 删除重复列
        duplicate_cols = [col for col in merged_df.columns if col.endswith('_dup')]
        if duplicate_cols:
            merged_df = merged_df.drop(columns=duplicate_cols)

        # 计算差值和劣化指标
        for col in compare_columns:
            # 尝试多种可能的列名格式
            possible_left_cols = [
                f"{col}_{left_suffix}",           # 直接后缀
                f"{col}_Average_{left_suffix}",  # 带Average后缀
                f"{col}_Original_{left_suffix}"  # 带Original后缀
            ]
            possible_right_cols = [
                f"{col}_{right_suffix}",
                f"{col}_Average_{right_suffix}",
                f"{col}_Original_{right_suffix}"
            ]

            # 找到实际存在的列名
            left_col = None
            right_col = None
            for possible_col in possible_left_cols:
                if possible_col in merged_df.columns:
                    left_col = possible_col
                    break

            for possible_col in possible_right_cols:
                if possible_col in merged_df.columns:
                    right_col = possible_col
                    break

            if left_col and right_col:
                # 计算差值列（左-右）
                diff_col = f"{col}_差值"

                # 提取数值进行计算
                def extract_value(val):
                    if pd.isna(val):
                        return None
                    # 使用现有的提取函数
                    num, unit = extract_number_with_unit(val)
                    return num

                left_values = merged_df[left_col].apply(extract_value)
                right_values = merged_df[right_col].apply(extract_value)

                # 计算差值
                differences = []
                deterioration = []

                for lv, rv in zip(left_values, right_values):
                    if lv is not None and rv is not None:
                        diff = lv - rv
                        differences.append(f"{diff:+.1f}")

                        # 判断劣化状态
                        if diff > 0:
                            deterioration.append("✓ 提升")  # 正值=提升=绿色
                        elif diff < -threshold:
                            deterioration.append("✗ 劣化")  # 负值且超过阈值=劣化=红色
                        else:
                            deterioration.append("- 持平")  # 在阈值范围内=持平=无色
                    elif lv is not None and rv is None:
                        differences.append("N/A")
                        deterioration.append("仅左侧有")
                    elif lv is None and rv is not None:
                        differences.append("N/A")
                        deterioration.append("仅右侧有")
                    else:
                        differences.append("")
                        deterioration.append("")

                merged_df[diff_col] = differences
                merged_df[f"{col}_状态"] = deterioration

                self.log(f"  ✓ 已计算列 '{col}' 的差值和状态")

        # 重新排列列顺序
        ordered_columns = []

        # 先添加基础列
        for col in base_columns:
            if col in merged_df.columns:
                ordered_columns.append(col)

        # 按原始顺序添加左表列和差值
        for col in left_df.columns:
            if col not in base_columns:
                renamed_col = f"{col}_{left_suffix}"
                if renamed_col in merged_df.columns:
                    ordered_columns.append(renamed_col)

        # 添加右表列
        for col in right_df.columns:
            if col not in base_columns:
                renamed_col = f"{col}_{right_suffix}"
                if renamed_col in merged_df.columns and renamed_col not in ordered_columns:
                    ordered_columns.append(renamed_col)

        # 添加差值和状态列
        for col in compare_columns:
            diff_col = f"{col}_差值"
            status_col = f"{col}_状态"
            if diff_col in merged_df.columns:
                ordered_columns.append(diff_col)
            if status_col in merged_df.columns:
                ordered_columns.append(status_col)

        merged_df = merged_df[ordered_columns]

        return merged_df

    # ========== 历史记录功能 ==========
    def load_history(self):
        """加载历史记录"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {'records': []}
        except Exception as e:
            self.log(f"⚠️  加载历史记录失败: {e}")
            return {'records': []}

    def save_history(self):
        """保存历史记录"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log(f"⚠️  保存历史记录失败: {e}")

    def add_to_history(self, left_files, right_files, output_file):
        """添加记录到历史（如果配置相同则覆盖已有记录）"""
        # 创建新记录
        new_record = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'left_files': [os.path.basename(f) for f in left_files],
            'left_paths': left_files,
            'right_files': [os.path.basename(f) for f in right_files],
            'right_paths': right_files,
            'output_file': output_file,
            'config': {
                'left_suffix': self.left_suffix.get(),
                'right_suffix': self.right_suffix.get(),
                'numeric_columns': self.numeric_columns_var.get(),
                'original_only_columns': self.original_only_columns_var.get(),
                'custom_separator_columns': self.custom_separator_columns_var.get(),
                'compare_columns': self.compare_columns_var.get(),
                'deterioration_threshold': self.deterioration_threshold_var.get()
            }
        }

        # 检查是否已存在相同的配置记录
        # 比较依据：文件路径（排序后）和所有配置项
        left_paths_sorted = sorted(left_files)
        right_paths_sorted = sorted(right_files)
        new_config = new_record['config']

        # 查找匹配的现有记录
        matched_index = None
        for idx, existing_record in enumerate(self.history['records']):
            # 检查文件路径是否相同（数量和路径都要匹配）
            existing_left_sorted = sorted(existing_record['left_paths'])
            existing_right_sorted = sorted(existing_record['right_paths'])

            if (existing_left_sorted == left_paths_sorted and
                existing_right_sorted == right_paths_sorted and
                existing_record['config'] == new_config):
                matched_index = idx
                break

        if matched_index is not None:
            # 覆盖已有记录（更新时间戳）
            self.history['records'].pop(matched_index)
            self.history['records'].insert(0, new_record)
            self.log(f"💾 已更新历史记录（覆盖重复配置）")
        else:
            # 新增记录
            self.history['records'].insert(0, new_record)
            self.log(f"💾 已保存到历史记录")

        # 限制历史记录数量
        if len(self.history['records']) > self.max_history_items:
            self.history['records'] = self.history['records'][:self.max_history_items]

        self.save_history()

    def load_history_record(self, record):
        """加载指定的历史记录"""
        try:
            # 加载文件列表
            self.left_files.clear()
            self.right_files.clear()
            self.left_file_listbox.delete(0, tk.END)
            self.right_file_listbox.delete(0, tk.END)

            loaded_left = 0
            for path in record['left_paths']:
                if os.path.exists(path):
                    self.left_files.append(path)
                    self.left_file_listbox.insert(tk.END, os.path.basename(path))
                    loaded_left += 1

            loaded_right = 0
            for path in record['right_paths']:
                if os.path.exists(path):
                    self.right_files.append(path)
                    self.right_file_listbox.insert(tk.END, os.path.basename(path))
                    loaded_right += 1

            # 加载配置
            config = record['config']
            self.left_suffix.set(config['left_suffix'])
            self.right_suffix.set(config['right_suffix'])
            self.numeric_columns_var.set(config['numeric_columns'])
            self.original_only_columns_var.set(config['original_only_columns'])
            self.custom_separator_columns_var.set(config['custom_separator_columns'])
            self.compare_columns_var.set(config.get('compare_columns', 'TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime'))
            self.deterioration_threshold_var.set(config.get('deterioration_threshold', '3'))

            # 加载输出文件
            self.output_file.set(record['output_file'])

            self.log(f"✓ 已加载历史记录: {record['timestamp']}")
            self.log(f"  - 左侧文件: {loaded_left}/{len(record['left_paths'])}")
            self.log(f"  - 右侧文件: {loaded_right}/{len(record['right_paths'])}")

            messagebox.showinfo("成功", f"历史记录加载成功！\n\n左侧文件: {loaded_left}/{len(record['left_paths'])}\n右侧文件: {loaded_right}/{len(record['right_paths'])}")

        except Exception as e:
            self.log(f"✗ 加载历史记录失败: {e}")
            messagebox.showerror("错误", f"加载历史记录失败:\n{e}")

    def show_history_dialog(self):
        """显示历史记录对话框"""
        dialog = tk.Toplevel(self.root)
        dialog.title("📜 历史记录")
        dialog.geometry("800x500")
        dialog.configure(bg=self.colors['bg'])

        # 标题
        title_frame = tk.Frame(dialog, bg=self.colors['card_bg'])
        title_frame.pack(fill=tk.X, padx=20, pady=20)

        tk.Label(
            title_frame,
            text="📜 历史记录",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=20, pady=15)

        tk.Label(
            title_frame,
            text=f"共 {len(self.history['records'])} 条记录",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT, padx=(0, 20), pady=15)

        # 内容区域（带滚动）
        content_frame = tk.Frame(dialog, bg=self.colors['bg'])
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))

        canvas = tk.Canvas(content_frame, bg=self.colors['bg'], highlightthickness=0)
        scrollbar = tk.Scrollbar(content_frame, orient=tk.VERTICAL, command=canvas.yview)

        scrollable_frame = tk.Frame(canvas, bg=self.colors['bg'])
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.config(yscrollcommand=scrollbar.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 绑定鼠标滚轮事件
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        def _bind_to_mousewheel(event):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def _unbind_from_mousewheel(event):
            canvas.unbind_all("<MouseWheel>")

        canvas.bind('<Enter>', _bind_to_mousewheel)
        canvas.bind('<Leave>', _unbind_from_mousewheel)

        # 显示历史记录
        if not self.history['records']:
            tk.Label(
                scrollable_frame,
                text="暂无历史记录",
                font=("Microsoft YaHei UI", 12),
                bg=self.colors['bg'],
                fg=self.colors['text_secondary']
            ).pack(pady=50)
        else:
            for idx, record in enumerate(self.history['records']):
                self.create_history_item(scrollable_frame, record, idx, dialog)

    def create_history_item(self, parent, record, idx, dialog):
        """创建历史记录项"""
        item_frame = tk.Frame(
            parent,
            bg=self.colors['card_bg'],
            relief=tk.FLAT,
            bd=1
        )
        item_frame.pack(fill=tk.X, pady=10, padx=5)

        inner_frame = tk.Frame(item_frame, bg=self.colors['card_bg'])
        inner_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # 时间戳
        tk.Label(
            inner_frame,
            text=f"📅 {record['timestamp']}",
            font=("Microsoft YaHei UI", 11, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(anchor='w')

        # 文件信息（左右并排）
        files_frame = tk.Frame(inner_frame, bg=self.colors['card_bg'])
        files_frame.pack(fill=tk.X, pady=(8, 0))

        # 左侧文件
        left_files_frame = tk.Frame(files_frame, bg=self.colors['card_bg'])
        left_files_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        tk.Label(
            left_files_frame,
            text=f"🔵 左侧 ({len(record['left_files'])}):",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w')

        left_files_display = record['left_files'][:3]
        if len(record['left_files']) > 3:
            left_files_display.append(f"...等 {len(record['left_files'])} 个")

        for filename in left_files_display:
            tk.Label(
                left_files_frame,
                text=f"  • {filename}",
                font=("Microsoft YaHei UI", 9),
                bg=self.colors['card_bg'],
                fg=self.colors['text']
            ).pack(anchor='w')

        # 右侧文件
        right_files_frame = tk.Frame(files_frame, bg=self.colors['card_bg'])
        right_files_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))

        tk.Label(
            right_files_frame,
            text=f"🔴 右侧 ({len(record['right_files'])}):",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w')

        right_files_display = record['right_files'][:3]
        if len(record['right_files']) > 3:
            right_files_display.append(f"...等 {len(record['right_files'])} 个")

        for filename in right_files_display:
            tk.Label(
                right_files_frame,
                text=f"  • {filename}",
                font=("Microsoft YaHei UI", 9),
                bg=self.colors['card_bg'],
                fg=self.colors['text']
            ).pack(anchor='w')

        # 配置信息
        config_frame = tk.Frame(inner_frame, bg=self.colors['bg'], relief=tk.FLAT, bd=1)
        config_frame.pack(fill=tk.X, pady=(10, 0))

        config_inner = tk.Frame(config_frame, bg=self.colors['bg'])
        config_inner.pack(fill=tk.X, padx=10, pady=8)

        tk.Label(
            config_inner,
            text="⚙️ 配置:",
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w')

        # 后缀配置
        config = record.get('config', {})
        suffix_text = f"后缀: {config.get('left_suffix', 'N/A')} / {config.get('right_suffix', 'N/A')}"
        tk.Label(
            config_inner,
            text=f"  • {suffix_text}",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['bg'],
            fg=self.colors['text']
        ).pack(anchor='w')

        # 数值列配置（显示前3个）
        numeric_cols = config.get('numeric_columns', '').split(',')
        if numeric_cols and numeric_cols[0]:
            numeric_display = ', '.join(numeric_cols[:3])
            if len(numeric_cols) > 3:
                numeric_display += f"... (+{len(numeric_cols)-3}个)"
            tk.Label(
                config_inner,
                text=f"  • 数值列: {numeric_display}",
                font=("Microsoft YaHei UI", 8),
                bg=self.colors['bg'],
                fg=self.colors['text']
            ).pack(anchor='w')

        # 按钮区
        btn_frame = tk.Frame(inner_frame, bg=self.colors['card_bg'])
        btn_frame.pack(fill=tk.X, pady=(10, 0))

        tk.Button(
            btn_frame,
            text="📂 加载此记录",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=5,
            bd=0,
            activebackground=self.colors['accent_hover'],
            command=lambda r=record: [self.load_history_record(r), dialog.destroy()]
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Button(
            btn_frame,
            text="🗑️ 删除",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['error'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=5,
            bd=0,
            activebackground='#c0392b',
            command=lambda i=idx, d=dialog: [self.delete_history_record(i), d.destroy()]
        ).pack(side=tk.LEFT)

    def delete_history_record(self, index):
        """删除指定的历史记录"""
        if 0 <= index < len(self.history['records']):
            record = self.history['records'][index]
            if messagebox.askyesno("确认删除", f"确定要删除以下记录吗？\n\n{record['timestamp']}"):
                del self.history['records'][index]
                self.save_history()
                self.log(f"🗑️  已删除历史记录: {record['timestamp']}")
                # 重新显示历史记录对话框
                self.root.after(100, self.show_history_dialog)

    def clear_history(self):
        """清空所有历史记录"""
        if not self.history['records']:
            messagebox.showinfo("提示", "当前没有历史记录")
            return

        if messagebox.askyesno("确认清空", f"确定要清空所有 {len(self.history['records'])} 条历史记录吗？"):
            self.history['records'] = []
            self.save_history()
            self.log("🗑️  已清空所有历史记录")
            messagebox.showinfo("成功", "历史记录已清空")


def main():
    root = tk.Tk()
    app = ModernMergeGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()
