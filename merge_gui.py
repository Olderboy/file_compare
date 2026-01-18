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


class MergeGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("测试结果合并对比工具")
        self.root.geometry("1100x1050")
        self.root.minsize(1000, 950)

        # 设置现代化主题颜色
        self.colors = {
            'bg': '#f0f2f5',
            'header_bg': '#667eea',
            'header_fg': '#ffffff',
            'accent': '#667eea',
            'accent_hover': '#5568d3',
            'success': '#10b981',
            'success_hover': '#059669',
            'warning': '#f59e0b',
            'warning_hover': '#d97706',
            'error': '#ef4444',
            'error_hover': '#dc2626',
            'card_bg': '#ffffff',
            'card_shadow': '#d1d5db',
            'input_bg': '#f9fafb',
            'input_border': '#e5e7eb',
            'input_focus': '#667eea',
            'text': '#1f2937',
            'text_secondary': '#6b7280',
            'border': '#e5e7eb'
        }

        self.root.configure(bg=self.colors['bg'])

        # 配置样式
        self.setup_styles()

        # 初始化变量（必须在创建界面之前）
        self.left_dir = tk.StringVar()
        self.right_dir = tk.StringVar()
        self.output_file = tk.StringVar(value=os.path.join(os.getcwd(), 'comparison_result.csv'))
        self.left_suffix = tk.StringVar(value="gauss")
        self.right_suffix = tk.StringVar(value="sr")
        self.left_files = []
        self.right_files = []

        # 统计列配置（默认值）
        self.numeric_columns_var = tk.StringVar(value="TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime,StartMemory,EndMemory,MaxMemory,MaxCpu")
        self.original_only_columns_var = tk.StringVar(value="FileCount")
        self.custom_separator_columns_var = tk.StringVar(value="EndTimeRecord:||")

        # 历史记录
        self.history_file = os.path.join(os.path.dirname(__file__), '.merge_history.json')
        self.max_history_items = 10
        self.history = self.load_history()

        # 创建主框架
        self.create_header()
        self.create_main_content()
        self.create_footer()

    def setup_styles(self):
        """配置ttk样式"""
        style = ttk.Style()

        # 配置Progressbar样式
        style.theme_use('clam')

        # 创建自定义Progressbar样式
        style.configure(
            "Custom.Horizontal.TProgressbar",
            troughcolor=self.colors['input_bg'],
            background=self.colors['accent'],
            borderwidth=0,
            thickness=8,
            lightcolor=self.colors['accent'],
            darkcolor=self.colors['accent_hover']
        )

    def create_header(self):
        """创建标题栏"""
        header = tk.Frame(self.root, bg=self.colors['header_bg'], height=70)
        header.pack(fill=tk.X, side=tk.TOP)
        header.pack_propagate(False)

        # 创建渐变效果的容器
        header_content = tk.Frame(header, bg=self.colors['header_bg'])
        header_content.pack(expand=True, fill='both', padx=20, pady=10)

        # 标题
        title = tk.Label(
            header_content,
            text="📊 测试结果合并对比工具",
            font=("Microsoft YaHei UI", 20, "bold"),
            bg=self.colors['header_bg'],
            fg=self.colors['header_fg']
        )
        title.pack(side=tk.LEFT, padx=10)

        # 副标题
        subtitle = tk.Label(
            header_content,
            text="Automated Test Result Comparison Tool",
            font=("Arial", 9),
            bg=self.colors['header_bg'],
            fg='#e0e7ff'
        )
        subtitle.pack(side=tk.LEFT, padx=(10, 0))

    def create_main_content(self):
        """创建主内容区"""
        main_frame = tk.Frame(self.root, bg=self.colors['bg'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        # 第一步：选择目录
        step1 = self.create_step_card(main_frame, "步骤 1: 选择测试数据目录", "选择两个包含测试结果的目录")
        step1.pack(fill=tk.X, pady=(0, 10))

        # 左侧目录和文件
        left_container = tk.Frame(step1, bg=self.colors['card_bg'])
        left_container.pack(fill=tk.X, padx=15, pady=5)

        # 左侧目录选择
        left_dir_frame = tk.Frame(left_container, bg=self.colors['card_bg'])
        left_dir_frame.pack(fill=tk.X, pady=(0, 5))

        tk.Label(
            left_dir_frame,
            text="🔵 左侧数据目录 (Gauss):",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=20,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        left_dir_entry = tk.Entry(
            left_dir_frame,
            textvariable=self.left_dir,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        left_dir_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Button(
            left_dir_frame,
            text="📁 选择目录",
            command=self.browse_left_dir,
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=20,
            pady=8,
            bd=0,
            activebackground=self.colors['accent_hover'],
            activeforeground='white'
        ).pack(side=tk.LEFT)

        # 左侧文件选择
        left_file_frame = tk.Frame(left_container, bg=self.colors['card_bg'])
        left_file_frame.pack(fill=tk.X, pady=(5, 0))

        tk.Label(
            left_file_frame,
            text="📄 左侧文件:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=20,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        self.left_file_listbox = tk.Listbox(
            left_file_frame,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            fg=self.colors['text'],
            height=3,
            selectmode=tk.MULTIPLE,
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            selectbackground=self.colors['accent'],
            selectforeground='white'
        )
        self.left_file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        left_btn_frame = tk.Frame(left_file_frame, bg=self.colors['card_bg'])
        left_btn_frame.pack(side=tk.LEFT)

        # 添加按钮
        tk.Button(
            left_btn_frame,
            text="➕ 添加",
            command=self.add_left_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['success_hover'],
            activeforeground='white'
        ).pack(pady=3)

        # 移除按钮
        tk.Button(
            left_btn_frame,
            text="➖ 移除",
            command=self.remove_left_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['error'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['error_hover'],
            activeforeground='white'
        ).pack(pady=3)

        # 清空按钮
        tk.Button(
            left_btn_frame,
            text="🗑️ 清空",
            command=self.clear_left_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['warning'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['warning_hover'],
            activeforeground='white'
        ).pack(pady=3)

        # 右侧目录和文件
        right_container = tk.Frame(step1, bg=self.colors['card_bg'])
        right_container.pack(fill=tk.X, padx=15, pady=5)

        # 右侧目录选择
        right_dir_frame = tk.Frame(right_container, bg=self.colors['card_bg'])
        right_dir_frame.pack(fill=tk.X, pady=(0, 5))

        tk.Label(
            right_dir_frame,
            text="🔴 右侧数据目录 (SR):",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=20,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        right_dir_entry = tk.Entry(
            right_dir_frame,
            textvariable=self.right_dir,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        right_dir_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Button(
            right_dir_frame,
            text="📁 选择目录",
            command=self.browse_right_dir,
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=20,
            pady=8,
            bd=0,
            activebackground=self.colors['accent_hover'],
            activeforeground='white'
        ).pack(side=tk.LEFT)

        # 右侧文件选择
        right_file_frame = tk.Frame(right_container, bg=self.colors['card_bg'])
        right_file_frame.pack(fill=tk.X, pady=(5, 0))

        tk.Label(
            right_file_frame,
            text="📄 右侧文件:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=20,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        self.right_file_listbox = tk.Listbox(
            right_file_frame,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            fg=self.colors['text'],
            height=3,
            selectmode=tk.MULTIPLE,
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus'],
            selectbackground=self.colors['accent'],
            selectforeground='white'
        )
        self.right_file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        right_btn_frame = tk.Frame(right_file_frame, bg=self.colors['card_bg'])
        right_btn_frame.pack(side=tk.LEFT)

        tk.Button(
            right_btn_frame,
            text="➕ 添加",
            command=self.add_right_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['success_hover'],
            activeforeground='white'
        ).pack(pady=3)

        tk.Button(
            right_btn_frame,
            text="➖ 移除",
            command=self.remove_right_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['error'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['error_hover'],
            activeforeground='white'
        ).pack(pady=3)

        tk.Button(
            right_btn_frame,
            text="🗑️ 清空",
            command=self.clear_right_files,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['warning'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            width=10,
            pady=5,
            bd=0,
            activebackground=self.colors['warning_hover'],
            activeforeground='white'
        ).pack(pady=3)

        # 第二步：配置列后缀
        step2 = self.create_step_card(main_frame, "步骤 2: 配置列名后缀", "为左右两边的数据列设置自定义后缀")
        step2.pack(fill=tk.X, pady=(0, 10))

        suffix_frame = tk.Frame(step2, bg=self.colors['card_bg'])
        suffix_frame.pack(fill=tk.X, padx=15, pady=8)

        # 左侧后缀
        left_suffix_frame = tk.Frame(suffix_frame, bg=self.colors['card_bg'])
        left_suffix_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            left_suffix_frame,
            text="🔵 左侧列后缀:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=15,
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
            width=18
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            left_suffix_frame,
            text="（例如: gauss, test1, old）",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 右侧后缀
        right_suffix_frame = tk.Frame(suffix_frame, bg=self.colors['card_bg'])
        right_suffix_frame.pack(fill=tk.X, pady=5)

        tk.Label(
            right_suffix_frame,
            text="🔴 右侧列后缀:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=15,
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
            width=18
        ).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(
            right_suffix_frame,
            text="（例如: sr, test2, new）",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 第三步：配置统计列
        step3 = self.create_step_card(main_frame, "步骤 3: 配置统计列", "设置需要统计和计算的列名")
        step3.pack(fill=tk.X, pady=(0, 15))

        columns_frame = tk.Frame(step3, bg=self.colors['card_bg'])
        columns_frame.pack(fill=tk.X, padx=15, pady=8)

        # 第一行：数值统计列
        numeric_frame = tk.Frame(columns_frame, bg=self.colors['card_bg'])
        numeric_frame.pack(fill=tk.X, pady=3)

        tk.Label(
            numeric_frame,
            text="📊 数值统计列:",
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=14,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 8))

        numeric_entry = tk.Entry(
            numeric_frame,
            textvariable=self.numeric_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        numeric_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))

        tk.Label(
            numeric_frame,
            text="计算平均值",
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(side=tk.LEFT)

        # 第二行：仅原始值列 + 自定义分隔符列（并排）
        row2_frame = tk.Frame(columns_frame, bg=self.colors['card_bg'])
        row2_frame.pack(fill=tk.X, pady=3)

        # 左侧：仅原始值列
        left_row2 = tk.Frame(row2_frame, bg=self.colors['card_bg'])
        left_row2.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Label(
            left_row2,
            text="📝 仅原始值:",
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=12,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 5))

        original_entry = tk.Entry(
            left_row2,
            textvariable=self.original_only_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        original_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 右侧：自定义分隔符列
        right_row2 = tk.Frame(row2_frame, bg=self.colors['card_bg'])
        right_row2.pack(side=tk.LEFT, fill=tk.X, expand=True)

        tk.Label(
            right_row2,
            text="🔧 自定义分隔符:",
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=14,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 5))

        custom_entry = tk.Entry(
            right_row2,
            textvariable=self.custom_separator_columns_var,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        custom_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 第四步：选择输出文件
        step4 = self.create_step_card(main_frame, "步骤 4: 选择输出文件", "指定最终对比结果的保存路径")
        step4.pack(fill=tk.X, pady=(0, 10))

        output_frame = tk.Frame(step4, bg=self.colors['card_bg'])
        output_frame.pack(fill=tk.X, padx=15, pady=8)

        tk.Label(
            output_frame,
            text="📁 输出文件:",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(side=tk.LEFT, padx=(0, 10))

        output_entry = tk.Entry(
            output_frame,
            textvariable=self.output_file,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['input_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['input_border'],
            highlightcolor=self.colors['input_focus']
        )
        output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Button(
            output_frame,
            text="💾 浏览...",
            command=self.browse_output_file,
            font=("Microsoft YaHei UI", 9, "bold"),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=20,
            pady=8,
            bd=0,
            activebackground=self.colors['accent_hover'],
            activeforeground='white'
        ).pack(side=tk.LEFT)

        # 第五步：进度和控制
        step5 = self.create_step_card(main_frame, "步骤 5: 执行合并", "点击开始按钮执行合并操作")
        step5.pack(fill=tk.X, pady=(0, 10))

        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            step5,
            variable=self.progress_var,
            maximum=100,
            style='Custom.Horizontal.TProgressbar'
        )
        self.progress_bar.pack(fill=tk.X, padx=15, pady=(8, 4))

        # 状态标签
        self.status_label = tk.Label(
            step5,
            text="等待开始...",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        )
        self.status_label.pack(pady=(0, 8))

        # 开始按钮
        button_frame = tk.Frame(step5, bg=self.colors['card_bg'])
        button_frame.pack(pady=(0, 8))

        self.start_button = tk.Button(
            button_frame,
            text="🚀 开始合并",
            command=self.start_merge,
            font=("Microsoft YaHei UI", 13, "bold"),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=50,
            pady=12,
            bd=0,
            activebackground=self.colors['success_hover'],
            activeforeground='white'
        )
        self.start_button.pack()

        # 第六步：日志输出
        step6 = self.create_step_card(main_frame, "执行日志", "显示详细的执行过程")
        step6.pack(fill=tk.BOTH, expand=True)

        # 创建日志文本框
        log_frame = tk.Frame(step6, bg=self.colors['card_bg'])
        log_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)

        # 添加滚动条
        scrollbar = tk.Scrollbar(log_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text = tk.Text(
            log_frame,
            font=("Consolas", 10),
            bg='#1e1e1e',
            fg='#00ff00',
            insertbackground='white',
            relief=tk.SOLID,
            bd=1,
            yscrollcommand=scrollbar.set,
            wrap=tk.WORD,
            height=10,
            padx=10,
            pady=10
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.log_text.yview)

    def create_step_card(self, parent, title, description):
        """创建步骤卡片"""
        # 外层容器（用于阴影效果）
        outer_container = tk.Frame(parent, bg=self.colors['bg'])
        outer_container.pack(fill=tk.X, pady=(0, 10))

        # 内层容器（卡片）
        card = tk.Frame(
            outer_container,
            bg=self.colors['card_bg'],
            relief=tk.FLAT,
            bd=0,
            highlightthickness=1,
            highlightbackground=self.colors['border']
        )
        card.pack(fill=tk.X, padx=2, pady=2)

        # 标题栏
        header = tk.Frame(card, bg=self.colors['accent'], height=45)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        # 标题容器
        title_container = tk.Frame(header, bg=self.colors['accent'])
        title_container.pack(expand=True, fill='both', padx=15, pady=8)

        tk.Label(
            title_container,
            text=title,
            font=("Microsoft YaHei UI", 11, "bold"),
            bg=self.colors['accent'],
            fg='white'
        ).pack(side=tk.LEFT)

        tk.Label(
            title_container,
            text=f"  {description}",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['accent'],
            fg='#e0e7ff'
        ).pack(side=tk.LEFT)

        # 内容区域
        content = tk.Frame(card, bg=self.colors['card_bg'])
        content.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        return content

    def create_directory_selector(self, parent, label_text, variable, command):
        """创建目录选择器"""
        frame = tk.Frame(parent, bg=self.colors['card_bg'])
        frame.pack(fill=tk.X, pady=5)

        tk.Label(
            frame,
            text=label_text,
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text'],
            width=20,
            anchor='w'
        ).pack(side=tk.LEFT, padx=(0, 10))

        entry = tk.Entry(
            frame,
            textvariable=variable,
            font=("Microsoft YaHei UI", 9),
            bg='white',
            relief=tk.FLAT,
            bd=5
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        tk.Button(
            frame,
            text="选择目录",
            command=command,
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15
        ).pack(side=tk.LEFT)

        return frame

    def create_footer(self):
        """创建页脚"""
        footer = tk.Frame(self.root, bg=self.colors['header_bg'], height=55)
        footer.pack(fill=tk.X, side=tk.BOTTOM)
        footer.pack_propagate(False)

        footer_content = tk.Frame(footer, bg=self.colors['header_bg'])
        footer_content.pack(expand=True)

        # 左侧信息
        info_frame = tk.Frame(footer_content, bg=self.colors['header_bg'])
        info_frame.pack(side=tk.LEFT, padx=20)

        tk.Label(
            info_frame,
            text="✓ 支持格式: CSV, Excel (.xlsx, .xls)",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['header_bg'],
            fg=self.colors['header_fg']
        ).pack(side=tk.LEFT, padx=10)

        # 右侧按钮
        btn_frame = tk.Frame(footer_content, bg=self.colors['header_bg'])
        btn_frame.pack(side=tk.RIGHT, padx=20)

        tk.Button(
            btn_frame,
            text="📜 历史记录",
            font=("Microsoft YaHei UI", 9),
            bg='#5B7CFA',
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=5,
            bd=0,
            activebackground='#4A6BFA',
            command=self.show_history_dialog
        ).pack(side=tk.LEFT, padx=5)

        tk.Button(
            btn_frame,
            text="🗑️ 清空历史",
            font=("Microsoft YaHei UI", 9),
            bg='#E74C3C',
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=5,
            bd=0,
            activebackground='#C0392B',
            command=self.clear_history
        ).pack(side=tk.LEFT, padx=5)

    def browse_left_dir(self):
        """浏览左侧目录"""
        # 获取初始目录：优先使用已选择的目录，否则使用当前目录
        initial_dir = self.left_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        directory = filedialog.askdirectory(
            title="选择左侧数据目录",
            initialdir=initial_dir
        )
        if directory:
            self.left_dir.set(directory)
            self.log(f"📁 左侧目录: {directory}")
            # 自动加载目录中的文件
            self.load_directory_files(directory, 'left')

    def browse_right_dir(self):
        """浏览右侧目录"""
        # 获取初始目录：优先使用已选择的目录，否则使用当前目录
        initial_dir = self.right_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        directory = filedialog.askdirectory(
            title="选择右侧数据目录",
            initialdir=initial_dir
        )
        if directory:
            self.right_dir.set(directory)
            self.log(f"📁 右侧目录: {directory}")
            # 自动加载目录中的文件
            self.load_directory_files(directory, 'right')

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

        # 清空列表
        listbox.delete(0, tk.END)
        file_list.clear()

        # 添加文件
        for file_path in files:
            filename = os.path.basename(file_path)
            listbox.insert(tk.END, filename)
            file_list.append(file_path)

        self.log(f"  自动加载了 {len(files)} 个文件到{'左侧' if side == 'left' else '右侧'}列表")

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
                'custom_separator_columns': self.custom_separator_columns_var.get()
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
        """加载历史记录"""
        try:
            # 加载配置
            config = record.get('config', {})
            if config:
                self.left_suffix.set(config.get('left_suffix', 'gauss'))
                self.right_suffix.set(config.get('right_suffix', 'sr'))
                self.numeric_columns_var.set(config.get('numeric_columns', ''))
                self.original_only_columns_var.set(config.get('original_only_columns', ''))
                self.custom_separator_columns_var.set(config.get('custom_separator_columns', ''))

            # 加载文件
            left_paths = record.get('left_paths', [])
            right_paths = record.get('right_paths', [])

            # 清空现有文件
            self.clear_files('left')
            self.clear_files('right')

            # 加载左侧文件
            for file_path in left_paths:
                if os.path.exists(file_path):
                    if file_path not in self.left_files:
                        filename = os.path.basename(file_path)
                        self.left_file_listbox.insert(tk.END, filename)
                        self.left_files.append(file_path)

            # 加载右侧文件
            for file_path in right_paths:
                if os.path.exists(file_path):
                    if file_path not in self.right_files:
                        filename = os.path.basename(file_path)
                        self.right_file_listbox.insert(tk.END, filename)
                        self.right_files.append(file_path)

            # 加载输出文件
            output_file = record.get('output_file', '')
            if output_file:
                self.output_file.set(output_file)

            self.log(f"✓ 已加载历史记录: {record.get('timestamp', '')}")
            messagebox.showinfo("成功", "历史记录加载成功！")

        except Exception as e:
            self.log(f"✗ 加载历史记录失败: {e}")
            messagebox.showerror("错误", f"加载历史记录失败:\n{e}")

    def show_history_dialog(self):
        """显示历史记录对话框"""
        # 创建历史记录窗口
        history_window = tk.Toplevel(self.root)
        history_window.title("历史记录")
        history_window.geometry("900x600")
        history_window.configure(bg=self.colors['bg'])
        history_window.transient(self.root)
        history_window.grab_set()

        # 标题
        header = tk.Frame(history_window, bg=self.colors['header_bg'], height=60)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(
            header,
            text="📜 历史记录",
            font=("Microsoft YaHei UI", 16, "bold"),
            bg=self.colors['header_bg'],
            fg=self.colors['header_fg']
        ).pack(pady=15)

        # 历史记录列表
        list_frame = tk.Frame(history_window, bg=self.colors['card_bg'])
        list_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 创建滚动区域
        canvas = tk.Canvas(list_frame, bg=self.colors['card_bg'], highlightthickness=0)
        scrollbar = tk.Scrollbar(list_frame, orient=tk.VERTICAL, command=canvas.yview)

        scrollable_frame = tk.Frame(canvas, bg=self.colors['card_bg'])
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 显示历史记录
        records = self.history.get('records', [])

        if not records:
            tk.Label(
                scrollable_frame,
                text="暂无历史记录",
                font=("Microsoft YaHei UI", 12),
                bg=self.colors['card_bg'],
                fg=self.colors['text_secondary']
            ).pack(pady=50)
        else:
            for i, record in enumerate(records):
                self.create_history_item(scrollable_frame, record, i, history_window)

        # 关闭按钮
        btn_frame = tk.Frame(history_window, bg=self.colors['bg'])
        btn_frame.pack(fill=tk.X, padx=20, pady=(0, 20))

        tk.Button(
            btn_frame,
            text="关闭",
            font=("Microsoft YaHei UI", 10),
            bg=self.colors['accent'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=30,
            pady=8,
            bd=0,
            command=history_window.destroy
        ).pack()

    def create_history_item(self, parent, record, index, window):
        """创建历史记录项"""
        item_frame = tk.Frame(
            parent,
            bg=self.colors['card_bg'],
            relief=tk.SOLID,
            bd=1,
            highlightbackground=self.colors['border'],
            highlightthickness=1
        )
        item_frame.pack(fill=tk.X, padx=10, pady=8)

        # 时间戳
        timestamp = record.get('timestamp', '未知时间')
        tk.Label(
            item_frame,
            text=f"📅 {timestamp}",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(anchor='w', padx=15, pady=(10, 5))

        # 文件信息
        left_files = record.get('left_files', [])
        right_files = record.get('right_files', [])

        info_frame = tk.Frame(item_frame, bg=self.colors['card_bg'])
        info_frame.pack(fill=tk.X, padx=15, pady=5)

        # 左侧文件
        tk.Label(
            info_frame,
            text=f"🔵 左侧 ({len(left_files)} 个):",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w')

        tk.Label(
            info_frame,
            text=", ".join(left_files[:3]) + ("..." if len(left_files) > 3 else ""),
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(anchor='w', padx=(20, 0))

        # 右侧文件
        tk.Label(
            info_frame,
            text=f"🔴 右侧 ({len(right_files)} 个):",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['card_bg'],
            fg=self.colors['text_secondary']
        ).pack(anchor='w', pady=(5, 0))

        tk.Label(
            info_frame,
            text=", ".join(right_files[:3]) + ("..." if len(right_files) > 3 else ""),
            font=("Microsoft YaHei UI", 8),
            bg=self.colors['card_bg'],
            fg=self.colors['text']
        ).pack(anchor='w', padx=(20, 0))

        # 按钮
        btn_frame = tk.Frame(item_frame, bg=self.colors['card_bg'])
        btn_frame.pack(fill=tk.X, padx=15, pady=(10, 10))

        tk.Button(
            btn_frame,
            text="📂 加载此记录",
            font=("Microsoft YaHei UI", 9),
            bg=self.colors['success'],
            fg='white',
            relief=tk.FLAT,
            cursor='hand2',
            padx=15,
            pady=5,
            bd=0,
            activebackground=self.colors['success_hover'],
            command=lambda: self.load_history_record(record)
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
            command=lambda: self.delete_history_record(index, window)
        ).pack(side=tk.LEFT)

    def delete_history_record(self, index, window):
        """删除历史记录"""
        if messagebox.askyesno("确认", "确定要删除这条历史记录吗？"):
            try:
                del self.history['records'][index]
                self.save_history()
                window.destroy()
                self.show_history_dialog()  # 重新显示对话框
                self.log("🗑️  已删除历史记录")
            except Exception as e:
                self.log(f"✗ 删除历史记录失败: {e}")
                messagebox.showerror("错误", f"删除失败:\n{e}")

    def clear_history(self):
        """清空所有历史记录"""
        if messagebox.askyesno("确认", "确定要清空所有历史记录吗？\n此操作不可恢复！"):
            try:
                self.history['records'] = []
                self.save_history()
                self.log("🗑️  已清空所有历史记录")
                messagebox.showinfo("成功", "历史记录已清空！")
            except Exception as e:
                self.log(f"✗ 清空历史记录失败: {e}")
                messagebox.showerror("错误", f"清空失败:\n{e}")

    def add_left_files(self):
        """添加左侧文件"""
        # 获取初始目录
        initial_dir = self.left_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            # 如果左侧目录未设置，尝试使用当前目录
            initial_dir = os.getcwd()

        files = filedialog.askopenfilenames(
            title="选择左侧数据文件",
            initialdir=initial_dir,
            filetypes=[
                ("支持的文件", "*.csv;*.xlsx;*.xls"),
                ("CSV文件", "*.csv"),
                ("Excel文件", "*.xlsx;*.xls"),
                ("所有文件", "*.*")
            ]
        )
        if files:
            for file_path in files:
                if file_path not in self.left_files:
                    filename = os.path.basename(file_path)
                    self.left_file_listbox.insert(tk.END, filename)
                    self.left_files.append(file_path)
            self.log(f"📄 添加了 {len(files)} 个左侧文件")

    def add_right_files(self):
        """添加右侧文件"""
        # 获取初始目录
        initial_dir = self.right_dir.get()
        if not initial_dir or not os.path.isdir(initial_dir):
            # 如果右侧目录未设置，尝试使用当前目录
            initial_dir = os.getcwd()

        files = filedialog.askopenfilenames(
            title="选择右侧数据文件",
            initialdir=initial_dir,
            filetypes=[
                ("支持的文件", "*.csv;*.xlsx;*.xls"),
                ("CSV文件", "*.csv"),
                ("Excel文件", "*.xlsx;*.xls"),
                ("所有文件", "*.*")
            ]
        )
        if files:
            for file_path in files:
                if file_path not in self.right_files:
                    filename = os.path.basename(file_path)
                    self.right_file_listbox.insert(tk.END, filename)
                    self.right_files.append(file_path)
            self.log(f"📄 添加了 {len(files)} 个右侧文件")

    def remove_left_files(self):
        """移除选中的左侧文件"""
        selection = self.left_file_listbox.curselection()
        if selection:
            # 从后往前删除，避免索引变化
            for idx in reversed(selection):
                self.left_file_listbox.delete(idx)
                del self.left_files[idx]
            self.log(f"🗑️  移除了 {len(selection)} 个左侧文件")

    def remove_right_files(self):
        """移除选中的右侧文件"""
        selection = self.right_file_listbox.curselection()
        if selection:
            # 从后往前删除，避免索引变化
            for idx in reversed(selection):
                self.right_file_listbox.delete(idx)
                del self.right_files[idx]
            self.log(f"🗑️  移除了 {len(selection)} 个右侧文件")

    def clear_left_files(self):
        """清空左侧所有文件"""
        self.left_file_listbox.delete(0, tk.END)
        self.left_files.clear()
        self.log(f"🗑️  清空了所有左侧文件")

    def clear_right_files(self):
        """清空右侧所有文件"""
        self.right_file_listbox.delete(0, tk.END)
        self.right_files.clear()
        self.log(f"🗑️  清空了所有右侧文件")

    def browse_output_file(self):
        """浏览输出文件"""
        # 获取初始目录：从当前输出文件路径中提取目录
        current_output = self.output_file.get()
        initial_dir = os.path.dirname(current_output)
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.getcwd()

        # 获取默认文件名
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

    def open_file(self, file_path):
        """打开文件（使用系统默认程序）"""
        try:
            if platform.system() == 'Windows':
                os.startfile(file_path)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.call(['open', file_path])
            else:  # Linux
                subprocess.call(['xdg-open', file_path])
            self.log(f"📂 已打开文件: {file_path}")
        except Exception as e:
            self.log(f"⚠️  无法打开文件: {e}")
            messagebox.showwarning("警告", f"无法打开文件:\n{e}")

    def update_progress(self, value, status):
        """更新进度"""
        self.progress_var.set(value)
        self.status_label.config(text=status)
        self.root.update()

    def get_test_files(self, directory):
        """获取目录中的所有测试文件"""
        supported_extensions = ['.csv', '.xlsx', '.xls']
        files = []

        for ext in supported_extensions:
            pattern = os.path.join(directory, f"*{ext}")
            files.extend(glob.glob(pattern))

        return sorted(files)

    def merge_files(self, files, output_path):
        """合并多个文件（参考merge_test_results.py的逻辑）"""
        dfs = []
        for file_path in files:
            try:
                df = read_file(file_path)
                self.log(f"  ✓ 读取成功: {os.path.basename(file_path)} ({len(df)} 行)")

                # 检查是否需要根据 OriginFileName 拆分
                if 'OriginFileName' in df.columns:
                    # 找出所有拆分点（即 OriginFileName 第一次重复的位置）
                    seen_files = set()
                    split_indices = []

                    for idx, origin_file in enumerate(df['OriginFileName']):
                        if origin_file in seen_files:
                            split_indices.append(idx)
                            # 重置，继续查找下一个拆分点
                            seen_files = set([origin_file])
                        else:
                            seen_files.add(origin_file)

                    if split_indices:
                        # 需要拆分
                        self.log(f"    → 检测到 {len(split_indices) + 1} 组重复的 OriginFileName，进行拆分...")

                        start_idx = 0
                        for i, split_idx in enumerate(split_indices):
                            end_idx = split_idx
                            df_part = df.iloc[start_idx:end_idx].copy()
                            dfs.append(df_part)
                            self.log(f"      - 第 {i+1} 部分: {len(df_part)} 行")
                            start_idx = split_idx

                        # 添加最后一部分
                        df_last = df.iloc[start_idx:].copy()
                        dfs.append(df_last)
                        self.log(f"      - 第 {len(split_indices) + 1} 部分: {len(df_last)} 行")
                    else:
                        # 没有重复，直接添加
                        dfs.append(df)
                else:
                    # 没有 OriginFileName 列，直接添加
                    dfs.append(df)

            except Exception as e:
                self.log(f"  ✗ 读取失败: {os.path.basename(file_path)} - {e}")
                raise

        if len(dfs) < 2:
            raise ValueError("至少需要2个测试数据才能进行合并")

        # 显示拆分后的DataFrame信息
        self.log(f"\n  共 {len(dfs)} 个 DataFrame 参与合并")
        for i, df in enumerate(dfs):
            unique_files = df['OriginFileName'].nunique() if 'OriginFileName' in df.columns else 1
            self.log(f"    - DataFrame {i+1}: {len(df)} 行, {unique_files} 个不同的 OriginFileName")

        # 获取行数
        num_rows = len(dfs[0])

        # 从界面获取列配置
        # 数值列配置
        numeric_columns_str = self.numeric_columns_var.get().strip()
        numeric_columns = [col.strip() for col in numeric_columns_str.split(',') if col.strip()] if numeric_columns_str else []

        # 只合并原始值的列
        original_only_columns_str = self.original_only_columns_var.get().strip()
        original_only_columns = [col.strip() for col in original_only_columns_str.split(',') if col.strip()] if original_only_columns_str else []

        # 自定义分隔符的列
        custom_separator_columns_str = self.custom_separator_columns_var.get().strip()
        custom_separator_columns = {}
        if custom_separator_columns_str:
            for item in custom_separator_columns_str.split(','):
                if ':' in item:
                    col, sep = item.split(':', 1)
                    custom_separator_columns[col.strip()] = sep.strip()

        # 基础列（固定）
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

        # 处理只合并原始值的列
        for col in original_only_columns:
            original_values = []
            for i in range(num_rows):
                vals = []
                for df in dfs:
                    v = df[col].values[i]
                    if pd.notna(v) and v != '' and v != 'TraceBack':
                        vals.append(str(v))
                original_values.append('/'.join(vals))
            result_df[f'{col}_Original'] = original_values

        # 处理自定义分隔符的列
        for col, separator in custom_separator_columns.items():
            original_values = []
            for i in range(num_rows):
                vals = []
                for df in dfs:
                    v = df[col].values[i]
                    if pd.notna(v) and v != '' and v != 'TraceBack':
                        vals.append(str(v))
                original_values.append(separator.join(vals))
            result_df[f'{col}_Original'] = original_values

        # 计算TotalTime浮动
        time_float_values = []
        for i in range(num_rows):
            times = []
            for df in dfs:
                from merge_test_results import extract_number
                t = extract_number(df['TotalTimeCount'].values[i])
                if t is not None:
                    times.append(t)

            if len(times) >= 2:
                time_diff = max(times) - min(times)
                time_float_values.append(f"{time_diff:.1f}s")
            else:
                time_float_values.append('')

        result_df['TotalTime_Float'] = time_float_values

        # 计算Memory增长
        memory_growth_values = []
        for i in range(num_rows):
            growth_values = []

            for df in dfs:
                max_mem_val = df['MaxMemory'].values[i]
                start_mem_val = df['StartMemory'].values[i]

                if pd.notna(max_mem_val) and pd.notna(start_mem_val) and max_mem_val != 'TraceBack' and start_mem_val != 'TraceBack':
                    max_mem_num, max_mem_unit = extract_number_with_unit(max_mem_val)
                    start_mem_num, start_mem_unit = extract_number_with_unit(start_mem_val)

                    if max_mem_num is not None and start_mem_num is not None:
                        max_mem_mb = max_mem_num * 1024 if max_mem_unit == 'G' else max_mem_num
                        start_mem_mb = start_mem_num * 1024 if start_mem_unit == 'G' else start_mem_num

                        growth = max_mem_mb - start_mem_mb
                        growth_values.append(growth)

            if growth_values:
                avg_growth = sum(growth_values) / len(growth_values)
                if avg_growth >= 1024:
                    memory_growth_values.append(f"{avg_growth/1024:.1f}G")
                else:
                    memory_growth_values.append(f"{avg_growth:.0f}M")
            else:
                memory_growth_values.append('')

        result_df['Memory_Growth_Avg'] = memory_growth_values

        # 保存中间结果
        save_file(result_df, output_path)
        return result_df

    def compare_results(self, left_df, right_df, output_path):
        """对比两个合并后的结果"""
        # 获取配置的后缀
        left_suffix = self.left_suffix.get().strip()
        right_suffix = self.right_suffix.get().strip()

        # 验证后缀
        if not left_suffix or not right_suffix:
            raise ValueError("左右后缀不能为空")

        self.log(f"\n🏷️  使用列后缀: 左侧='{left_suffix}', 右侧='{right_suffix}'")

        # 匹配列
        match_columns = ['OriginFileName', 'Type', 'Level']
        base_columns = ['OriginFileName', 'PreferenceStdFileName', 'Type', 'Level', 'Env&Ver']

        # 添加调试信息
        self.log(f"\n📊 左表行数: {len(left_df)}, 右表行数: {len(right_df)}")

        # 检查匹配键的唯一性
        for df_name, df in [("左表", left_df), ("右表", right_df)]:
            key_combo = df[match_columns].apply(lambda row: '|'.join(str(v) for v in row), axis=1)
            unique_count = key_combo.nunique()
            total_count = len(df)
            self.log(f"  {df_name}: 匹配键组合数 = {unique_count}, 总行数 = {total_count}")
            if unique_count < total_count:
                self.log(f"  ⚠️  {df_name}存在重复的匹配键，可能导致数据膨胀")

        # 重命名左表列
        left_columns = left_df.columns.tolist()
        left_new_columns = {}
        for col in left_columns:
            if col not in base_columns:
                left_new_columns[col] = f"{col}_{left_suffix}"
        left_df_renamed = left_df.rename(columns=left_new_columns)

        # 重命名右表列
        right_columns = right_df.columns.tolist()
        right_new_columns = {}
        for col in right_columns:
            if col not in base_columns:
                right_new_columns[col] = f"{col}_{right_suffix}"
        right_df_renamed = right_df.rename(columns=right_new_columns)

        # 执行merge（使用 inner join 确保只匹配两边都存在的记录）
        self.log(f"\n🔗 执行 merge 操作...")

        try:
            merged_df = pd.merge(
                left_df_renamed,
                right_df_renamed,
                on=match_columns,
                how='inner',  # 改为 inner join，只保留两边都匹配的行
                validate='one_to_one'  # 验证一对一关系
            )
        except pd.errors.MergeError as e:
            self.log(f"  ⚠️  一对一验证失败: {e}")
            self.log(f"  🔄 降级为普通 merge（可能产生数据膨胀）")
            merged_df = pd.merge(
                left_df_renamed,
                right_df_renamed,
                on=match_columns,
                how='inner'
            )

        self.log(f"  ✓ Merge 后行数: {len(merged_df)}")

        # 检查是否有行数膨胀
        if len(merged_df) > max(len(left_df), len(right_df)):
            self.log(f"  ⚠️  警告: Merge 后行数({len(merged_df)})大于原始数据行数，可能存在匹配键重复")

        # 重新排列列顺序
        ordered_columns = []

        # 基础列
        for col in base_columns:
            if col in merged_df.columns:
                ordered_columns.append(col)

        # 左表列（保持原顺序）
        for col in left_columns:
            if col not in base_columns:
                renamed_col = f"{col}_{left_suffix}"
                if renamed_col in merged_df.columns:
                    ordered_columns.append(renamed_col)

        # 右表所有列（保持原顺序）
        for col in right_columns:
            if col not in base_columns:
                renamed_col = f"{col}_{right_suffix}"
                if renamed_col in merged_df.columns and renamed_col not in ordered_columns:
                    ordered_columns.append(renamed_col)

        merged_df = merged_df[ordered_columns]
        save_file(merged_df, output_path)
        return merged_df

    def start_merge(self):
        """开始合并流程"""
        # 验证输入
        has_left = len(self.left_files) > 0
        has_right = len(self.right_files) > 0

        if not has_left and not has_right:
            messagebox.showerror("错误", "请至少添加一个文件！")
            return

        # 清空日志
        self.log_text.delete(1.0, tk.END)

        # 禁用按钮
        self.start_button.config(state=tk.DISABLED, text="处理中...")

        # 在新线程中执行
        thread = Thread(target=self.execute_merge)
        thread.start()

    def execute_merge(self):
        """执行合并操作（在后台线程中）"""
        try:
            # 检查有哪些数据
            has_left = len(self.left_files) > 0
            has_right = len(self.right_files) > 0
            both_sides = has_left and has_right

            # 步骤1: 获取文件列表
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

            # 判断执行模式
            if not both_sides:
                # 单边合并模式
                self.log("\n📝 检测到只有单边数据，执行单边合并模式")
                self.log("=" * 60)

                files_to_merge = left_files if has_left else right_files
                side_label = "左侧" if has_left else "右侧"
                suffix_label = self.left_suffix.get() if has_left else self.right_suffix.get()

                self.update_progress(20, f"正在合并{side_label}数据...")

                self.log(f"\n🔹 合并{side_label}数据 ({suffix_label})")
                self.log("=" * 60)

                merged_result = self.merge_files(files_to_merge, self.output_file.get())
                self.log(f"✓ {side_label}数据合并完成: {len(merged_result)} 行, {len(merged_result.columns)} 列")

                self.update_progress(100, "处理完成！")

                # 完成
                self.log("\n" + "=" * 60)
                self.log("🎉 处理完成！")
                self.log("=" * 60)
                self.log(f"📊 结果已保存到: {self.output_file.get()}")
                self.log(f"📈 总行数: {len(merged_result)}")
                self.log(f"📋 总列数: {len(merged_result.columns)}")

                # 询问是否打开文件
                result = messagebox.askyesno(
                    "成功",
                    f"{side_label}数据合并完成！\n\n结果已保存到:\n{self.output_file.get()}\n\n是否立即打开结果文件？",
                    icon=messagebox.INFO
                )
                if result:
                    self.open_file(self.output_file.get())

                # 保存到历史记录
                try:
                    self.add_to_history(files_to_merge if side_label == "左侧" else [], [], self.output_file.get())
                except Exception as e:
                    self.log(f"⚠️  保存历史记录失败: {e}")
            else:
                # 双边对比模式
                self.update_progress(10, "正在合并左侧数据...")

                # 步骤2: 合并左侧文件
                self.log("\n" + "=" * 60)
                left_suffix_label = self.left_suffix.get()
                self.log(f"🔵 步骤 2: 合并左侧数据 ({left_suffix_label})")
                self.log("=" * 60)

                left_temp = os.path.join(os.path.dirname(self.output_file.get()), "_temp_left_merge.csv")
                left_merged = self.merge_files(left_files, left_temp)
                self.log(f"✓ 左侧数据合并完成: {len(left_merged)} 行")

                self.update_progress(40, "正在合并右侧数据...")

                # 步骤3: 合并右侧文件
                self.log("\n" + "=" * 60)
                right_suffix_label = self.right_suffix.get()
                self.log(f"🔴 步骤 3: 合并右侧数据 ({right_suffix_label})")
                self.log("=" * 60)

                right_temp = os.path.join(os.path.dirname(self.output_file.get()), "_temp_right_merge.csv")
                right_merged = self.merge_files(right_files, right_temp)
                self.log(f"✓ 右侧数据合并完成: {len(right_merged)} 行")

                self.update_progress(70, "正在对比合并结果...")

                # 步骤4: 对比两个结果
                self.log("\n" + "=" * 60)
                self.log("⚖️  步骤 4: 对比两个合并结果")
                self.log("=" * 60)

                final_result = self.compare_results(left_merged, right_merged, self.output_file.get())
                self.log(f"✓ 对比完成: {len(final_result)} 行, {len(final_result.columns)} 列")

                self.update_progress(100, "处理完成！")

                # 清理临时文件
                try:
                    os.remove(left_temp)
                    os.remove(right_temp)
                except:
                    pass

                # 完成
                self.log("\n" + "=" * 60)
                self.log("🎉 处理完成！")
                self.log("=" * 60)
                self.log(f"📊 结果已保存到: {self.output_file.get()}")
                self.log(f"📈 总行数: {len(final_result)}")
                self.log(f"📋 总列数: {len(final_result.columns)}")

                # 询问是否打开文件
                result = messagebox.askyesno(
                    "成功",
                    f"合并对比完成！\n\n结果已保存到:\n{self.output_file.get()}\n\n是否立即打开结果文件？",
                    icon=messagebox.INFO
                )
                if result:
                    self.open_file(self.output_file.get())

                # 保存到历史记录
                try:
                    self.add_to_history(left_files, right_files, self.output_file.get())
                except Exception as e:
                    self.log(f"⚠️  保存历史记录失败: {e}")

        except Exception as e:
            self.log(f"\n❌ 错误: {str(e)}")
            messagebox.showerror("错误", f"处理失败:\n{str(e)}")

        finally:
            # 恢复按钮
            self.start_button.config(state=tk.NORMAL, text="🚀 开始合并")


def main():
    root = tk.Tk()
    app = MergeGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()
