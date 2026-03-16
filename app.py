import sys
import os
import json
import sqlite3
import asyncio
import math
import shutil
import io
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
import scipy.stats as si
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QSplitter, QTreeWidget, QTreeWidgetItem, QTextEdit, 
    QLabel, QPushButton, QGroupBox, QSpinBox, QDoubleSpinBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QComboBox, QSlider,
    QLineEdit, QInputDialog, QDialog, QFormLayout, QDialogButtonBox, QGridLayout,
    QSizePolicy, QMenu, QDockWidget, QCheckBox
)
from PyQt6.QtCore import Qt, pyqtSignal, QObject, QTimer, QPointF, QRectF
from PyQt6.QtGui import QFont, QColor, QPalette, QPainter, QPicture, QPen, QAction
import pyqtgraph as pg
from ib_insync import IB, util, Contract, Stock, ComboLeg, LimitOrder, Index, Option
import qasync

# --- GOOGLE DRIVE IMPORTS ---
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False

# ==============================================================================
# --- CUSTOM UI COMPONENTS ---
# ==============================================================================
class SortableTreeWidgetItem(QTreeWidgetItem):
    def __lt__(self, other):
        column = self.treeWidget().sortColumn()
        text1 = self.text(column)
        text2 = other.text(column)
        try: return float(text1.replace('$', '').replace(',', '').replace('%', '').strip()) < float(text2.replace('$', '').replace(',', '').replace('%', '').strip())
        except ValueError: return text1 < text2

class GroupStrategyDialog(QDialog):
    def __init__(self, default_ticker, existing_strategies, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create Strategy Combo")
        self.setMinimumWidth(350)
        self.setStyleSheet("QDialog { background-color: #0d1117; color: #c9d1d9; } QLineEdit, QComboBox { background: #010409; color: #c9d1d9; border: 1px solid #30363d; padding: 6px; font-weight: bold;} QLabel { color: #8b949e; font-weight: bold; }")
        layout = QFormLayout(self)
        self.ticker_input = QLineEdit(default_ticker)
        layout.addRow("Underlying:", self.ticker_input)
        self.strategy_input = QComboBox()
        self.strategy_input.setEditable(True)
        self.strategy_input.addItems(existing_strategies)
        layout.addRow("Strategy Group:", self.strategy_input)
        self.combo_input = QLineEdit()
        layout.addRow("Combo Name:", self.combo_input)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)
    def get_data(self): return self.ticker_input.text().upper().strip(), self.strategy_input.currentText().strip(), self.combo_input.text().strip()

class EmittingStream(QObject):
    textWritten = pyqtSignal(str)
    def write(self, text): self.textWritten.emit(str(text))
    def flush(self): pass

# ==============================================================================
# --- NATIVE CANDLESTICK CHARTING ENGINE ---
# ==============================================================================
class TVViewBox(pg.ViewBox):
    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self.setMouseMode(self.PanMode)

class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data
        self.generatePicture()
    def generatePicture(self):
        self.picture = QPicture()
        p = QPainter(self.picture)
        p.setPen(pg.mkPen('#8b949e', width=1))
        w = 86400 * 0.2
        for (t, open_price, close_price, low_price, high_price) in self.data:
            p.drawLine(QPointF(t, low_price), QPointF(t, high_price))
            p.setBrush(pg.mkBrush('#f85149' if open_price > close_price else '#3fb950'))
            p.drawRect(QRectF(t - w, open_price, w * 2, close_price - open_price))
        p.end()
    def paint(self, p, *args): p.drawPicture(0, 0, self.picture)
    def boundingRect(self): return QRectF(self.picture.boundingRect())

# ==============================================================================
# --- MAIN APPLICATION WINDOW ---
# ==============================================================================
class BVSLaunchpad(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BVS Launchpad - Trading OS")
        self.setMinimumSize(1200, 800)
        self.setGeometry(50, 50, 1600, 950)
        
        self.ib = IB()
        self.db_conn = None
        self.current_df = None 
        self.working_basket = [] 
        self.chain_strikes = []
        self.current_chain_spot = 0.0 
        self.current_risk_data = None 
        self.active_chain_contracts = [] 
        self.active_strategies_file = os.path.join(os.path.dirname(__file__), "active_strategies.json")
        self.drive_service = None

        self.apply_theme()
        self.init_database()
        self.build_ui()
        
        sys.stdout = EmittingStream(textWritten=self.normal_output_written)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] BVS Launchpad Initialized.")
        QTimer.singleShot(500, self.connect_to_ibkr)

    def apply_theme(self):
        self.setStyleSheet("""
            * { font-family: Inter, Roboto, Helvetica, Arial, sans-serif; }
            QMainWindow { background-color: #0B0E11; } 
            QWidget { color: #FFFFFF; background-color: #0B0E11; }
            QTabWidget::pane { border: 1px solid #1C212A; background: #151921; border-radius: 4px; }
            QTabBar::tab { background: #151921; border: 1px solid #1C212A; padding: 6px 14px; color: #90A4AE; font-weight: bold; font-size: 13px; border-top-left-radius: 4px; border-top-right-radius: 4px;}
            QTabBar::tab:selected { background: #0B0E11; color: #26A69A; border-bottom: 2px solid #26A69A; }
            QTreeWidget, QTableWidget { background-color: #0B0E11; color: #FFFFFF; gridline-color: #1C212A; border: 1px solid #1C212A; font-size: 12px; alternate-background-color: #151921;}
            QTreeWidget::item, QTableWidget::item { padding: 4px; border-bottom: 1px solid #1C212A; }
            QHeaderView::section { background-color: #151921; color: #90A4AE; padding: 6px; border: 1px solid #1C212A; font-weight: bold; text-align: center;}
            QTextEdit { background-color: #151921; border: 1px solid #1C212A; color: #FFFFFF; border-radius: 4px;}
            QPushButton { background-color: #151921; color: #FFFFFF; font-weight: bold; border: 1px solid #1C212A; padding: 6px 12px; border-radius: 4px; }
            QPushButton:hover { background-color: #26A69A; color: #0B0E11; }
            QPushButton:pressed { background-color: #1E8278; border-style: inset;}
            QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox { background: #151921; color: #FFFFFF; border: 1px solid #1C212A; padding: 4px; font-weight: bold; border-radius: 4px; }
            QSplitter::handle { background-color: #1C212A; }
            QDockWidget { color: #FFFFFF; font-weight: bold; font-size: 13px; }
            QDockWidget::title { background: #151921; padding: 6px; border: 1px solid #1C212A; border-radius: 4px;}
            QSlider::groove:horizontal { border: 1px solid #1C212A; height: 4px; background: #151921; margin: 2px 0; border-radius: 2px;}
            QSlider::handle:horizontal { background: #26A69A; border: 1px solid #FFFFFF; width: 14px; height: 14px; margin: -5px 0; border-radius: 7px; }
            QGroupBox { border: 1px solid #1C212A; font-weight: bold; color: #FFFFFF; margin-top: 10px; border-radius: 4px;}
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; color: #90A4AE;}
        """)

    def build_ui(self):
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        main_layout = QVBoxLayout(self.central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.tab_workspace = QWidget()
        self.tab_charts = QWidget()
        self.tab_quant = QWidget()
        self.tab_cloud = QWidget()

        self.tabs.addTab(self.tab_workspace, "Master Workspace (Portfolio + Builder)")
        self.tabs.addTab(self.tab_charts, "Technical Charts")
        self.tabs.addTab(self.tab_quant, "Quant Analytics")
        self.tabs.addTab(self.tab_cloud, "Cloud Sync")

        self.build_master_workspace()
        self.build_technical_charts()
        self.build_quant_analytics()
        self.build_cloud_sync()
        
        self.console_output = QTextEdit()
        self.console_output.setReadOnly(True)
        self.console_output.setFixedHeight(100)
        main_layout.addWidget(self.console_output)

    # --------------------------------------------------------------------------
    # --- PHASE 1 & 3: UNIFIED MASTER WORKSPACE (DOCK ENGINE) ---
    # --------------------------------------------------------------------------
    def build_master_workspace(self):
        layout = QVBoxLayout(self.tab_workspace)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.workspace_mw = QMainWindow()
        self.workspace_mw.setDockNestingEnabled(True)
        self.workspace_mw.setStyleSheet("QMainWindow { background-color: #0B0E11; }")
        layout.addWidget(self.workspace_mw)
        dock_features = QDockWidget.DockWidgetFeature.DockWidgetFloatable | QDockWidget.DockWidgetFeature.DockWidgetMovable

        # ==========================================
        # DOCK 1: STRATEGY NAVIGATOR (Portfolio)
        # ==========================================
        self.dock_port = QDockWidget("Strategy Hierarchy", self.workspace_mw)
        self.dock_port.setFeatures(dock_features)
        
        port_widget = QWidget(); port_widget.setMinimumWidth(350)
        port_layout = QVBoxLayout(port_widget)
        port_layout.setContentsMargins(5, 5, 5, 5)
        
        ribbon_layout = QHBoxLayout()
        lbl_account = QLabel("Account:")
        lbl_account.setStyleSheet("font-size: 14px; font-weight: bold; color: #8b949e;")
        
        self.account_selector = QComboBox()
        self.account_selector.currentTextChanged.connect(self.refresh_portfolio_grid)
        btn_refresh_port = QPushButton("REFRESH")
        btn_refresh_port.setStyleSheet("background-color: #00C853; color: #0B0E11; border: none;")
        btn_refresh_port.clicked.connect(self.refresh_portfolio_grid)
        
        ribbon_layout.addWidget(lbl_account)
        ribbon_layout.addWidget(self.account_selector)
        ribbon_layout.addWidget(btn_refresh_port)
        ribbon_layout.addStretch()
        port_layout.addLayout(ribbon_layout)
        
        filter_layout = QHBoxLayout()
        self.filter_ticker = QLineEdit(); self.filter_ticker.setPlaceholderText("Filter Ticker..."); self.filter_ticker.textChanged.connect(self.filter_portfolio_tree)
        self.filter_expiry = QLineEdit(); self.filter_expiry.setPlaceholderText("Expiry..."); self.filter_expiry.textChanged.connect(self.filter_portfolio_tree)
        self.filter_strike = QLineEdit(); self.filter_strike.setPlaceholderText("Strike..."); self.filter_strike.textChanged.connect(self.filter_portfolio_tree)
        filter_layout.addWidget(self.filter_ticker); filter_layout.addWidget(self.filter_expiry); filter_layout.addWidget(self.filter_strike)
        port_layout.addLayout(filter_layout)
        
        # --- SGPV Dashboard ---
        self.sgpv_dashboard = QWidget()
        self.sgpv_dashboard.setStyleSheet("QWidget { background-color: #151921; border-radius: 4px; border: 1px solid #30363d; margin-top: 5px; margin-bottom: 5px; } QLabel { border: none; font-size: 11px; font-weight: bold; color: #90A4AE; }")
        sgpv_layout = QHBoxLayout(self.sgpv_dashboard)
        sgpv_layout.setContentsMargins(10, 5, 10, 5)
        
        self.lbl_net_liq = QLabel("NET LIQ: ---")
        self.lbl_sgpv_opt = QLabel("OPT SGPV: ---")
        self.lbl_sgpv_stk = QLabel("STK SGPV: ---")
        self.lbl_sgpv_ratio = QLabel("RATIO: ---")
        
        for lbl in [self.lbl_net_liq, self.lbl_sgpv_opt, self.lbl_sgpv_stk, self.lbl_sgpv_ratio]:
            sgpv_layout.addWidget(lbl)
            
        port_layout.addWidget(self.sgpv_dashboard)
        
        strat_btns = QHBoxLayout()
        btn_group = QPushButton("📂 GROUP"); btn_group.setStyleSheet("background-color: #1f6feb; color: white;"); btn_group.clicked.connect(self.group_selected_legs)
        btn_ungroup = QPushButton("✖ UNG"); btn_ungroup.setStyleSheet("background-color: #30363d; color: #c9d1d9;"); btn_ungroup.clicked.connect(self.ungroup_selected_legs)
        btn_expand = QPushButton("⮟ EXP"); btn_expand.setStyleSheet("background-color: #30363d; color: #c9d1d9;"); btn_expand.clicked.connect(lambda: self.portfolio_tree.expandAll())
        btn_collapse = QPushButton("⮝ COL"); btn_collapse.setStyleSheet("background-color: #30363d; color: #c9d1d9;"); btn_collapse.clicked.connect(lambda: self.portfolio_tree.collapseAll())
        strat_btns.addWidget(btn_group); strat_btns.addWidget(btn_ungroup); strat_btns.addWidget(btn_expand); strat_btns.addWidget(btn_collapse); strat_btns.addStretch()
        port_layout.addLayout(strat_btns)
        
        self.portfolio_tree = QTreeWidget()
        self.portfolio_tree.setHeaderLabels(["Position", "Qty", "Cost/Price", "Delta", "Theta", "Vega", "Realized", "Velocity", "AI Rec"])
        self.portfolio_tree.setIndentation(15) 
        self.portfolio_tree.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        header = self.portfolio_tree.header()
        header.setStretchLastSection(False) 
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive); self.portfolio_tree.setColumnWidth(0, 300)
        
        for col in range(1, 8):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive); self.portfolio_tree.setColumnWidth(col, 75)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.Interactive); self.portfolio_tree.setColumnWidth(8, 150)
            
        self.portfolio_tree.setAlternatingRowColors(True)
        self.portfolio_tree.setSortingEnabled(True) 
        self.portfolio_tree.itemClicked.connect(self.on_portfolio_item_clicked)
        self.portfolio_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.portfolio_tree.customContextMenuRequested.connect(self.show_portfolio_context_menu)
        port_layout.addWidget(self.portfolio_tree)
        
        btn_analyze = QPushButton("📈 MODEL SELECTED LEGS IN BASKET")
        btn_analyze.setStyleSheet("background-color: #c084fc; color: #ffffff; font-weight: bold; padding: 10px; border: 1px solid #a855f7;")
        btn_analyze.clicked.connect(self.analyze_portfolio_selection)
        port_layout.addWidget(btn_analyze)
        
        self.dock_port.setWidget(port_widget)
        self.workspace_mw.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.dock_port)

        # ==========================================
        # DOCK 2: OPTION CHAIN
        # ==========================================
        self.dock_chain = QDockWidget("Live Option Chain", self.workspace_mw)
        self.dock_chain.setFeatures(dock_features)
        chain_widget = QWidget()
        chain_layout = QVBoxLayout(chain_widget)
        chain_layout.setContentsMargins(5, 5, 5, 5)
        
        chain_ribbon = QHBoxLayout()
        lbl_chain = QLabel("Underlying:")
        lbl_chain.setStyleSheet("font-size: 14px; font-weight: bold; color: #8b949e;")
        self.chain_ticker_in = QLineEdit(); self.chain_ticker_in.setPlaceholderText("SPX"); self.chain_ticker_in.setMinimumWidth(80); self.chain_ticker_in.returnPressed.connect(self.fetch_expirations)
        btn_get_exp = QPushButton("GET CHAIN"); btn_get_exp.setStyleSheet("background-color: #30363d; color: white;"); btn_get_exp.clicked.connect(self.fetch_expirations)
        self.exp_combo = QComboBox(); self.exp_combo.setMinimumWidth(120)
        btn_load_chain = QPushButton("LOAD"); btn_load_chain.setStyleSheet("background-color: #1f6feb; color: white;"); btn_load_chain.clicked.connect(self.load_option_chain)
        self.lbl_chain_spot = QLabel("SPOT: ---")
        self.lbl_chain_spot.setStyleSheet("font-size: 16px; font-weight: bold; color: #d2a8ff; padding-left: 20px;")
        
        chain_ribbon.addWidget(lbl_chain); chain_ribbon.addWidget(self.chain_ticker_in); chain_ribbon.addWidget(btn_get_exp)
        chain_ribbon.addWidget(QLabel("DTE:")); chain_ribbon.addWidget(self.exp_combo); chain_ribbon.addWidget(btn_load_chain)
        self.inp_chain_strikes = QSpinBox(); self.inp_chain_strikes.setRange(2, 200); self.inp_chain_strikes.setValue(20); self.inp_chain_strikes.setMaximumWidth(50)
        chain_ribbon.addWidget(QLabel("Strikes:")); chain_ribbon.addWidget(self.inp_chain_strikes)
        chain_ribbon.addWidget(self.lbl_chain_spot); chain_ribbon.addStretch()
        chain_layout.addLayout(chain_ribbon)
        
        self.chain_table = QTableWidget()
        self.chain_table.setColumnCount(11)
        self.chain_table.setHorizontalHeaderLabels(["C Delta", "C Vol", "C OI", "CALL BID", "CALL ASK", "STRIKE", "PUT BID", "PUT ASK", "P Vol", "P OI", "P Delta"])
        self.chain_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.chain_table.horizontalHeader().setMinimumSectionSize(60)
        self.chain_table.verticalHeader().setVisible(False)
        self.chain_table.setAlternatingRowColors(True)
        self.chain_table.cellDoubleClicked.connect(self.on_chain_clicked)
        chain_layout.addWidget(self.chain_table)
        self.dock_chain.setWidget(chain_widget)
        self.workspace_mw.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.dock_chain)
        self.workspace_mw.tabifyDockWidget(self.dock_port, self.dock_chain)
        self.dock_chain.raise_()

        # ==========================================
        # DOCK 3: UNIFIED RISK PROFILE
        # ==========================================
        self.dock_risk = QDockWidget("Multi-Timeframe Risk Profile", self.workspace_mw)
        self.dock_risk.setFeatures(dock_features)
        risk_widget = QWidget(); risk_widget.setMinimumWidth(500); risk_layout = QVBoxLayout(risk_widget); risk_layout.setContentsMargins(5, 5, 5, 5)
        
        self.lbl_combo_name = QLabel("Selected Combo: None")
        self.lbl_combo_name.setStyleSheet("font-size: 14px; font-weight: bold; color: #e3b341;")
        risk_layout.addWidget(self.lbl_combo_name)

        greeks_layout = QHBoxLayout()
        self.lbl_agg_delta = QLabel("Delta: ---"); self.lbl_agg_gamma = QLabel("Gamma: ---"); self.lbl_agg_theta = QLabel("Theta: ---")
        self.lbl_agg_vega  = QLabel("Vega: ---"); self.lbl_agg_pnl   = QLabel("Net PnL: ---")
        font = QFont("Helvetica", 12, QFont.Weight.Bold)
        for lbl in [self.lbl_agg_delta, self.lbl_agg_gamma, self.lbl_agg_theta, self.lbl_agg_vega, self.lbl_agg_pnl]:
            lbl.setFont(font); lbl.setStyleSheet("color: #8b949e;")
            greeks_layout.addWidget(lbl)
        greeks_layout.addStretch()
        risk_layout.addLayout(greeks_layout)
        
        self.port_risk_plot = pg.PlotWidget(); self.port_risk_plot.setBackground('#0B0E11'); self.port_risk_plot.showGrid(x=False, y=False)
        self.port_risk_plot.getAxis('bottom').setPen('#6b7280'); self.port_risk_plot.getAxis('left').setPen('#6b7280')
        self.port_risk_plot.setLabel('left', 'PnL ($)'); self.port_risk_plot.setLabel('bottom', 'Underlying Price'); self.port_risk_plot.addLegend()
        
        self.risk_vLine = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('#26A69A', width=1.5, style=Qt.PenStyle.DashLine))
        self.port_risk_plot.addItem(self.risk_vLine, ignoreBounds=True)
        
        self.risk_inspector = pg.TextItem(color='#FFFFFF', fill=pg.mkBrush(21, 25, 33, 230), border=pg.mkPen('#26A69A', width=1))
        self.risk_inspector.setZValue(100)
        self.port_risk_plot.addItem(self.risk_inspector, ignoreBounds=True)
        self.risk_inspector.hide()
        
        self.risk_proxy = pg.SignalProxy(self.port_risk_plot.scene().sigMouseMoved, rateLimit=60, slot=self.risk_mouse_moved)
        risk_layout.addWidget(self.port_risk_plot, stretch=1)
        
        # --- OPTIONSTRAT STYLE: WHAT-IF SLIDERS ---
        self.slider_layout = QHBoxLayout()
        
        self.btn_reset_sliders = QPushButton("♻️ Reset")
        self.btn_reset_sliders.setStyleSheet("background-color: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 4px; font-size: 11px;")
        self.btn_reset_sliders.setMaximumWidth(60)
        self.btn_reset_sliders.clicked.connect(lambda: [self.slider_iv.setValue(0), self.slider_dte.setValue(0), self.inp_t_lines.setValue(4)])

        self.lbl_t_lines = QLabel("T+ Lines:")
        self.lbl_t_lines.setStyleSheet("font-family: Menlo, monospace; font-size: 13px; font-weight: bold; color: #8b949e;")
        self.inp_t_lines = QSpinBox()
        self.inp_t_lines.setRange(0, 10); self.inp_t_lines.setValue(4)
        self.inp_t_lines.setMaximumWidth(50)
        self.inp_t_lines.valueChanged.connect(lambda _: asyncio.create_task(self._async_update_risk_graph()))

        self.lbl_iv_adj = QLabel("IV Adj: +0%")
        self.lbl_iv_adj.setStyleSheet("font-family: Menlo, monospace; font-size: 13px; font-weight: bold; color: #8b949e;")
        self.slider_iv = QSlider(Qt.Orientation.Horizontal)
        self.slider_iv.setRange(-50, 50); self.slider_iv.setValue(0)
        self.slider_iv.valueChanged.connect(self.on_iv_slider_changed)
        
        self.lbl_dte_adj = QLabel("Days Fwd: +0")
        self.lbl_dte_adj.setStyleSheet("font-family: Menlo, monospace; font-size: 13px; font-weight: bold; color: #8b949e;")
        self.slider_dte = QSlider(Qt.Orientation.Horizontal)
        self.slider_dte.setRange(0, 100); self.slider_dte.setValue(0)
        self.slider_dte.valueChanged.connect(self.on_dte_slider_changed)
        
        for widget in [self.lbl_t_lines, self.inp_t_lines, self.lbl_iv_adj, self.slider_iv, self.lbl_dte_adj, self.slider_dte, self.btn_reset_sliders]:
            self.slider_layout.addWidget(widget)
        
        risk_layout.addLayout(self.slider_layout)
        
        self.dock_risk.setWidget(risk_widget)
        self.workspace_mw.addDockWidget(Qt.DockWidgetArea.RightDockWidgetArea, self.dock_risk)

        # ==========================================
        # DOCK 4: EXECUTION DESK
        # ==========================================
        self.dock_exec = QDockWidget("Working Basket & Execution", self.workspace_mw)
        self.dock_exec.setFeatures(dock_features)
        exec_widget = QWidget(); exec_widget.setMinimumWidth(450); exec_layout = QVBoxLayout(exec_widget); exec_layout.setContentsMargins(5, 5, 5, 5)

        strat_builder_layout = QHBoxLayout()
        strat_builder_layout.addWidget(QLabel("1-Click Build:"))
        self.btn_build_straddle = QPushButton("Straddle")
        self.btn_build_strangle = QPushButton("Strangle")
        self.btn_build_iron_condor = QPushButton("Iron Condor")
        self.btn_build_vert_call = QPushButton("Call Vert")
        self.btn_build_vert_put = QPushButton("Put Vert")
        for btn in [self.btn_build_straddle, self.btn_build_strangle, self.btn_build_iron_condor, self.btn_build_vert_call, self.btn_build_vert_put]:
            btn.setStyleSheet("background-color: #30363d; color: #c9d1d9; font-weight: bold; padding: 5px;")
            strat_builder_layout.addWidget(btn)
        
        self.btn_build_straddle.clicked.connect(lambda: self.build_template_strategy('straddle'))
        self.btn_build_strangle.clicked.connect(lambda: self.build_template_strategy('strangle'))
        self.btn_build_iron_condor.clicked.connect(lambda: self.build_template_strategy('iron_condor'))
        self.btn_build_vert_call.clicked.connect(lambda: self.build_template_strategy('call_vert'))
        self.btn_build_vert_put.clicked.connect(lambda: self.build_template_strategy('put_vert'))
        exec_layout.addLayout(strat_builder_layout)
        
        eq_trade_layout = QHBoxLayout()
        eq_trade_layout.addWidget(QLabel("Underlying Trade:"))
        self.btn_buy_ul = QPushButton("BUY")
        self.btn_buy_ul.setStyleSheet("background-color: #3fb950; color: white; font-weight: bold;")
        self.btn_sell_ul = QPushButton("SELL")
        self.btn_sell_ul.setStyleSheet("background-color: #f85149; color: white; font-weight: bold;")
        self.inp_ul_qty = QSpinBox()
        self.inp_ul_qty.setRange(1, 1000000); self.inp_ul_qty.setValue(100)
        eq_trade_layout.addWidget(self.btn_buy_ul); eq_trade_layout.addWidget(self.btn_sell_ul); eq_trade_layout.addWidget(QLabel("Qty:")); eq_trade_layout.addWidget(self.inp_ul_qty); eq_trade_layout.addStretch()
        exec_layout.addLayout(eq_trade_layout)

        self.btn_buy_ul.clicked.connect(lambda: self.add_underlying_to_basket('BUY'))
        self.btn_sell_ul.clicked.connect(lambda: self.add_underlying_to_basket('SELL'))
        
        self.basket_table = QTableWidget()
        self.basket_table.setColumnCount(6)
        self.basket_table.setHorizontalHeaderLabels(["Action", "Qty", "Type", "Strike", "Price", "Ext."])
        self.basket_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.basket_table.verticalHeader().setVisible(False)
        exec_layout.addWidget(self.basket_table, stretch=2)
        
        b_btns = QHBoxLayout()
        btn_clear = QPushButton("CLEAR BASKET"); btn_clear.setStyleSheet("background-color: #30363d; color: white;"); btn_clear.clicked.connect(self.clear_basket)
        b_btns.addWidget(btn_clear)
        exec_layout.addLayout(b_btns)
        
        self.lbl_price_quotes = QLabel("Bid: ---  |  Mid: ---  |  Ask: ---")
        self.lbl_price_quotes.setStyleSheet("font-size: 14px; font-weight: bold; color: #d2a8ff; margin-top: 10px;")
        exec_layout.addWidget(self.lbl_price_quotes)
        
        self.lbl_net_price = QLabel("Net Price: $0.00")
        self.lbl_net_price.setStyleSheet("font-size: 16px; font-weight: bold;")
        exec_layout.addWidget(self.lbl_net_price)
        
        tabs_exec = QTabWidget()
        tabs_exec.setStyleSheet("QTabBar::tab { padding: 4px; font-size: 11px; }")
        
        # TAB 1: Quick Limit
        tab_quick = QWidget(); qt_layout = QVBoxLayout(tab_quick)
        qt_layout.setContentsMargins(0, 5, 0, 0)
        
        exec_ctrl_layout = QHBoxLayout()
        exec_ctrl_layout.addWidget(QLabel("Qty:"))
        self.inp_exec_qty = QSpinBox(); self.inp_exec_qty.setMinimum(1); self.inp_exec_qty.setValue(1)
        exec_ctrl_layout.addWidget(self.inp_exec_qty)
        
        exec_ctrl_layout.addWidget(QLabel("Limit:"))
        self.inp_exec_lmt = QDoubleSpinBox(); self.inp_exec_lmt.setRange(-9999.0, 9999.0); self.inp_exec_lmt.setDecimals(2)
        exec_ctrl_layout.addWidget(self.inp_exec_lmt)
        
        self.combo_tif = QComboBox(); self.combo_tif.addItems(["DAY", "GTC"])
        self.chk_outside_rth = QCheckBox("Fill outside RTH")
        exec_ctrl_layout.addWidget(QLabel("TIF:")); exec_ctrl_layout.addWidget(self.combo_tif); exec_ctrl_layout.addWidget(self.chk_outside_rth)
        qt_layout.addLayout(exec_ctrl_layout)
        
        btn_transmit = QPushButton("TRANSMIT LIMIT ORDER")
        btn_transmit.setStyleSheet("background-color: #da3633; color: white; font-weight: bold; padding: 10px; border-radius: 4px;")
        btn_transmit.clicked.connect(self.transmit_basket_order)
        qt_layout.addWidget(btn_transmit)
        tabs_exec.addTab(tab_quick, "Limit Order")
        
        # TAB 2: Walk-Limit Machine
        tab_walk = QWidget(); w_layout = QVBoxLayout(tab_walk)
        w_layout.setContentsMargins(0, 5, 0, 0)
        walk_ctrls = QGridLayout()
        
        walk_ctrls.addWidget(QLabel("Start Price:"), 0, 0)
        self.inp_walk_start = QDoubleSpinBox(); self.inp_walk_start.setRange(-9999.0, 9999.0); self.inp_walk_start.setDecimals(2)
        walk_ctrls.addWidget(self.inp_walk_start, 0, 1)
        
        walk_ctrls.addWidget(QLabel("End Price:"), 0, 2)
        self.inp_walk_end = QDoubleSpinBox(); self.inp_walk_end.setRange(-9999.0, 9999.0); self.inp_walk_end.setDecimals(2)
        walk_ctrls.addWidget(self.inp_walk_end, 0, 3)
        
        walk_ctrls.addWidget(QLabel("Steps:"), 1, 0)
        self.inp_walk_steps = QSpinBox(); self.inp_walk_steps.setRange(2, 100); self.inp_walk_steps.setValue(5)
        walk_ctrls.addWidget(self.inp_walk_steps, 1, 1)
        
        walk_ctrls.addWidget(QLabel("Secs/Step:"), 1, 2)
        self.inp_walk_time = QSpinBox(); self.inp_walk_time.setRange(1, 3600); self.inp_walk_time.setValue(10)
        walk_ctrls.addWidget(self.inp_walk_time, 1, 3)
        
        self.lbl_walk_impact = QLabel("Step Impact: --- (---%)")
        self.lbl_walk_impact.setStyleSheet("color: #e3b341; font-weight: bold; font-size: 11px;")
        walk_ctrls.addWidget(self.lbl_walk_impact, 2, 0, 1, 4)
        
        def update_walk_impact():
            steps = self.inp_walk_steps.value()
            rng = abs(self.inp_walk_end.value() - self.inp_walk_start.value())
            if steps > 1 and rng > 0:
                step_sz = rng / (steps - 1)
                pct = (step_sz / rng) * 100
                self.lbl_walk_impact.setText(f"Step Size: {step_sz:.2f} ({pct:.1f}%)")
            else:
                self.lbl_walk_impact.setText("Step Size: --- (---%)")
        
        self.inp_walk_start.valueChanged.connect(lambda _: update_walk_impact())
        self.inp_walk_end.valueChanged.connect(lambda _: update_walk_impact())
        self.inp_walk_steps.valueChanged.connect(lambda _: update_walk_impact())
        
        w_layout.addLayout(walk_ctrls)
        
        self.btn_walk = QPushButton("START WALK-LIMIT")
        self.btn_walk.setStyleSheet("background-color: #26A69A; color: white; font-weight: bold; padding: 10px; border-radius: 4px;")
        self.btn_walk.clicked.connect(self.start_walk_order)
        w_layout.addWidget(self.btn_walk)
        
        tabs_exec.addTab(tab_walk, "Walk-Limit OS")
        
        exec_layout.addWidget(tabs_exec)
        
        self.dock_exec.setWidget(exec_widget)
        self.workspace_mw.splitDockWidget(self.dock_risk, self.dock_exec, Qt.Orientation.Vertical)

        self.workspace_mw.resizeDocks([self.dock_chain, self.dock_risk], [400, 800], Qt.Orientation.Horizontal)
        self.workspace_mw.resizeDocks([self.dock_risk, self.dock_exec], [600, 300], Qt.Orientation.Vertical)

    # --------------------------------------------------------------------------
    # --- PHASE 2: TECHNICAL CHARTS ---
    # --------------------------------------------------------------------------
    def build_technical_charts(self):
        layout = QVBoxLayout(self.tab_charts)
        ribbon_layout = QHBoxLayout()
        lbl_chart = QLabel("Load Ticker:")
        lbl_chart.setStyleSheet("font-size: 16px; font-weight: bold; color: #8b949e;")
        
        self.ticker_input = QLineEdit(); self.ticker_input.setPlaceholderText("e.g., SPY, AAPL"); self.ticker_input.setMinimumWidth(150); self.ticker_input.returnPressed.connect(self.load_candlestick_chart)
        btn_load_chart = QPushButton("LOAD CHART"); btn_load_chart.setStyleSheet("background-color: #1f6feb; color: white;"); btn_load_chart.clicked.connect(self.load_candlestick_chart)
        btn_reset_zoom = QPushButton("RESET ZOOM"); btn_reset_zoom.setStyleSheet("background-color: #30363d; color: #c9d1d9;"); btn_reset_zoom.clicked.connect(self.reset_chart_view)
        
        ribbon_layout.addWidget(lbl_chart); ribbon_layout.addWidget(self.ticker_input); ribbon_layout.addWidget(btn_load_chart); ribbon_layout.addWidget(btn_reset_zoom)
        
        self.chk_sma = QCheckBox("SMA 20"); self.chk_sma.setChecked(True); self.chk_sma.stateChanged.connect(self.toggle_indicators)
        self.chk_ema = QCheckBox("EMA 50"); self.chk_ema.setChecked(True); self.chk_ema.stateChanged.connect(self.toggle_indicators)
        self.chk_bb = QCheckBox("Bollinger Bands"); self.chk_bb.setChecked(True); self.chk_bb.stateChanged.connect(self.toggle_indicators)
        
        ribbon_layout.addSpacing(20)
        ribbon_layout.addWidget(self.chk_sma); ribbon_layout.addWidget(self.chk_ema); ribbon_layout.addWidget(self.chk_bb)
        ribbon_layout.addStretch()
        layout.addLayout(ribbon_layout)
        
        self.chart_ribbon_widget = QWidget(); self.chart_ribbon_widget.setStyleSheet("background: #161b22; border: 1px solid #30363d;")
        ribbon_vbox = QVBoxLayout(self.chart_ribbon_widget); ribbon_vbox.setContentsMargins(5, 5, 5, 5); ribbon_vbox.setSpacing(2)
        
        self.chart_label_row1 = QLabel("Hover over the chart to see OHLCV data."); self.chart_label_row2 = QLabel("")
        for lbl in [self.chart_label_row1, self.chart_label_row2]:
            lbl.setStyleSheet("font-family: Helvetica; font-size: 13px; color: #c9d1d9; border: none; background: transparent;")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); ribbon_vbox.addWidget(lbl)
        layout.addWidget(self.chart_ribbon_widget)
        
        date_axis = pg.DateAxisItem(orientation='bottom')
        self.chart_plot = pg.PlotWidget(axisItems={'bottom': date_axis}, viewBox=TVViewBox())
        self.chart_plot.setBackground('#0d1117'); self.chart_plot.showGrid(x=False, y=False); self.chart_plot.addLegend(offset=(10, 10))
        self.chart_plot.showAxis('right'); self.chart_plot.hideAxis('left'); self.chart_plot.getAxis('right').setLabel('Price ($)'); self.chart_plot.getAxis('right').setWidth(60)
        self.chart_plot.getAxis('bottom').setPen('#6b7280'); self.chart_plot.getAxis('right').setPen('#6b7280')
        self.chart_plot.setMouseEnabled(x=True, y=True); self.chart_plot.getViewBox().enableAutoRange(axis=pg.ViewBox.YAxis); self.chart_plot.getViewBox().setAutoVisible(y=True)
        
        self.volume_vb = pg.ViewBox()
        self.chart_plot.scene().addItem(self.volume_vb)
        self.volume_vb.setXLink(self.chart_plot.getViewBox()); self.volume_vb.setMouseEnabled(x=False, y=False) 
        self.chart_plot.getViewBox().sigResized.connect(lambda: [self.volume_vb.setGeometry(self.chart_plot.getViewBox().sceneBoundingRect()), self.volume_vb.linkedViewChanged(self.chart_plot.getViewBox(), self.volume_vb.XAxis)])
        layout.addWidget(self.chart_plot)

        self.vLine = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('#8b949e', width=1, style=Qt.PenStyle.DashLine))
        self.hLine = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('#8b949e', width=1, style=Qt.PenStyle.DashLine), label='{value:.2f}', labelOpts={'position': 0.98, 'color': '#c9d1d9', 'fill': '#161b22', 'anchor': (0, 0.5)})
        self.vLine_lbl = pg.TextItem(color='#c9d1d9', fill='#161b22', anchor=(0.5, 1))
        self.current_price_line = pg.InfiniteLine(angle=0, movable=False, label='{value:.2f}', labelOpts={'position': 0.98, 'color': '#ffffff', 'anchor': (0, 0.5)})
        
        for item in [self.vLine, self.hLine, self.vLine_lbl, self.current_price_line]: self.chart_plot.addItem(item, ignoreBounds=True)
        self.proxy = pg.SignalProxy(self.chart_plot.scene().sigMouseMoved, rateLimit=60, slot=self.mouse_moved)

    def toggle_indicators(self):
        if hasattr(self, 'plot_sma') and self.plot_sma is not None: self.plot_sma.setVisible(self.chk_sma.isChecked())
        if hasattr(self, 'plot_ema') and self.plot_ema is not None: self.plot_ema.setVisible(self.chk_ema.isChecked())
        if hasattr(self, 'plot_bb_up') and self.plot_bb_up is not None: self.plot_bb_up.setVisible(self.chk_bb.isChecked())
        if hasattr(self, 'plot_bb_dn') and self.plot_bb_dn is not None: self.plot_bb_dn.setVisible(self.chk_bb.isChecked())

    def mouse_moved(self, evt):
        if self.chart_plot.sceneBoundingRect().contains(evt[0]):
            mousePoint = self.chart_plot.getViewBox().mapSceneToView(evt[0])
            self.vLine.setPos(mousePoint.x()); self.hLine.setPos(mousePoint.y())
            self.vLine_lbl.setText(datetime.fromtimestamp(mousePoint.x()).strftime('%Y-%m-%d'))
            self.vLine_lbl.setPos(mousePoint.x(), self.chart_plot.getViewBox().viewRect().bottom())
            
            if hasattr(self, 'current_df') and self.current_df is not None and not self.current_df.empty:
                idx = np.searchsorted(self.current_df['timestamp'].values, mousePoint.x())
                if 0 < idx < len(self.current_df['timestamp'].values):
                    if abs(self.current_df['timestamp'].values[idx] - mousePoint.x()) > abs(self.current_df['timestamp'].values[idx-1] - mousePoint.x()): idx -= 1
                    row = self.current_df.iloc[idx]
                    self.chart_label_row1.setText(f"<b>Date:</b> <span style='color:#58a6ff'>{datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d')}</span>   |   <b>O:</b> <span style='color:#c9d1d9'>{row['open']:.2f}</span>   |   <b>H:</b> <span style='color:#3fb950'>{row['high']:.2f}</span>")
                    self.chart_label_row2.setText(f"<b>L:</b> <span style='color:#f85149'>{row['low']:.2f}</span>   |   <b>C:</b> <span style='color:#c9d1d9'>{row['close']:.2f}</span>   |   <b>Vol:</b> <span style='color:#e3b341'>{row['volume']:,.0f}</span>")

    def load_candlestick_chart(self):
        if self.ticker_input.text().upper().strip() and self.ib.isConnected(): asyncio.create_task(self._async_load_chart(self.ticker_input.text().upper().strip()))
        
    async def _async_load_chart(self, ticker):
        try:
            contract = Stock(ticker, 'SMART', 'USD')
            if ticker in ['SPX', 'VIX']: contract = Index(ticker, 'CBOE', 'USD')
            elif ticker in ['NDX']: contract = Index(ticker, 'NASDAQ', 'USD')
            elif ticker in ['RUT']: contract = Index(ticker, 'RUSSELL', 'USD')
            await self.ib.qualifyContractsAsync(contract)
            
            bars = await self.ib.reqHistoricalDataAsync(contract, endDateTime='', durationStr='1 Y', barSizeSetting='1 day', whatToShow='TRADES', useRTH=True, formatDate=1)
            if not bars: return
                
            self.current_df = pd.DataFrame([{'timestamp': (datetime.combine(b.date, datetime.min.time()) if isinstance(b.date, date) and not isinstance(b.date, datetime) else b.date).timestamp(), 'open': b.open, 'close': b.close, 'low': b.low, 'high': b.high, 'volume': float(b.volume) if b.volume else 0.0} for b in bars])
            df = self.current_df
            df['sma20'] = df['close'].rolling(window=20).mean()
            df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
            df['std20'] = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['sma20'] + (2 * df['std20'])
            df['bb_lower'] = df['sma20'] - (2 * df['std20'])
            
            self.chart_plot.clear()
            self.chart_plot.setTitle(f"Daily OHLC: {ticker}", color='#d2a8ff', size='14pt')
            self.chart_plot.addItem(CandlestickItem([(r['timestamp'], r['open'], r['close'], r['low'], r['high']) for _, r in df.iterrows()]))
            
            df_clean_sma = df.dropna(subset=['sma20'])
            df_clean_ema = df.dropna(subset=['ema50'])
            self.plot_sma = self.chart_plot.plot(df_clean_sma['timestamp'].values, df_clean_sma['sma20'].values, pen=pg.mkPen('#e3b341', width=2), name="SMA 20")
            self.plot_ema = self.chart_plot.plot(df_clean_ema['timestamp'].values, df_clean_ema['ema50'].values, pen=pg.mkPen('#388bfd', width=2), name="EMA 50")
            self.plot_bb_up = self.chart_plot.plot(df_clean_sma['timestamp'].values, df_clean_sma['bb_upper'].values, pen=pg.mkPen('#8b949e', width=1, style=Qt.PenStyle.DashLine), name="BB Up")
            self.plot_bb_dn = self.chart_plot.plot(df_clean_sma['timestamp'].values, df_clean_sma['bb_lower'].values, pen=pg.mkPen('#8b949e', width=1, style=Qt.PenStyle.DashLine), name="BB Dn")
            
            self.toggle_indicators() 
            
            for item in [self.vLine, self.hLine, self.vLine_lbl, self.current_price_line]: self.chart_plot.addItem(item, ignoreBounds=True)
            line_color = '#3fb950' if df.iloc[-1]['close'] >= df.iloc[-1]['open'] else '#f85149'
            self.current_price_line.setPen(pg.mkPen(line_color, width=1.5, style=Qt.PenStyle.DashLine))
            self.current_price_line.label.fill = pg.mkBrush(line_color)
            self.current_price_line.setPos(df.iloc[-1]['close'])
            
            self.volume_vb.clear()
            if df['volume'].max() > 0: self.volume_vb.setYRange(0, df['volume'].max() * 4)
            if (df['close'] >= df['open']).any(): self.volume_vb.addItem(pg.BarGraphItem(x=df['timestamp'][df['close'] >= df['open']].values, height=df['volume'][df['close'] >= df['open']].values, width=86400 * 0.4, brush=pg.mkBrush(63, 185, 80, 100), pen=pg.mkPen(63, 185, 80, 100)))
            if (df['close'] < df['open']).any(): self.volume_vb.addItem(pg.BarGraphItem(x=df['timestamp'][df['close'] < df['open']].values, height=df['volume'][df['close'] < df['open']].values, width=86400 * 0.4, brush=pg.mkBrush(248, 81, 73, 100), pen=pg.mkPen(248, 81, 73, 100)))
            self.reset_chart_view()
        except Exception as e: print(f"[CHART ERROR] Failed to load data for {ticker}: {e}")

    def reset_chart_view(self): self.chart_plot.autoRange()

    # --------------------------------------------------------------------------
    # --- PHASE 4: QUANT ANALYTICS TAB ---
    # --------------------------------------------------------------------------
    def build_quant_analytics(self):
        layout = QVBoxLayout(self.tab_quant)
        toolbar = QHBoxLayout()
        self.quant_ticker = QLineEdit()
        self.quant_ticker.setPlaceholderText("Enter Ticker (e.g. SPY)")
        self.quant_ticker.setMaximumWidth(150)
        self.quant_ticker.returnPressed.connect(self.run_quant_lab)
        btn_run = QPushButton("RUN QUANT LAB")
        btn_run.setStyleSheet("background-color: #1f6feb; color: white;")
        btn_run.clicked.connect(self.run_quant_lab)
        
        toolbar.addWidget(QLabel("Analyze Ticker:"))
        toolbar.addWidget(self.quant_ticker)
        toolbar.addWidget(btn_run)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.vol_plot = pg.PlotWidget(title="Volatility Lab: HV vs IV (1 Year)")
        self.vol_plot.setBackground('#0d1117'); self.vol_plot.addLegend(); self.vol_plot.setLabel('left', 'Volatility (%)'); self.vol_plot.showGrid(x=False, y=False)
        splitter.addWidget(self.vol_plot)
        
        self.cone_plot = pg.PlotWidget(title="Probability Cone (45-Day Expected Move)")
        self.cone_plot.setBackground('#0d1117'); self.cone_plot.addLegend(); self.cone_plot.setLabel('left', 'Underlying Price ($)'); self.cone_plot.setLabel('bottom', 'Days Forward'); self.cone_plot.showGrid(x=False, y=False)
        splitter.addWidget(self.cone_plot)
        layout.addWidget(splitter)
        
    def run_quant_lab(self):
        ticker = self.quant_ticker.text().upper().strip()
        if ticker and self.ib.isConnected(): asyncio.create_task(self._async_run_quant_lab(ticker))

    async def _async_run_quant_lab(self, ticker):
        try:
            contract = Stock(ticker, 'SMART', 'USD')
            if ticker in ['SPX', 'VIX']: contract = Index(ticker, 'CBOE', 'USD')
            elif ticker in ['NDX']: contract = Index(ticker, 'NASDAQ', 'USD')
            elif ticker in ['RUT']: contract = Index(ticker, 'RUSSELL', 'USD')
            await self.ib.qualifyContractsAsync(contract)
            
            bars = await self.ib.reqHistoricalDataAsync(contract, endDateTime='', durationStr='1 Y', barSizeSetting='1 day', whatToShow='TRADES', useRTH=True, formatDate=1)
            if not bars: return
                
            df = pd.DataFrame([{'date': b.date, 'close': b.close} for b in bars])
            df['pct_change'] = df['close'].pct_change()
            df['HV20'] = df['pct_change'].rolling(20).std() * math.sqrt(252) * 100
            df['HV50'] = df['pct_change'].rolling(50).std() * math.sqrt(252) * 100
            
            iv_bars = None
            try: iv_bars = await self.ib.reqHistoricalDataAsync(contract, endDateTime='', durationStr='1 Y', barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True, formatDate=1)
            except Exception: pass
                
            if iv_bars:
                iv_df = pd.DataFrame([{'date': b.date, 'IV': b.close * 100} for b in iv_bars])
                df = pd.merge(df, iv_df, on='date', how='left')
            else: df['IV'] = np.nan
                
            df = df.dropna(subset=['HV20', 'HV50'])
            x_times = [datetime.combine(d, datetime.min.time()).timestamp() if not isinstance(d, datetime) else d.timestamp() for d in df['date']]
            self.vol_plot.clear()
            self.vol_plot.setAxisItems({'bottom': pg.DateAxisItem(orientation='bottom')})
            self.vol_plot.plot(x_times, df['HV20'].values, pen=pg.mkPen('#388bfd', width=2), name="20-Day HV")
            self.vol_plot.plot(x_times, df['HV50'].values, pen=pg.mkPen('#8b949e', width=2, style=Qt.PenStyle.DashLine), name="50-Day HV")
            
            current_iv = df['HV20'].iloc[-1] 
            if 'IV' in df.columns and not df['IV'].isna().all():
                iv_clean = df.dropna(subset=['IV'])
                if not iv_clean.empty:
                    x_iv = [datetime.combine(d, datetime.min.time()).timestamp() if not isinstance(d, datetime) else d.timestamp() for d in iv_clean['date']]
                    self.vol_plot.plot(x_iv, iv_clean['IV'].values, pen=pg.mkPen('#e3b341', width=2), name="Implied Vol (IV)")
                    current_iv = iv_clean['IV'].iloc[-1]
            
            spot = df['close'].iloc[-1]
            days_forward = np.arange(0, 46)
            upper_1sd = spot * (1 + (current_iv/100) * np.sqrt(days_forward/365))
            lower_1sd = spot * (1 - (current_iv/100) * np.sqrt(days_forward/365))
            upper_2sd = spot * (1 + (current_iv/100 * 2) * np.sqrt(days_forward/365))
            lower_2sd = spot * (1 - (current_iv/100 * 2) * np.sqrt(days_forward/365))
            
            self.cone_plot.clear()
            self.cone_plot.plot(days_forward, upper_1sd, pen=pg.mkPen('#3fb950', width=2, style=Qt.PenStyle.DashLine), name="+1 SD (68%)")
            self.cone_plot.plot(days_forward, lower_1sd, pen=pg.mkPen('#f85149', width=2, style=Qt.PenStyle.DashLine), name="-1 SD (68%)")
            self.cone_plot.plot(days_forward, upper_2sd, pen=pg.mkPen('#3fb950', width=1, style=Qt.PenStyle.DotLine), name="+2 SD (95%)")
            self.cone_plot.plot(days_forward, lower_2sd, pen=pg.mkPen('#f85149', width=1, style=Qt.PenStyle.DotLine), name="-2 SD (95%)")
            self.cone_plot.plot(days_forward, np.full_like(days_forward, spot), pen=pg.mkPen('#ffffff', width=1), name="Current Spot")
            
            fill = pg.FillBetweenItem(pg.PlotCurveItem(days_forward, lower_1sd), pg.PlotCurveItem(days_forward, upper_1sd), brush=pg.mkBrush(56, 139, 253, 30))
            self.cone_plot.addItem(fill)
        except Exception as e: print(f"[QUANT ERROR] {e}")

    # --------------------------------------------------------------------------
    # --- PHASE 5: CLOUD SYNCHRONIZATION ---
    # --------------------------------------------------------------------------
    def build_cloud_sync(self):
        layout = QVBoxLayout(self.tab_cloud)
        
        ribbon = QHBoxLayout()
        self.lbl_cloud_status = QLabel("Status: 🔴 Not Connected")
        self.lbl_cloud_status.setStyleSheet("font-size: 14px; font-weight: bold;")
        
        btn_auth = QPushButton("🔑 Connect to Google Drive")
        btn_auth.clicked.connect(self.authenticate_drive)
        
        ribbon.addWidget(self.lbl_cloud_status)
        ribbon.addWidget(btn_auth)
        ribbon.addStretch()
        layout.addLayout(ribbon)
        
        sync_group = QGroupBox("Database Synchronization (trade_guardian_v4.db)")
        sync_layout = QVBoxLayout(sync_group)
        
        btn_layout = QHBoxLayout()
        self.btn_push = QPushButton("☁️ PUSH (Local -> Cloud)")
        self.btn_push.setStyleSheet("background-color: #1f6feb; color: white; font-weight: bold; padding: 15px;")
        self.btn_push.clicked.connect(self.push_to_cloud)
        self.btn_push.setEnabled(False)
        
        self.btn_pull = QPushButton("☁️ PULL (Cloud -> Local)")
        self.btn_pull.setStyleSheet("background-color: #3fb950; color: white; font-weight: bold; padding: 15px;")
        self.btn_pull.clicked.connect(self.pull_from_cloud)
        self.btn_pull.setEnabled(False)
        
        btn_layout.addWidget(self.btn_push)
        btn_layout.addWidget(self.btn_pull)
        sync_layout.addLayout(btn_layout)
        
        self.cloud_console = QTextEdit()
        self.cloud_console.setReadOnly(True)
        self.cloud_console.setStyleSheet("background-color: #010409; color: #c9d1d9;")
        sync_layout.addWidget(self.cloud_console)
        
        layout.addWidget(sync_group)
        
        if not GOOGLE_DRIVE_AVAILABLE:
            self.cloud_console.append("[ERROR] Google API libraries not found. Run: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
            btn_auth.setEnabled(False)
            
    def authenticate_drive(self):
        if not GOOGLE_DRIVE_AVAILABLE: return
        cred_path = os.path.join(os.path.dirname(__file__), "credentials.json")
        if not os.path.exists(cred_path):
            self.cloud_console.append(f"[ERROR] '{cred_path}' not found. Please place your service account JSON file in the same folder as main.py.")
            return
            
        try:
            self.cloud_console.append("[*] Authenticating with Google Drive...")
            creds = service_account.Credentials.from_service_account_file(cred_path, scopes=['https://www.googleapis.com/auth/drive'])
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.lbl_cloud_status.setText("Status: 🟢 Connected")
            self.lbl_cloud_status.setStyleSheet("color: #3fb950; font-size: 14px; font-weight: bold;")
            self.btn_push.setEnabled(True)
            self.btn_pull.setEnabled(True)
            self.cloud_console.append("[SUCCESS] Connected to Google Drive API!")
        except Exception as e:
            self.cloud_console.append(f"[ERROR] Authentication failed: {e}")

    def push_to_cloud(self):
        if self.drive_service:
            self.btn_push.setEnabled(False)
            self.cloud_console.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting PUSH to Cloud...")
            asyncio.create_task(self._async_push_to_cloud())

    async def _async_push_to_cloud(self):
        try:
            result = await asyncio.to_thread(self._blocking_push)
            self.cloud_console.append(result)
        except Exception as e:
            self.cloud_console.append(f"[ERROR] Push failed: {e}")
        finally:
            self.btn_push.setEnabled(True)

    def _blocking_push(self):
        db_name = "trade_guardian_v4.db"
        local_path = os.path.join(os.path.dirname(__file__), db_name)
        if not os.path.exists(local_path): return f"[ERROR] Local database {db_name} does not exist."
            
        query = f"name='{db_name}' and trashed=false"
        results = self.drive_service.files().list(q=query, pageSize=1, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        media = MediaFileUpload(local_path, mimetype='application/x-sqlite3', resumable=True)
        if items:
            file_id = items[0]['id']
            self.drive_service.files().update(fileId=file_id, media_body=media).execute()
            return f"[SUCCESS] Successfully OVERWRITTEN '{db_name}' in Google Drive."
        else:
            file_metadata = {'name': db_name}
            self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return f"[SUCCESS] Successfully UPLOADED new '{db_name}' to Google Drive."

    def pull_from_cloud(self):
        if self.drive_service:
            self.btn_pull.setEnabled(False)
            self.cloud_console.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting PULL from Cloud...")
            asyncio.create_task(self._async_pull_from_cloud())

    async def _async_pull_from_cloud(self):
        try:
            if self.db_conn:
                self.db_conn.close()
                self.db_conn = None
            
            result = await asyncio.to_thread(self._blocking_pull)
            self.cloud_console.append(result)
            
            self.init_database()
            self.load_journal_data()
            self.refresh_portfolio_grid()
            
        except Exception as e:
            self.cloud_console.append(f"[ERROR] Pull failed: {e}")
            if not self.db_conn: self.init_database()
        finally:
            self.btn_pull.setEnabled(True)

    def _blocking_pull(self):
        db_name = "trade_guardian_v4.db"
        local_path = os.path.join(os.path.dirname(__file__), db_name)
        
        query = f"name='{db_name}' and trashed=false"
        results = self.drive_service.files().list(q=query, pageSize=1, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        if not items: return f"[ERROR] '{db_name}' not found in Google Drive."
            
        file_id = items[0]['id']
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
        with open(local_path, "wb") as f:
            f.write(fh.getbuffer())
            
        return f"[SUCCESS] Successfully DOWNLOADED '{db_name}' from Google Drive."

    # --------------------------------------------------------------------------
    # --- CORE WORKFLOW LOGIC & V150 DATABASE ALIGNMENT ---
    # --------------------------------------------------------------------------
    def init_database(self):
        db_path = os.path.join(os.path.dirname(__file__), "trade_guardian_v4.db")
        self.db_conn = sqlite3.connect(db_path)
        c = self.db_conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trades (
                        id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE,
                        exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL,
                        theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT,
                        parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE,
                        pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL,
                        FOREIGN KEY(trade_id) REFERENCES trades(id))''')
        c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                        name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER,
                        min_stability REAL, description TEXT, typical_debit REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS campaign_adjustments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT, date TEXT, 
                        action TEXT, details TEXT, realized_pnl REAL)''')
        self.db_conn.commit()
        
        c.execute("SELECT count(*) FROM strategy_config")
        if c.fetchone()[0] == 0:
            defaults = [('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000), ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200), ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000), ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)]
            c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
            self.db_conn.commit()
            
    def load_strategy_groups(self):
        if os.path.exists(self.active_strategies_file):
            try:
                with open(self.active_strategies_file, 'r') as f: return json.load(f)
            except: pass
        return {}

    def save_strategy_groups(self, data):
        with open(self.active_strategies_file, 'w') as f: json.dump(data, f, indent=4)

    def refresh_portfolio_grid(self):
        if not self.ib.isConnected() or not self.account_selector.currentText(): return
        selected_account = self.account_selector.currentText()
            
        try:
            positions = self.ib.positions(account=selected_account)
            self.portfolio_tree.clear()
            
            all_groups = self.load_strategy_groups()
            acc_groups = all_groups.get(selected_account, {})
            grouped_conids = {cid: {"g_id": g_id, "data": g_data} for g_id, g_data in acc_groups.items() for cid in g_data.get('legs', [])}
                    
            ticker_nodes, strategy_nodes, combo_nodes = {}, {}, {}
            
            # --- SGPV Variables ---
            sgpv_opt = 0.0
            sgpv_stk = 0.0
            net_liq = 0.0
            
            for val in self.ib.accountValues(account=selected_account):
                if val.tag == 'NetLiquidationByCurrency' and val.currency == 'BASE':
                    try: net_liq = float(val.value)
                    except: pass
            
            for pos in positions:
                conid, ticker = pos.contract.conId, pos.contract.symbol
                
                if ticker not in ticker_nodes:
                    t_node = SortableTreeWidgetItem(self.portfolio_tree); t_node.setText(0, f"📊 {ticker}"); t_node.setFont(0, QFont("Menlo", 14, QFont.Weight.Bold)); t_node.setForeground(0, QColor('#58a6ff'))
                    ticker_nodes[ticker] = t_node
                    u_node = SortableTreeWidgetItem(t_node); u_node.setText(0, f"📁 Ungrouped {ticker}"); u_node.setFont(0, QFont("Menlo", 12, QFont.Weight.Bold)); u_node.setForeground(0, QColor('#8b949e'))
                    strategy_nodes[(ticker, "UNGROUPED")] = u_node

                group_info = grouped_conids.get(conid)
                if group_info:
                    g_id, g_data = group_info["g_id"], group_info["data"]
                    strat_name, combo_name, realized_pnl = g_data.get("strategy", "General Strategy"), g_data.get("name", "Custom Combo"), g_data.get("realized_pnl", 0.0)
                    strat_key = (ticker, strat_name)
                    
                    if strat_key not in strategy_nodes:
                        s_node = SortableTreeWidgetItem(ticker_nodes[ticker]); s_node.setText(0, f"📑 {strat_name}"); s_node.setFont(0, QFont("Menlo", 13, QFont.Weight.Bold)); s_node.setForeground(0, QColor('#d2a8ff'))
                        strategy_nodes[strat_key] = s_node
                        
                    if g_id not in combo_nodes:
                        c_node = SortableTreeWidgetItem(strategy_nodes[strat_key]); c_node.setText(0, f"📦 {combo_name}"); c_node.setFont(0, QFont("Menlo", 12, QFont.Weight.Bold)); c_node.setForeground(0, QColor('#e3b341')); c_node.setData(0, Qt.ItemDataRole.UserRole, g_id)
                        c_node.setText(6, f"${realized_pnl:.2f}")
                        c_node.setForeground(6, QColor('#3fb950' if realized_pnl >= 0 else '#f85149'))
                        combo_nodes[g_id] = c_node
                    parent_node = combo_nodes[g_id]
                else: parent_node = strategy_nodes[(ticker, "UNGROUPED")]
                    
                child = SortableTreeWidgetItem(parent_node)
                sym = pos.contract.localSymbol if pos.contract.localSymbol else pos.contract.symbol
                
                # --- OPTIONNET EXPLORER VISUAL LEG COLORING ---
                qty_val = float(pos.position)
                color_hex = '#c9d1d9' # Default
                if pos.contract.secType == 'OPT':
                    is_call = pos.contract.right == 'C'
                    is_put = pos.contract.right == 'P'
                    if qty_val > 0 and is_call: color_hex = '#3fb950' # Long Call: Green
                    elif qty_val < 0 and is_call: color_hex = '#f85149' # Short Call: Red
                    elif qty_val > 0 and is_put: color_hex = '#58a6ff' # Long Put: Blue
                    elif qty_val < 0 and is_put: color_hex = '#d2a8ff' # Short Put: Purple
                
                child.setText(0, sym); child.setFont(0, QFont("Menlo", 12, QFont.Weight.Bold)); child.setToolTip(0, f"Full Leg Details: {sym}")
                child.setForeground(0, QColor(color_hex))
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable); child.setCheckState(0, Qt.CheckState.Unchecked)
                multiplier = float(pos.contract.multiplier) if pos.contract.multiplier else 100.0
                
                child.setData(0, Qt.ItemDataRole.UserRole + 1, {
                    "symbol": pos.contract.symbol, "secType": pos.contract.secType, "expiry": pos.contract.lastTradeDateOrContractMonth,
                    "strike": pos.contract.strike, "right": pos.contract.right, "multiplier": multiplier, "qty": pos.position, "avgCost": pos.avgCost / multiplier 
                })
                child.setData(0, Qt.ItemDataRole.UserRole, conid)
                child.setText(1, str(pos.position)); child.setForeground(1, QColor('#3fb950' if pos.position > 0 else '#f85149' if pos.position < 0 else '#c9d1d9'))
                child.setText(2, f"${pos.avgCost / multiplier:.2f}")
                
                # SGPV Accumulation
                if pos.contract.secType == 'OPT':
                    sgpv_opt += abs(pos.position) * multiplier * getattr(pos.contract, 'strike', 0.0) # Proxy for notional exposure
                elif pos.contract.secType == 'STK':
                    sgpv_stk += abs(pos.position) * (pos.avgCost / multiplier if pos.avgCost else 0.0)
                
            self.portfolio_tree.expandAll()
            
            # Update SGPV UI
            self.lbl_net_liq.setText(f"NET LIQ: ${net_liq:,.0f}")
            self.lbl_sgpv_opt.setText(f"OPT SGPV: ${sgpv_opt:,.0f}")
            self.lbl_sgpv_stk.setText(f"STK SGPV: ${sgpv_stk:,.0f}")
            
            if net_liq > 0:
                ratio = (sgpv_opt + sgpv_stk) / net_liq
                self.lbl_sgpv_ratio.setText(f"RATIO: {ratio:.1f}")
                if ratio > 50:
                    self.lbl_sgpv_ratio.setStyleSheet("color: #FF5252;")
                elif ratio > 30:
                    self.lbl_sgpv_ratio.setStyleSheet("color: #FFD600;")
                else:
                    self.lbl_sgpv_ratio.setStyleSheet("color: #00C853;")
            
            if hasattr(self, 'filter_ticker'): self.filter_portfolio_tree() 
            asyncio.create_task(self._async_populate_tree_greeks(positions))
        except Exception as e: print(f"[ERROR] Failed to fetch portfolio tree: {e}")

    def on_portfolio_item_clicked(self, item, column):
        curr = item
        combo_name = "None"
        while curr is not None:
            if curr.text(0).startswith("📦 "):
                combo_name = curr.text(0).replace("📦 ", "")
                break
            curr = curr.parent()
        self.lbl_combo_name.setText(f"Selected Combo: {combo_name}")

        leaves = self._get_all_leaves(item)
        if not leaves: return
        self.stage_order(leaves, model_only=True)
        expiries = []
        symbol = None
        for l in leaves:
            data = l.data(0, Qt.ItemDataRole.UserRole + 1)
            if data:
                if not symbol: symbol = data.get('symbol')
                if data.get('secType') == 'OPT' and data.get('expiry'):
                    expiries.append(data.get('expiry'))
                    
        if symbol: asyncio.create_task(self._async_sync_workspace(symbol, expiries))

    async def _async_sync_workspace(self, symbol, expiries):
        try:
            if self.ticker_input.text().upper() != symbol.upper():
                self.ticker_input.setText(symbol)
                await self._async_load_chart(symbol)
                
            if self.chain_ticker_in.text().upper() != symbol.upper():
                self.chain_ticker_in.setText(symbol)
            
            await self._async_fetch_expirations(symbol)
            
            valid_exps = [e for e in expiries if e]
            if valid_exps:
                earliest = sorted(valid_exps)[0] 
                formatted = f"{earliest[:4]}-{earliest[4:6]}-{earliest[6:]}"
                idx = -1
                for i in range(self.exp_combo.count()):
                    if self.exp_combo.itemText(i).startswith(formatted):
                        idx = i
                        break
                if idx >= 0:
                    self.exp_combo.setCurrentIndex(idx)
                    await self._async_load_chain(symbol, earliest)
        except Exception as e: pass

    def analyze_portfolio_selection(self):
        selected_leaves = []
        def find_checked(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if child.checkState(0) == Qt.CheckState.Checked and child.data(0, Qt.ItemDataRole.UserRole + 1): selected_leaves.append(child)
                find_checked(child)
        for i in range(self.portfolio_tree.topLevelItemCount()): find_checked(self.portfolio_tree.topLevelItem(i))
        if selected_leaves: self.stage_order(selected_leaves, model_only=True)

    def stage_order(self, leg_items, model_only=False):
        self.working_basket = []
        raw_legs = []
        for item in leg_items:
            data = item.data(0, Qt.ItemDataRole.UserRole + 1)
            if data and data['qty'] != 0: raw_legs.append(data)
            
        if not raw_legs:
            self.refresh_basket_ui()
            return
            
        ratios = [int(abs(d['qty'])) for d in raw_legs]
        combo_gcd = ratios[0]
        for r in ratios[1:]: combo_gcd = math.gcd(combo_gcd, r)
            
        inferred_ticker = raw_legs[0]['symbol']
        
        for data in raw_legs:
            raw_qty = data['qty']
            action = "BUY" if raw_qty > 0 else "SELL"
            if not model_only: action = "SELL" if raw_qty > 0 else "BUY" 
            normalized_qty = int(abs(raw_qty) // combo_gcd)
            
            self.working_basket.append({
                'action': action, 'qty': normalized_qty, 'type': data.get('right', 'STK') if data.get('secType') == 'OPT' else 'STK',
                'strike': data.get('strike', 0.0), 'price': data.get('avgCost', 0.0), 'dte_str': str(data.get('expiry', 'N/A')),
                'is_existing': model_only, 'secType': data.get('secType', 'STK'), 'multiplier': data.get('multiplier', 100.0),
                'bid': data.get('avgCost', 0.0), 'ask': data.get('avgCost', 0.0) 
            })
            
        if not model_only: self.inp_exec_qty.setValue(combo_gcd)
            
        self.chain_ticker_in.setText(inferred_ticker)
        self.fetch_expirations() 
        self.refresh_basket_ui()

    def calculate_bs_price(self, S, K, T, r, sigma, opt_type):
        if T <= 0: return max(0, S - K) if opt_type == 'C' else max(0, K - S)
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        if opt_type == 'C': return S * si.norm.cdf(d1) - K * np.exp(-r * T) * si.norm.cdf(d2)
        else: return K * np.exp(-r * T) * si.norm.cdf(-d2) - S * si.norm.cdf(-d1)

    def calculate_bs_greeks(self, S, K, T, r, sigma, opt_type):
        if T <= 0 or S <= 0 or K <= 0: return 0.0, 0.0, 0.0, 0.0
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        nd1 = si.norm.cdf(d1); nd2 = si.norm.cdf(d2); n_d1 = np.exp(-d1**2 / 2) / np.sqrt(2 * np.pi)
        gamma = n_d1 / (S * sigma * np.sqrt(T)); vega = S * n_d1 * np.sqrt(T) / 100
        if opt_type == 'C': delta = nd1; theta = (-S * n_d1 * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * nd2) / 365
        else: delta = nd1 - 1; theta = (-S * n_d1 * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * si.norm.cdf(-d2)) / 365
        return delta, gamma, theta, vega

    def get_ai_recommendation(self, pnl, days, theta, delta, strat):
        if not hasattr(self, 'strat_configs'):
            self.strat_configs = {}
            try:
                c = self.db_conn.cursor()
                c.execute("SELECT name, target_pnl, target_days FROM strategy_config")
                for r in c.fetchall(): self.strat_configs[r[0]] = {'pnl': r[1], 'dit': r[2]}
            except: pass
            
        bench = self.strat_configs.get(strat, {'pnl': 500, 'dit': 45})
        target = bench['pnl']
        avg_days = bench['dit']
        
        if pnl >= target: return "🟢 TAKE PROFIT", '#3fb950'
        if pnl >= target * 0.8: return "🟡 PREPARE EXIT", '#e3b341'
        if pnl < 0:
            if theta > 0:
                recov = abs(pnl) / theta
                if recov > max(1, avg_days - days): return "🔴 KILL: ZOMBIE", '#f85149'
            elif days > 15:
                return "🔴 KILL: NEG THETA", '#f85149'
                
        stability = theta / (abs(delta) + 1)
        if stability < 0.3 and days > 5: return "🟠 RISK REVIEW", '#e3b341'
        if days < (avg_days * 0.7): return "🔵 COOKING", '#58a6ff'
        if days > (avg_days * 1.25): return "🔴 STALE", '#f85149'
        return "⚪ HOLD", '#8b949e'

    async def _async_populate_tree_greeks(self, positions):
        try:
            symbols = list(set([p.contract.symbol for p in positions]))
            spots = {}
            for sym in symbols:
                contract = Stock(sym, 'SMART', 'USD')
                if sym in ['SPX', 'VIX']: contract = Index(sym, 'CBOE', 'USD')
                elif sym in ['NDX']: contract = Index(sym, 'NASDAQ', 'USD')
                elif sym in ['RUT']: contract = Index(sym, 'RUSSELL', 'USD')
                await self.ib.qualifyContractsAsync(contract)
                tickers = await self.ib.reqTickersAsync(contract)
                spots[sym] = tickers[0].marketPrice() if tickers and not math.isnan(tickers[0].marketPrice()) else 100.0

            def calc_node(node):
                n_delta, n_theta, n_vega, n_pnl, n_debit = 0.0, 0.0, 0.0, 0.0, 0.0
                data = node.data(0, Qt.ItemDataRole.UserRole + 1)
                if data: 
                    spot = spots.get(data['symbol'], 100.0)
                    qty = data['qty']
                    mult = data['multiplier']
                    avgCost = data['avgCost']
                    if data['secType'] == 'OPT':
                        T = max(1, (datetime.strptime(data['expiry'], "%Y%m%d") - datetime.now()).days) / 365.0
                        d, g, th, v = self.calculate_bs_greeks(spot, data['strike'], T, 0.05, 0.20, data['right'])
                        curr_px = self.calculate_bs_price(spot, data['strike'], T, 0.05, 0.20, data['right'])
                        n_delta, n_theta, n_vega = d * qty * mult, th * qty * mult, v * qty * mult
                        n_pnl = (curr_px - avgCost) * qty * mult
                    else: 
                        n_delta = qty
                        n_pnl = (spot - avgCost) * qty * mult
                else: 
                    for i in range(node.childCount()):
                        cd, cth, cv, cpnl, cdeb = calc_node(node.child(i))
                        n_delta += cd; n_theta += cth; n_vega += cv; n_pnl += cpnl
                
                node.setText(3, f"{n_delta:.2f}"); node.setText(4, f"{n_theta:.2f}"); node.setText(5, f"{n_vega:.2f}")
                for col in [3, 4, 5]:
                    val = float(node.text(col))
                    node.setForeground(col, QColor('#3fb950' if val > 0.01 else '#f85149' if val < -0.01 else '#8b949e'))
                    
                g_id = node.data(0, Qt.ItemDataRole.UserRole)
                if g_id and isinstance(g_id, str) and g_id.startswith('grp_'):
                    realized = float(node.text(6).replace('$', '')) if node.text(6) else 0.0
                    total_live = n_pnl + realized
                    try: days_held = max(1, (datetime.now() - datetime.fromtimestamp(int(g_id.split('_')[1]))).days)
                    except: days_held = 1
                    velocity = total_live / days_held
                    node.setText(7, f"${velocity:.2f}/d")
                    node.setForeground(7, QColor('#3fb950' if velocity > 0 else '#f85149'))
                    strat_name = node.parent().text(0).replace('📑 ', '').strip()
                    rec_txt, rec_clr = self.get_ai_recommendation(total_live, days_held, n_theta, n_delta, strat_name)
                    node.setText(8, rec_txt)
                    node.setForeground(8, QColor(rec_clr))
                    node.setFont(8, QFont("Menlo", 11, QFont.Weight.Bold))
                    
                return n_delta, n_theta, n_vega, n_pnl, n_debit
            for i in range(self.portfolio_tree.topLevelItemCount()): calc_node(self.portfolio_tree.topLevelItem(i))
        except Exception as e: print(f"[GREEK ERROR] Failed to populate tree math: {e}")

    def refresh_basket_ui(self):
        self.basket_table.setRowCount(0)
        b_bid, b_ask = 0.0, 0.0
        active_new_legs = False
        
        for i, leg in enumerate(self.working_basket):
            self.basket_table.insertRow(i)
            action_text = f"HOLD {leg['action']}" if leg.get('is_existing') else leg['action']
            action = QTableWidgetItem(action_text)
            action.setForeground(QColor('#58a6ff' if leg.get('is_existing') else '#3fb950' if leg['action'] == 'BUY' else '#f85149'))
            action.setFont(QFont("Menlo", 11, QFont.Weight.Bold))
            ext_val = leg['price'] * leg['qty'] * (1 if leg.get('secType') == 'STK' else 100)
            
            if not leg.get('is_existing'):
                active_new_legs = True
                qty = leg['qty']
                l_bid, l_ask = leg.get('bid', leg['price']), leg.get('ask', leg['price'])
                if leg['action'] == 'BUY': b_bid -= l_ask * qty; b_ask -= l_bid * qty
                else: b_bid += l_bid * qty; b_ask += l_ask * qty
            
            for j, item in enumerate([action, QTableWidgetItem(str(leg['qty'])), QTableWidgetItem(leg['type']), QTableWidgetItem(str(leg['strike']) if leg['type'] != 'STK' else '---'), QTableWidgetItem(f"${leg['price']:.2f}"), QTableWidgetItem(f"${ext_val:.2f}")]):
                if j > 0: item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter); self.basket_table.setItem(i, j, item)
                
        if active_new_legs:
            mid = (b_bid + b_ask) / 2
            self.lbl_price_quotes.setText(f"Bid: ${b_bid:.2f}   |   Mid: ${mid:.2f}   |   Ask: ${b_ask:.2f}")
            self.inp_exec_lmt.setValue(mid)
            if mid > 0: self.lbl_net_price.setText("Net Credit"); self.lbl_net_price.setStyleSheet("font-size: 16px; font-weight: bold; color: #3fb950;")
            else: self.lbl_net_price.setText("Net Debit"); self.lbl_net_price.setStyleSheet("font-size: 16px; font-weight: bold; color: #f85149;")
        else:
            self.lbl_price_quotes.setText("Bid: ---   |   Mid: ---   |   Ask: ---")
            self.lbl_net_price.setText("Net Price: $0.00"); self.lbl_net_price.setStyleSheet("font-size: 16px; font-weight: bold;")
            self.inp_exec_lmt.setValue(0.0)
            
        asyncio.create_task(self._async_update_risk_graph())

    def add_underlying_to_basket(self, action):
        ticker = self.chain_ticker_in.text().upper().strip()
        if not ticker: return
        qty = self.inp_ul_qty.value()
        spot = self.current_chain_spot if self.current_chain_spot > 0 else 100.0
        leg = {
            'action': action, 'qty': qty, 'secType': 'STK', 'strike': 0.0,
            'type': '', 'price': spot, 'dte_str': 'N/A', 'symbol': ticker, 'multiplier': 1, 'is_existing': False
        }
        self.working_basket.append(leg)
        self.refresh_basket_ui()

    def clear_basket(self): self.working_basket = []; self.refresh_basket_ui()

    def build_template_strategy(self, strat_type):
        if not hasattr(self, 'chain_strikes') or not self.chain_strikes:
            print("[WARN] Please load an option chain first.")
            return

        spot = self.current_chain_spot
        strikes = self.chain_strikes
        exp = self.exp_combo.currentText()
        if not exp: return
        
        closest_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
        self.clear_basket()
        
        def add_leg(action, qty, opt_type, strike_offset):
            idx = closest_idx + strike_offset
            if 0 <= idx < len(strikes):
                price = 0.0
                for c in getattr(self, 'active_chain_contracts', []):
                    if c.strike == strikes[idx] and c.right == opt_type:
                        tick = self.ib.ticker(c)
                        if tick and not math.isnan(tick.bid) and not math.isnan(tick.ask):
                            price = (tick.bid + tick.ask) / 2
                        elif tick and not math.isnan(tick.close):
                            price = tick.close
                        break

                self.working_basket.append({
                    'action': action, 'qty': qty, 'type': opt_type, 'strike': strikes[idx], 
                    'price': price, 'dte_str': exp.replace('-', ''), 'is_existing': False, 
                    'secType': 'OPT', 'multiplier': 100.0, 'bid': price, 'ask': price
                })

        if strat_type == 'straddle':
            add_leg("SELL", 1, "C", 0); add_leg("SELL", 1, "P", 0)
        elif strat_type == 'strangle':
            add_leg("SELL", 1, "C", 2); add_leg("SELL", 1, "P", -2)
        elif strat_type == 'iron_condor':
            add_leg("SELL", 1, "C", 2); add_leg("BUY", 1, "C", 4)
            add_leg("SELL", 1, "P", -2); add_leg("BUY", 1, "P", -4)
        elif strat_type == 'call_vert':
            add_leg("SELL", 1, "C", 1); add_leg("BUY", 1, "C", 2)
        elif strat_type == 'put_vert':
            add_leg("SELL", 1, "P", -1); add_leg("BUY", 1, "P", -2)
            
        self.refresh_basket_ui()

    async def _async_update_risk_graph(self):
        self.port_risk_plot.clear(); self.port_risk_plot.addItem(self.risk_vLine, ignoreBounds=True)
        if not self.working_basket:
            for lbl in [self.lbl_agg_delta, self.lbl_agg_gamma, self.lbl_agg_theta, self.lbl_agg_vega, self.lbl_agg_pnl]: lbl.setText(lbl.text().split(":")[0] + ": ---")
            self.lbl_combo_name.setText("Selected Combo: None")
            return
        try:
            target_symbol = self.chain_ticker_in.text().upper().strip() or "SPX"
            contract = Stock(target_symbol, 'SMART', 'USD')
            if target_symbol in ['SPX', 'VIX']: contract = Index(target_symbol, 'CBOE', 'USD')
            elif target_symbol in ['NDX']: contract = Index(target_symbol, 'NASDAQ', 'USD')
            elif target_symbol in ['RUT']: contract = Index(target_symbol, 'RUSSELL', 'USD')
                
            await self.ib.qualifyContractsAsync(contract)
            if contract.conId > 0: self.ib.reqMktData(contract, '', False, False)
            await asyncio.sleep(0.2)
            tick = self.ib.ticker(contract)
            spot = tick.marketPrice() if tick and not math.isnan(tick.marketPrice()) else (tick.close if tick else self.current_chain_spot)
            if math.isnan(spot) or spot == 0: spot = 100.0
            
            total_delta, total_gamma, total_theta, total_vega, total_pnl = 0.0, 0.0, 0.0, 0.0, 0.0
            max_dte = max([max(1, (datetime.strptime(l['dte_str'], "%Y%m%d") - datetime.now()).days) for l in self.working_basket if l['secType'] == 'OPT' and l['dte_str'] != 'N/A'] + [1])

            iv_adj = self.slider_iv.value() / 100.0 if hasattr(self, 'slider_iv') else 0.0
            dte_fwd = self.slider_dte.value() if hasattr(self, 'slider_dte') else 0
            
            if hasattr(self, 'slider_dte') and self.slider_dte.maximum() != int(max_dte):
                self.slider_dte.blockSignals(True)
                self.slider_dte.setRange(0, max(0, int(max_dte) - 1))
                self.slider_dte.blockSignals(False)

            for leg in self.working_basket:
                qty = leg['qty']; mult = leg['multiplier']; mult_sign = 1 if leg['action'] == 'BUY' else -1
                if leg['secType'] == 'OPT' and leg['dte_str'] != 'N/A':
                    actual_days = max(1, (datetime.strptime(leg['dte_str'], "%Y%m%d") - datetime.now()).days)
                    T = max(0.001, actual_days - dte_fwd) / 365.0
                    act_iv = max(0.01, 0.20 + iv_adj) # Fallback if IV tracking isn't live
                    d, g, th, v = self.calculate_bs_greeks(spot, leg['strike'], T, 0.05, act_iv, leg['type'])
                    current_price = self.calculate_bs_price(spot, leg['strike'], T, 0.05, act_iv, leg['type'])
                    total_delta += (d * qty * mult * mult_sign); total_gamma += (g * qty * mult * mult_sign)
                    total_theta += (th * qty * mult * mult_sign); total_vega  += (v * qty * mult * mult_sign)
                    if leg.get('is_existing'): total_pnl += (current_price - leg['price']) * qty * mult * mult_sign
                elif leg['secType'] == 'STK':
                    total_delta += (qty * mult_sign)
                    if leg.get('is_existing'): total_pnl += (spot - leg['price']) * qty * mult_sign
                    
            def fmt_greek(lbl, name, val, fmt):
                lbl.setText(f"{name}: {val:{fmt}}")
                lbl.setStyleSheet(f"color: {'#3fb950' if val > 0 else ('#f85149' if val < 0 else '#8b949e')}; font-weight: bold; font-family: Helvetica;")
                
            fmt_greek(self.lbl_agg_delta, "Delta", total_delta, ".2f")
            fmt_greek(self.lbl_agg_gamma, "Gamma", total_gamma, ".4f")
            fmt_greek(self.lbl_agg_theta, "Theta", total_theta, ".2f")
            fmt_greek(self.lbl_agg_vega, "Vega",  total_vega,  ".2f")
            fmt_greek(self.lbl_agg_pnl,   "Net PnL", total_pnl, ".2f")

            strikes = [leg['strike'] for leg in self.working_basket if leg['secType'] == 'OPT']
            if strikes:
                spread = max(max(strikes) - min(strikes), spot * 0.05)
                x_min, x_max = min(spot * 0.8, min(strikes) - spread * 0.8), max(spot * 1.2, max(strikes) + spread * 0.8)
            else: x_min, x_max = spot * 0.85, spot * 1.15
                
            x = np.linspace(x_min, x_max, 400); today = datetime.now() + timedelta(days=dte_fwd)
            eff_max_dte = max(0.001, max_dte - dte_fwd)
            
            # --- OPTIONSTRAT T-LINES / TIMEFRAMES ---
            time_slices = []
            num_t_lines = self.inp_t_lines.value()
            palette = ['#388bfd', '#d2a8ff', '#ff7b72', '#7ee787', '#f0883e', '#ffa657', '#79c0ff']
            if num_t_lines > 0:
                steps = np.linspace(eff_max_dte, 0.001, num_t_lines + 1)[:-1]
                for i, days_left in enumerate(steps):
                    c = palette[i % len(palette)]
                    if i == 0:
                        time_slices.append((days_left, 'T+0', c, Qt.PenStyle.SolidLine, 2.5))
                    else:
                        days_passed = eff_max_dte - days_left
                        time_slices.append((days_left, f'T+{int(days_passed)}', c, Qt.PenStyle.DashLine, 1.5))
            time_slices.append((0.001, 'T+Expiry', '#e3b341', Qt.PenStyle.SolidLine, 3))
            
            self.current_risk_data = {'x': x, 'lines': []}
            all_y_min, all_y_max = 0, 0
            t0_spot_pnl = 0.0
            
            for days_left, lbl_prefix, color, style, width in time_slices:
                y_pnl = np.zeros_like(x); name = f"{lbl_prefix} ({(today + timedelta(days=max(0, eff_max_dte - days_left))).strftime('%b %d')})"
                for leg in self.working_basket:
                    mult_sign = 1 if leg['action'] == 'BUY' else -1
                    if leg['secType'] == 'OPT' and leg['dte_str'] != 'N/A':
                        leg_total_dte = max(1, (datetime.strptime(leg['dte_str'], "%Y%m%d") - today).days)
                        T = max(0.001, leg_total_dte - (eff_max_dte - days_left)) / 365.0
                        prices = np.array([self.calculate_bs_price(s, leg['strike'], T, 0.05, max(0.01, 0.20 + iv_adj), leg['type']) for s in x])
                        y_pnl += (prices - leg['price']) * leg['qty'] * leg['multiplier'] * mult_sign 
                    elif leg['secType'] == 'STK':
                        y_pnl += (x - leg['price']) * leg['qty'] * mult_sign
                
                if lbl_prefix == 'T+Expiry':
                    base_line = self.port_risk_plot.plot(x, np.zeros_like(x), pen=pg.mkPen('#8b949e', style=Qt.PenStyle.DashLine))
                    self.port_risk_plot.addItem(pg.FillBetweenItem(base_line, self.port_risk_plot.plot(x, np.maximum(y_pnl, 0), pen=None), brush=pg.mkBrush(63, 185, 80, 50)))
                    self.port_risk_plot.addItem(pg.FillBetweenItem(base_line, self.port_risk_plot.plot(x, np.minimum(y_pnl, 0), pen=None), brush=pg.mkBrush(248, 81, 73, 50)))
                elif lbl_prefix == 'T+0':
                    t0_spot_pnl = 0.0
                    for leg in self.working_basket:
                        m_sign = 1 if leg['action'] == 'BUY' else -1
                        if leg['secType'] == 'OPT' and leg['dte_str'] != 'N/A':
                            leg_total_dte = max(1, (datetime.strptime(leg['dte_str'], "%Y%m%d") - today).days)
                            T = max(0.001, leg_total_dte - (eff_max_dte - days_left)) / 365.0
                            calc_p = self.calculate_bs_price(spot, leg['strike'], T, 0.05, max(0.01, 0.20 + iv_adj), leg['type'])
                            t0_spot_pnl += (calc_p - leg['price']) * leg['qty'] * leg['multiplier'] * m_sign
                        elif leg['secType'] == 'STK':
                            t0_spot_pnl += (spot - leg['price']) * leg['qty'] * m_sign

                self.port_risk_plot.plot(x, y_pnl, pen=pg.mkPen(color, width=width, style=style), name=name)
                all_y_min, all_y_max = min(all_y_min, np.min(y_pnl)), max(all_y_max, np.max(y_pnl))
                self.current_risk_data['lines'].append({'name': name, 'y': y_pnl, 'color': color})

            y_padding = max(abs(all_y_max - all_y_min) * 0.1, 10)
            self.port_risk_plot.setYRange(all_y_min - y_padding, all_y_max + y_padding)
            self.port_risk_plot.setXRange(min(strikes) - spread*0.8 if strikes else spot*0.9, max(strikes) + spread*0.8 if strikes else spot*1.1)
            self.port_risk_plot.addItem(pg.InfiniteLine(pos=spot, angle=90, pen=pg.mkPen('#ffffff', width=1, style=Qt.PenStyle.DashLine), label=f'${spot:.2f}', labelOpts={'position':0.05, 'color':'#ffffff'}))

            if num_t_lines > 0:
                dot = pg.ScatterPlotItem(x=[spot], y=[t0_spot_pnl], size=8, pen=pg.mkPen('#ffffff'), brush=pg.mkBrush('#ffffff'))
                self.port_risk_plot.addItem(dot)

            # --- GENERATE HEATMAP & KPIs ---
            t_expiry_pnl = np.zeros_like(x)
            for leg in self.working_basket:
                mult_sign = 1 if leg['action'] == 'BUY' else -1
                if leg['secType'] == 'OPT' and leg['dte_str'] != 'N/A':
                    T = 0.001 / 365.0
                    prices = np.array([self.calculate_bs_price(s, leg['strike'], T, 0.05, 0.20, leg['type']) for s in x])
                    t_expiry_pnl += (prices - leg['price']) * leg['qty'] * leg['multiplier'] * mult_sign
                elif leg['secType'] == 'STK':
                    t_expiry_pnl += (x - leg['price']) * leg['qty'] * mult_sign

        except Exception as e: print(f"[RISK ERROR] Failed to simulate profile: {e}")

    def risk_mouse_moved(self, evt):
        if getattr(self, 'current_risk_data', None) is None: return
        pos = evt[0]
        if self.port_risk_plot.sceneBoundingRect().contains(pos):
            x_val = self.port_risk_plot.getViewBox().mapSceneToView(pos).x()
            self.risk_vLine.setPos(x_val)
            
            x_array = self.current_risk_data['x']
            idx = np.searchsorted(x_array, x_val)
            if 0 < idx < len(x_array):
                if abs(x_array[idx] - x_val) > abs(x_array[idx-1] - x_val): idx -= 1
                price_str = f"<div style='margin-bottom: 5px; font-size: 13px;'><b>SPOT:</b> <span style='color:#26A69A'>${x_array[idx]:.2f}</span></div>"
                lines_html = []
                for line in self.current_risk_data['lines']:
                    pnl = line['y'][idx]
                    lines_html.append(f"<div style='margin-bottom: 3px;'><span style='color:{line['color']}'>{line['name']}:</span> <span style='color:{'#00C853' if pnl >= 0 else '#FF5252'}; font-weight:bold;'>${pnl:.2f}</span></div>")
                
                inspector_html = f"<div style='font-family: Inter, sans-serif; font-size: 11px; padding: 6px;'>" + price_str + "".join(lines_html) + "</div>"
                self.risk_inspector.setHtml(inspector_html)
                
                vb = self.port_risk_plot.getViewBox()
                y_pos = vb.viewRange()[1][1] - (vb.viewRange()[1][1] - vb.viewRange()[1][0]) * 0.1
                self.risk_inspector.setPos(x_val, y_pos)
                self.risk_inspector.setAnchor((0, 0) if pos.x() < vb.sceneBoundingRect().center().x() else (1, 0))
                self.risk_inspector.show()
        else:
            self.risk_inspector.hide()

    def on_iv_slider_changed(self, value):
        if hasattr(self, 'lbl_iv_adj'): self.lbl_iv_adj.setText(f"IV Adj: {value:+d}%")
        asyncio.create_task(self._async_update_risk_graph())
        
    def on_dte_slider_changed(self, value):
        if hasattr(self, 'lbl_dte_adj'): self.lbl_dte_adj.setText(f"Days Fwd: +{value}")
        asyncio.create_task(self._async_update_risk_graph())

    def transmit_basket_order(self):
        if not self.ib.isConnected(): return print("[WARN] Cannot transmit: IBKR not connected.")
        new_legs = [l for l in self.working_basket if not l.get('is_existing')]
        if not new_legs: return print("[WARN] No NEW legs to transmit.")
            
        ticker = self.chain_ticker_in.text().upper().strip()
        account = self.account_selector.currentText()
        tif = self.combo_tif.currentText()
        outside_rth = self.chk_outside_rth.isChecked()
        lmt_price = self.inp_exec_lmt.value()
        qty = self.inp_exec_qty.value()
        
        asyncio.create_task(self._async_transmit_order(new_legs, ticker, account, tif, outside_rth, lmt_price, qty))

    async def _async_transmit_order(self, new_legs, ticker, account, tif, outside_rth, lmt_price, qty):
        try:
            print(f"[*] Qualifying {len(new_legs)} legs for {ticker} with IBKR...")
            contracts = [Option(ticker, l['dte_str'], l['strike'], l['type'], 'SMART') if l['secType'] == 'OPT' else Stock(ticker, 'SMART', 'USD') for l in new_legs]
            
            qualified = await self.ib.qualifyContractsAsync(*contracts)
            if len(qualified) != len(contracts): return print("[EXEC ERROR] Could not qualify all legs with IBKR. Aborting order.")

            if len(new_legs) == 1:
                exec_contract = qualified[0]
                action = new_legs[0]['action']
                exec_lmt = abs(lmt_price)
            else:
                exec_contract = Contract(symbol=ticker, secType='BAG', currency='USD', exchange='SMART')
                exec_contract.comboLegs = [ComboLeg(conId=q_c.conId, ratio=int(leg['qty']), action=leg['action'], exchange='SMART') for q_c, leg in zip(qualified, new_legs)]
                action = "BUY"
                exec_lmt = -lmt_price 

            order = LimitOrder(action=action, totalQuantity=qty, lmtPrice=round(exec_lmt, 2), tif=tif, outsideRth=outside_rth, account=account)
            trade = self.ib.placeOrder(exec_contract, order)
            
            cursor = self.db_conn.cursor()
            trade_id = f"{ticker}_ADJ_{int(datetime.now().timestamp())}"
            cursor.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                           (trade_id, f"{ticker} Adjs ({tif})", "Other", "Active", datetime.now().strftime("%Y-%m-%d"), 0, abs(lmt_price*100*qty), qty, 0.0, 0.0, 0.0, 0.0, 0.0))
            self.db_conn.commit(); self.load_journal_data() 
            print(f"[SUCCESS] 🚀 Live Order transmitted! (Ref: {trade.order.orderId})")
            self.clear_basket()
        except Exception as e: print(f"[EXEC ERROR] Failed to execute order: {e}")

    def start_walk_order(self):
        if not self.ib.isConnected(): return print("[WARN] Cannot start walk: IBKR not connected.")
        new_legs = [l for l in self.working_basket if not l.get('is_existing')]
        if not new_legs: return print("[WARN] No NEW legs to transmit for Walk.")
        
        ticker = self.chain_ticker_in.text().upper().strip()
        account = self.account_selector.currentText()
        qty = self.inp_exec_qty.value()
        
        start_px = self.inp_walk_start.value()
        end_px = self.inp_walk_end.value()
        steps = self.inp_walk_steps.value()
        time_per_step = self.inp_walk_time.value()
        
        self.console_output.append(f"[*] Starting Walk-Limit: {steps} steps from {start_px} to {end_px} ({time_per_step}s interval)")
        asyncio.create_task(self._async_walk_machine(new_legs, ticker, account, qty, start_px, end_px, steps, time_per_step))

    async def _async_walk_machine(self, new_legs, ticker, account, qty, start_px, end_px, steps, time_per_step):
        try:
            contracts = [Option(ticker, l['dte_str'], l['strike'], l['type'], 'SMART') if l['secType'] == 'OPT' else Stock(ticker, 'SMART', 'USD') for l in new_legs]
            qualified = await self.ib.qualifyContractsAsync(*contracts)
            if len(qualified) != len(contracts): return print("[EXEC ERROR] Could not qualify all legs. Walk aborted.")

            if len(new_legs) == 1:
                exec_contract = qualified[0]
                action = new_legs[0]['action']
                mult = 1 if action == "BUY" else -1
            else:
                exec_contract = Contract(symbol=ticker, secType='BAG', currency='USD', exchange='SMART')
                exec_contract.comboLegs = [ComboLeg(conId=q_c.conId, ratio=int(leg['qty']), action=leg['action'], exchange='SMART') for q_c, leg in zip(qualified, new_legs)]
                action = "BUY"
                mult = -1

            step_prices = np.linspace(start_px, end_px, steps)
            
            # Place initial order
            current_lmt = step_prices[0] * mult
            order = LimitOrder(action=action, totalQuantity=qty, lmtPrice=round(abs(current_lmt), 2), tif="DAY", outsideRth=True, account=account)
            trade = self.ib.placeOrder(exec_contract, order)
            self.console_output.append(f"[WALK] Step 1/{steps}: Order {trade.order.orderId} placed at limit {step_prices[0]:.2f}")
            
            for i in range(1, steps):
                for _ in range(time_per_step * 10): 
                    if trade.orderStatus.status in ['Filled', 'Cancelled']: 
                        self.console_output.append(f"[WALK] Walk finished! Status: {trade.orderStatus.status}")
                        return
                    await asyncio.sleep(0.1)
                
                # Next step
                current_lmt = step_prices[i] * mult
                order.lmtPrice = round(abs(current_lmt), 2)
                trade = self.ib.placeOrder(exec_contract, order)
                self.console_output.append(f"[WALK] Step {i+1}/{steps}: Modifying order {trade.order.orderId} to limit {step_prices[i]:.2f}")
            
            self.console_output.append(f"[WALK] Walk limit reached final price ({step_prices[-1]:.2f}). Waiting for fill.")
            
        except Exception as e: print(f"[WALK ERROR] Stopped: {e}")

    def fetch_expirations(self):
        if self.chain_ticker_in.text().upper().strip(): asyncio.create_task(self._async_fetch_expirations(self.chain_ticker_in.text().upper().strip()))

    async def _async_fetch_expirations(self, ticker):
        try:
            contract = Stock(ticker, 'SMART', 'USD')
            if ticker in ['SPX', 'VIX']: contract = Index(ticker, 'CBOE', 'USD')
            elif ticker in ['NDX']: contract = Index(ticker, 'NASDAQ', 'USD')
            elif ticker in ['RUT']: contract = Index(ticker, 'RUSSELL', 'USD')
                
            await self.ib.qualifyContractsAsync(contract)
            chains = await self.ib.reqSecDefOptParamsAsync(contract.symbol, '', contract.secType, contract.conId)
            exps = set()
            for chain in chains:
                if chain.exchange == 'SMART' or contract.secType == 'IND': exps.update(chain.expirations)
                    
            if not exps: return
            today = datetime.now()
            items = []
            for d in sorted(list(exps)):
                dt = datetime.strptime(d, "%Y%m%d")
                dte = max(0, (dt - today).days)
                items.append(f"{d[:4]}-{d[4:6]}-{d[6:]} [{dte} DTE]")
            self.exp_combo.clear(); self.exp_combo.addItems(items)
            tickers = await self.ib.reqTickersAsync(contract)
            if tickers and not math.isnan(tickers[0].marketPrice()):
                self.current_chain_spot = tickers[0].marketPrice()
                self.lbl_chain_spot.setText(f"SPOT: ${self.current_chain_spot:.2f}")
        except Exception as e: print(f"[CHAIN ERROR] Failed to fetch expirations: {e}")

    def load_option_chain(self):
        if self.chain_ticker_in.text().upper().strip() and self.exp_combo.currentText(): 
            exp_date = self.exp_combo.currentText().split(" ")[0].replace("-", "")
            asyncio.create_task(self._async_load_chain(self.chain_ticker_in.text().upper().strip(), exp_date))

    async def _async_load_chain(self, ticker, exp_ibkr):
        try:
            if hasattr(self, 'active_chain_contracts') and self.active_chain_contracts:
                for c in self.active_chain_contracts:
                    if c.conId > 0: self.ib.cancelMktData(c)
                self.active_chain_contracts = []
                
            contract = Stock(ticker, 'SMART', 'USD')
            if ticker in ['SPX', 'VIX']: contract = Index(ticker, 'CBOE', 'USD')
            elif ticker in ['NDX']: contract = Index(ticker, 'NASDAQ', 'USD')
            elif ticker in ['RUT']: contract = Index(ticker, 'RUSSELL', 'USD')
                
            await self.ib.qualifyContractsAsync(contract)
            self.ib.reqMktData(contract, '', False, False)
            await asyncio.sleep(0.5); tick = self.ib.ticker(contract)
            spot = tick.marketPrice() if tick and not math.isnan(tick.marketPrice()) else (tick.close if tick else 100.0)
            if math.isnan(spot): spot = 100.0
            
            self.current_chain_spot = spot; self.lbl_chain_spot.setText(f"SPOT: ${spot:.2f}")
            chains = await self.ib.reqSecDefOptParamsAsync(contract.symbol, '', contract.secType, contract.conId)
            strikes = set()
            for c in chains:
                if c.exchange == 'SMART' or contract.secType == 'IND': strikes.update(c.strikes)
            
            sorted_strikes = sorted(list(strikes))
            if not sorted_strikes: return
            closest_idx = min(range(len(sorted_strikes)), key=lambda i: abs(sorted_strikes[i] - spot))
            
            num_strikes_half = self.inp_chain_strikes.value() // 2
            start_idx = max(0, closest_idx - num_strikes_half)
            end_idx = min(len(sorted_strikes), closest_idx + num_strikes_half + 1)
            target_strikes = sorted_strikes[start_idx:end_idx]
            self.chain_strikes = target_strikes
            
            exchange = 'SMART' if contract.secType != 'IND' else contract.exchange
            call_contracts = [Option(ticker, exp_ibkr, s, 'C', exchange, currency='USD') for s in target_strikes]
            put_contracts = [Option(ticker, exp_ibkr, s, 'P', exchange, currency='USD') for s in target_strikes]
            
            self.active_chain_contracts = call_contracts + put_contracts
            await self.ib.qualifyContractsAsync(*self.active_chain_contracts)
            for c in self.active_chain_contracts: 
                if c.conId > 0: self.ib.reqMktData(c, '100,101,104,106', False, False)
            await asyncio.sleep(2.0) 
            
            dte_text = self.exp_combo.currentText()
            dte = 1
            if "[" in dte_text:
                try: dte = max(1, int(dte_text.split("[")[1].split(" ")[0]))
                except: pass

            atm_idx = closest_idx - start_idx
            atm_c = call_contracts[atm_idx] if 0 <= atm_idx < len(call_contracts) else None
            atm_tick = self.ib.ticker(atm_c) if atm_c else None
            iv = atm_tick.modelGreeks.impliedVol if atm_tick and atm_tick.modelGreeks and getattr(atm_tick.modelGreeks, 'impliedVol', None) else 0.20
            if not iv or math.isnan(iv): iv = 0.20
            
            one_sd_move = spot * iv * math.sqrt(dte / 365.0)
            two_sd_move = 2 * one_sd_move
            
            self.chain_table.setRowCount(0)
            for i, strike in enumerate(target_strikes):
                self.chain_table.insertRow(i)
                c_tick, p_tick = self.ib.ticker(call_contracts[i]), self.ib.ticker(put_contracts[i])
                def get_oi(t): return next((tk.size for t in (t.ticks if t else []) for tk in [t] if tk.tickType == 86), 0)
                
                items = [
                    QTableWidgetItem(f"{c_tick.modelGreeks.delta:.2f}" if c_tick and c_tick.modelGreeks and c_tick.modelGreeks.delta else "---"),
                    QTableWidgetItem(f"{c_tick.volume or 0:.0f}" if c_tick else "0"),
                    QTableWidgetItem(f"{get_oi(c_tick):.0f}"),
                    QTableWidgetItem(f"{c_tick.bid:.2f}" if c_tick and not math.isnan(c_tick.bid) else "---"),
                    QTableWidgetItem(f"{c_tick.ask:.2f}" if c_tick and not math.isnan(c_tick.ask) else "---"),
                    QTableWidgetItem(f"{strike:.1f}"),
                    QTableWidgetItem(f"{p_tick.bid:.2f}" if p_tick and not math.isnan(p_tick.bid) else "---"),
                    QTableWidgetItem(f"{p_tick.ask:.2f}" if p_tick and not math.isnan(p_tick.ask) else "---"),
                    QTableWidgetItem(f"{p_tick.volume or 0:.0f}" if p_tick else "0"),
                    QTableWidgetItem(f"{get_oi(p_tick):.0f}"),
                    QTableWidgetItem(f"{p_tick.modelGreeks.delta:.2f}" if p_tick and p_tick.modelGreeks and p_tick.modelGreeks.delta else "---")
                ]
                
                for j, item in enumerate(items):
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    if j == 5: 
                        item.setFont(QFont("Menlo", 12, QFont.Weight.Bold))
                        dist = abs(strike - spot)
                        if strike == target_strikes[atm_idx]:
                            item.setBackground(QColor('#1f6feb')); item.setForeground(QColor('#ffffff'))
                        elif dist <= one_sd_move:
                            item.setBackground(QColor('#238636')); item.setForeground(QColor('#ffffff'))
                        elif dist <= two_sd_move:
                            item.setBackground(QColor('#b08800')); item.setForeground(QColor('#ffffff'))
                        else:
                            item.setBackground(QColor('#161b22')); item.setForeground(QColor('#58a6ff'))
                    self.chain_table.setItem(i, j, item)
        except Exception as e: print(f"[CHAIN ERROR] Failed to load chain: {e}")

    def on_chain_clicked(self, row, col):
        if col not in [3, 4, 6, 7]: return
        strike = self.chain_strikes[row]
        price_str = self.chain_table.item(row, col).text()
        if price_str == "---": return
        price = float(price_str)
        
        action, opt_type = ("SELL", "C") if col == 3 else ("BUY", "C") if col == 4 else ("SELL", "P") if col == 6 else ("BUY", "P")
        
        c_bid_s, c_ask_s = self.chain_table.item(row, 3).text(), self.chain_table.item(row, 4).text()
        p_bid_s, p_ask_s = self.chain_table.item(row, 6).text(), self.chain_table.item(row, 7).text()
        if opt_type == 'C': bid_val, ask_val = float(c_bid_s) if c_bid_s != "---" else price, float(c_ask_s) if c_ask_s != "---" else price
        else: bid_val, ask_val = float(p_bid_s) if p_bid_s != "---" else price, float(p_ask_s) if p_ask_s != "---" else price
            
        self.working_basket.append({
            'action': action, 'qty': 1, 'type': opt_type, 'strike': strike, 
            'price': price, 'dte_str': self.exp_combo.currentText(), 
            'is_existing': False, 'secType': 'OPT', 'multiplier': 100.0,
            'bid': bid_val, 'ask': ask_val
        })
        self.refresh_basket_ui()

    def show_portfolio_context_menu(self, position):
        item = self.portfolio_tree.itemAt(position)
        if not item: return
        menu = QMenu()
        data = item.data(0, Qt.ItemDataRole.UserRole + 1)
        g_id = item.data(0, Qt.ItemDataRole.UserRole)
        
        if data: 
            action_close = menu.addAction("Stage Closing Order")
            action_close.triggered.connect(lambda: self.stage_order([item], model_only=False))
        elif g_id: 
            action_close = menu.addAction("Close Entire Strategy")
            action_close.triggered.connect(lambda: self.stage_order(self._get_all_leaves(item), model_only=False))
            menu.addSeparator()
            action_realize = menu.addAction("Log Realized PnL (Roll/Close)")
            action_realize.triggered.connect(lambda: self.add_realized_pnl(g_id))
        menu.exec(self.portfolio_tree.viewport().mapToGlobal(position))

    def _get_all_leaves(self, node):
        leaves = []
        def traverse(n):
            if n.data(0, Qt.ItemDataRole.UserRole + 1): leaves.append(n)
            for i in range(n.childCount()): traverse(n.child(i))
        traverse(node)
        return leaves

    def group_selected_legs(self):
        selected_account = self.account_selector.currentText()
        if not selected_account: return
        selected_conids = []
        inferred_ticker = "SPX"
        def find_checked(node):
            nonlocal inferred_ticker
            for i in range(node.childCount()):
                if node.child(i).checkState(0) == Qt.CheckState.Checked:
                    if node.child(i).data(0, Qt.ItemDataRole.UserRole):
                        selected_conids.append(node.child(i).data(0, Qt.ItemDataRole.UserRole))
                        data = node.child(i).data(0, Qt.ItemDataRole.UserRole + 1)
                        if data and "symbol" in data: inferred_ticker = data["symbol"]
                find_checked(node.child(i))
        for i in range(self.portfolio_tree.topLevelItemCount()): find_checked(self.portfolio_tree.topLevelItem(i))
            
        if not selected_conids: return
        all_groups = self.load_strategy_groups()
        acc_groups = all_groups.setdefault(selected_account, {})
        dialog = GroupStrategyDialog(inferred_ticker, list(set([g.get('strategy', 'General') for g in acc_groups.values()])), self)
        if dialog.exec():
            ticker, strat_name, combo_name = dialog.get_data()
            if not strat_name or not combo_name: return
            acc_groups[f"grp_{int(datetime.now().timestamp())}"] = {"ticker": ticker, "strategy": strat_name, "name": combo_name, "legs": selected_conids, "realized_pnl": 0.0}
            self.save_strategy_groups(all_groups); self.refresh_portfolio_grid()

    def ungroup_selected_legs(self):
        selected_account = self.account_selector.currentText()
        if not selected_account: return
        selected_conids = []
        def find_checked(node):
            for i in range(node.childCount()):
                if node.child(i).checkState(0) == Qt.CheckState.Checked and node.child(i).data(0, Qt.ItemDataRole.UserRole):
                    selected_conids.append(node.child(i).data(0, Qt.ItemDataRole.UserRole))
                find_checked(node.child(i))
        for i in range(self.portfolio_tree.topLevelItemCount()): find_checked(self.portfolio_tree.topLevelItem(i))
        if not selected_conids: return
        
        all_groups = self.load_strategy_groups()
        modified = False
        for g_id, g_data in list(all_groups.get(selected_account, {}).items()):
            new_legs = [l for l in g_data.get('legs', []) if l not in selected_conids]
            if len(new_legs) != len(g_data.get('legs', [])):
                modified = True
                if not new_legs: del all_groups[selected_account][g_id] 
                else: g_data['legs'] = new_legs
        if modified: self.save_strategy_groups(all_groups); self.refresh_portfolio_grid()

    def filter_portfolio_tree(self):
        t_f, e_f, s_f = self.filter_ticker.text().upper().strip(), self.filter_expiry.text().strip(), self.filter_strike.text().strip()
        for i in range(self.portfolio_tree.topLevelItemCount()): self._recursive_filter(self.portfolio_tree.topLevelItem(i), t_f, e_f, s_f)

    def _recursive_filter(self, node, t_filter, e_filter, s_filter):
        any_child_visible = False
        for i in range(node.childCount()):
            if self._recursive_filter(node.child(i), t_filter, e_filter, s_filter): any_child_visible = True
        data = node.data(0, Qt.ItemDataRole.UserRole + 1)
        if data: is_match = (not t_filter or t_filter in str(data.get('symbol', '')).upper()) and (not e_filter or e_filter in str(data.get('expiry', ''))) and (not s_filter or s_filter in str(data.get('strike', '')))
        else: is_match = not t_filter or t_filter in node.text(0).upper()
        visible = any_child_visible or is_match
        node.setHidden(not visible)
        return visible

    def normal_output_written(self, text):
        cursor = self.console_output.textCursor(); cursor.movePosition(cursor.MoveOperation.End); cursor.insertText(text)
        self.console_output.setTextCursor(cursor); self.console_output.ensureCursorVisible()

    def connect_to_ibkr(self): asyncio.create_task(self.connect_async())
    async def connect_async(self):
        try:
            self.ib.connectedEvent += self.on_ib_connected; self.ib.disconnectedEvent += self.on_ib_disconnected; self.ib.errorEvent += self.on_ib_error
            util.useQt('PyQt6') 
            await self.ib.connectAsync('127.0.0.1', 7496, clientId=1) 
        except Exception as e: print(f"[IBKR ERROR] Connection failed. Is TWS Open? ({e})")

    def on_ib_connected(self):
        self.account_selector.clear(); self.account_selector.addItems(self.ib.managedAccounts()); self.refresh_portfolio_grid()
        
    def on_ib_disconnected(self): pass

    def on_ib_error(self, reqId, errorCode, errorString, contract):
        if errorCode in [2104, 2106, 2158]: return
        if errorCode == 200 and "security definition" in errorString.lower(): return
        print(f"[IBKR ERR {errorCode}] {errorString}")

    def build_campaign_tracker(self):
        layout = QVBoxLayout(self.tab_journal)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0,0,0,0)
        lbl_left = QLabel("📂 Active ONE Campaigns")
        lbl_left.setStyleSheet("font-size: 16px; font-weight: bold; color: #818cf8; padding: 5px;")
        left_layout.addWidget(lbl_left)
        
        self.campaign_list = QTreeWidget()
        self.campaign_list.setHeaderLabels(["Campaign / Strategy", "Net PnL"])
        self.campaign_list.setColumnWidth(0, 220)
        self.campaign_list.itemClicked.connect(self.on_campaign_selected)
        left_layout.addWidget(self.campaign_list)
        splitter.addWidget(left_widget)
        
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0,0,0,0)
        
        ribbon = QHBoxLayout()
        self.lbl_camp_title = QLabel("Select a campaign to view ledger")
        self.lbl_camp_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #38bdf8; padding: 5px;")
        self.btn_log_adj = QPushButton("➕ Log Manual Adjustment")
        self.btn_log_adj.clicked.connect(self.prompt_manual_adjustment)
        self.btn_log_adj.setEnabled(False)
        ribbon.addWidget(self.lbl_camp_title)
        ribbon.addStretch()
        ribbon.addWidget(self.btn_log_adj)
        right_layout.addLayout(ribbon)
        
        self.ledger_table = QTableWidget()
        self.ledger_table.setColumnCount(4)
        self.ledger_table.setHorizontalHeaderLabels(["Date/Time", "Action", "Details", "Realized PnL"])
        self.ledger_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        right_layout.addWidget(self.ledger_table)
        
        splitter.addWidget(right_widget)
        splitter.setSizes([350, 850])
        layout.addWidget(splitter)
        self.load_campaign_data()

    def load_campaign_data(self):
        self.campaign_list.clear()
        all_groups = self.load_strategy_groups()
        for acc, groups in all_groups.items():
            acc_node = QTreeWidgetItem(self.campaign_list, [f"🏦 {acc}", ""])
            for g_id, g_data in groups.items():
                name = g_data.get('name', 'Unknown')
                strat = g_data.get('strategy', '')
                pnl = g_data.get('realized_pnl', 0.0)
                item = QTreeWidgetItem(acc_node, [f"{name} ({strat})", f"${pnl:.2f}"])
                item.setForeground(1, QColor('#3fb950' if pnl >= 0 else '#f85149'))
                item.setData(0, Qt.ItemDataRole.UserRole, g_id)
        self.campaign_list.expandAll()

    def on_campaign_selected(self, item, col):
        g_id = item.data(0, Qt.ItemDataRole.UserRole)
        if not g_id: 
            self.lbl_camp_title.setText("Select a campaign to view ledger")
            self.btn_log_adj.setEnabled(False)
            self.ledger_table.setRowCount(0)
            return
            
        self.current_campaign_id = g_id
        self.lbl_camp_title.setText(f"Ledger: {item.text(0)}")
        self.btn_log_adj.setEnabled(True)
        self.refresh_ledger(g_id)

    def refresh_ledger(self, g_id):
        if not self.db_conn: return
        self.ledger_table.setRowCount(0)
        c = self.db_conn.cursor()
        c.execute("SELECT date, action, details, realized_pnl FROM campaign_adjustments WHERE group_id=? ORDER BY date DESC", (g_id,))
        for idx, row in enumerate(c.fetchall()):
            self.ledger_table.insertRow(idx)
            for j, val in enumerate(row):
                itm = QTableWidgetItem(str(val))
                itm.setFlags(itm.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if j == 3 and val is not None:
                    itm.setText(f"${float(val):.2f}")
                    itm.setForeground(QColor('#3fb950' if float(val) >= 0 else '#f85149'))
                self.ledger_table.setItem(idx, j, itm)

    def add_realized_pnl(self, g_id):
        pnl, ok = QInputDialog.getDouble(self, "Log PnL", "Enter Realized PnL from roll/close:", 0.0, -99999, 99999, 2)
        if ok:
            all_groups = self.load_strategy_groups()
            for acc, groups in all_groups.items():
                if g_id in groups:
                    groups[g_id]['realized_pnl'] = groups[g_id].get('realized_pnl', 0.0) + pnl
                    self.save_strategy_groups(all_groups)
                    if self.db_conn:
                        c = self.db_conn.cursor()
                        c.execute("INSERT INTO campaign_adjustments (group_id, date, action, details, realized_pnl) VALUES (?, datetime('now', 'localtime'), ?, ?, ?)", 
                                  (g_id, "Strategy Roll/Close", f"Realized {pnl:.2f} PnL via UI.", pnl))
                        self.db_conn.commit()
                    self.refresh_portfolio_grid()
                    self.load_campaign_data()
                    if getattr(self, 'current_campaign_id', None) == g_id: self.refresh_ledger(g_id)
                    break
                    
    def prompt_manual_adjustment(self):
        g_id = getattr(self, 'current_campaign_id', None)
        if not g_id: return
        action, ok = QInputDialog.getText(self, "Log Adjustment", "Action (e.g., Rolled Call, Defended Put):")
        if not ok or not action: return
        details, ok = QInputDialog.getText(self, "Log Details", "Details (e.g., Opened 4050 / Closed 4000):")
        if not ok: return
        if self.db_conn:
            c = self.db_conn.cursor()
            c.execute("INSERT INTO campaign_adjustments (group_id, date, action, details, realized_pnl) VALUES (?, datetime('now', 'localtime'), ?, ?, ?)",
                      (g_id, action, details, 0.0))
            self.db_conn.commit()
            self.refresh_ledger(g_id)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)
    window = BVSLaunchpad()
    window.show()
    with loop: loop.run_forever()
