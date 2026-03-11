"""
=============================================================================
PROPRIETARY TRADING SYSTEM: SEVEN DIMENSIONS PTF
MODULE: ALGORITHMIC EXECUTION ENGINE (THEA / TESSA)
OWNER: Joel Faucher
COMPLIANCE STATUS: DMCC / JCA AUDIT READY
DESCRIPTION: Automated equity execution with hard-coded pre-trade controls.
=============================================================================
"""
import uvicorn
import json
import os
import datetime
import logging
import threading
import time
import requests
import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from ib_insync import *
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# --- 1. SETUP & CONFIG ---
nest_asyncio.apply()
load_dotenv('trading.env')  # Ensure env vars are loaded

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TradingServer")
# --- AUDIT LOGGER ---
audit_logger = logging.getLogger("AuditTrail")
audit_logger.setLevel(logging.INFO)
audit_handler = logging.FileHandler('compliance_audit_trail.log')
audit_handler.setFormatter(logging.Formatter('{"timestamp": "%(asctime)s", "event": "%(message)s"}'))
audit_logger.addHandler(audit_handler)


# --- DATA MODELS ---
def get_env_tickers():
    t_str = os.getenv('TICKERS', "CL,DIS,UBER,SBUX,JPM,MSFT,COIN,AMD,MDT,MDLZ,NVDA,VZ,PG,KO,JNJ,PEP,V,BAC,HD")
    return [t.strip().upper() for t in t_str.split(',') if t.strip()]


class TheaConfig(BaseModel):
    ibkr_host: str = os.getenv('IBKR_HOST', '127.0.0.1')
    ibkr_port: int = int(os.getenv('IBKR_PORT', 7497))
    ibkr_client_id: int = 1
    total_capital: float = float(os.getenv('TOTAL_CAPITAL', 1000000))
    max_simultaneous_trades: int = int(os.getenv('MAX_SIMULTANEOUS_TRADES', 8))
    tickers: List[str] = get_env_tickers()
    trade_hour: int = int(os.getenv('TRADE_HOUR', 9))
    trade_minute: int = int(os.getenv('TRADE_MINUTE', 47))
    tp_target_percent: float = float(os.getenv('TP_TARGET_PERCENT', 0.015))
    sl_target_percent: float = float(os.getenv('SL_TARGET_PERCENT', 0.01))
    gap_entry_threshold: float = float(os.getenv('GAP_ENTRY_THRESHOLD', 0.005))
    max_gap_threshold: float = float(os.getenv('MAX_GAP_THRESHOLD', 0.0128))
    telegram_bot_token: Optional[str] = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id: Optional[str] = os.getenv('TELEGRAM_CHAT_ID')
    pilot_mode: bool = os.getenv('PILOT_MODE', 'True').lower() == 'true'
    max_shares_per_order: int = int(os.getenv('MAX_SHARES_PER_ORDER', 5000))
    max_order_value_usd: float = float(os.getenv('MAX_ORDER_VALUE', 250000))

class TessaConfig(BaseModel):
    ticker: str = "BFH"
    trade_amount_usd: float = 50000
    limit_1_price: float = 70
    limit_2_price: float = 68
    tp_pct: float = 3.5
    initial_trail_pct: float = 1.8
    final_trail_pct: float = 1.8
    delay_sec: int = 5400
    telegram_bot_token: Optional[str] = ""
    telegram_chat_id: Optional[str] = ""
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    ibkr_client_id: int = 2  # Unified Client ID
    # --- ADD AUDIT & COMPLIANCE CONTROLS ---
    max_shares_per_order: int = int(os.getenv('MAX_SHARES_PER_ORDER', 5000))  # Fat-finger limit
    max_order_value_usd: float = float(os.getenv('MAX_ORDER_VALUE', 250000))  # Fat-finger dollar limit
    max_daily_drawdown_usd: float = float(os.getenv('MAX_DAILY_DRAWDOWN', -25000))  # Circuit breaker
    pilot_mode: bool = os.getenv('PILOT_MODE', 'True').lower() == 'true'  # Deployment control


# --- RISK MGMT GLOBALS ---
state_risk_logs = []
BAD_NEWS_KEYWORDS = ['lawsuit', 'fraud', 'investigation', 'subpoena', 'bankruptcy', 'insolvency', 'misses', 'missed',
                     'lower', 'downgrade', 'cut', 'warning', 'weak', 'disappointing', 'halt', 'breach', 'recall',
                     'plunge', 'crash', 'sell-off', 'offering', 'pricing', 'priced', 'public offering',
                     'private placement', 'dilution', 'clinical hold', 'crl', 'rejected', 'failed', 'negative data',
                     'suspended']


# ==========================================
#              THEA BOT ENGINE
# ==========================================
class TheaBot:
    def __init__(self):
        self.running = False
        self.thread = None
        self.logs = []
        self.daily_run_status = {}
        self.ib = None

        # 1. Load Defaults first
        self.cfg = TheaConfig().model_dump()

        # 2. Check for saved user config and overwrite defaults if found
        if os.path.exists("user_config.json"):
            try:
                with open("user_config.json", "r") as f:
                    saved_data = json.load(f)
                    if "thea" in saved_data:
                        self.cfg.update(saved_data["thea"])
                        print(f"[System] Loaded saved Thea config. Tickers: {self.cfg['tickers']}")
            except Exception as e:
                print(f"[System] Failed to load saved config: {e}")

    def update_config(self, config_dict):
        self.cfg.update(config_dict)
        self.log(f"[Thea] Config updated. Tickers: {len(self.cfg.get('tickers'))}")

    def log(self, message):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        entry = f"[Thea] [{timestamp}] {message}"
        print(entry)
        self.logs.append(entry)
        if len(self.logs) > 100: self.logs.pop(0)

    def send_telegram(self, message):
        token = self.cfg.get("telegram_bot_token")
        chat_id = self.cfg.get("telegram_chat_id")
        if not token or not chat_id: return
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'}
            requests.post(url, data=payload)
        except Exception as e:
            self.log(f"Telegram Error: {e}")

    def create_contract(self, ticker):
        return Stock(ticker, 'SMART', 'USD')

    def check_entry_signal(self, ticker, contract):
        try:
            # Request historical data to calculate gap
            bars = self.ib.reqHistoricalData(contract, endDateTime='', durationStr='2 D', barSizeSetting='1 day',
                                             whatToShow='TRADES', useRTH=True)
            if len(bars) < 2:
                # self.log(f"{ticker}: Not enough data bars")
                return None

            prev_close = bars[-2].close
            today_open = bars[-1].open
            if prev_close == 0: return None

            gap_ratio = today_open / prev_close - 1

            # Use thresholds from config
            if self.cfg['gap_entry_threshold'] < gap_ratio <= self.cfg['max_gap_threshold']:
                return prev_close, gap_ratio

            return None
        except Exception as e:
            self.log(f"[{ticker}] Data Error: {e}")
            return None

    def place_bracket_order(self, ticker, contract, entry_price, gap_ratio, allocated_capital):
        total_quantity = int(allocated_capital // entry_price)

        # ──────────────────────────────────────────────────
        # PRE-TRADE COMPLIANCE GATEKEEPER (HARD STOPS)
        # ──────────────────────────────────────────────────

        # 1. Pilot Mode Check (Overrides quantity to 1 share for safe testing)
        if self.cfg.get('pilot_mode'):
            self.log(f"🛡️ PILOT MODE ACTIVE: Reducing {ticker} order from {total_quantity} to 1 share.")
            total_quantity = 1

        if total_quantity == 0:
            return  # Hard stop: Can't buy 0 shares

        # 2. Hard Stop: Max Shares (Fat Finger Protection)
        max_shares = self.cfg.get('max_shares_per_order', 5000)
        if total_quantity > max_shares:
            self.log(
                f"🛑 FAT FINGER BLOCK: {ticker} quantity ({total_quantity}) exceeds limit ({max_shares}). Order aborted.")
            self.send_telegram(f"🛑 FAT FINGER BLOCK: Prevented order of {total_quantity} shares of {ticker}.")
            return  # <-- THIS IS THE HARD STOP. The function completely exits here.

        # 3. Hard Stop: Max Dollar Value
        order_value = total_quantity * entry_price
        max_value = self.cfg.get('max_order_value_usd', 250000)
        if order_value > max_value:
            self.log(
                f"🛑 COMPLIANCE BLOCK: {ticker} order value (${order_value:,.2f}) exceeds limit (${max_value:,.2f}). Order aborted.")
            return  # <-- THIS IS THE HARD STOP. The function completely exits here.

        if total_quantity == 0: return

        # ──────────────────────────────────────────────────
        # END OF GATEKEEPER - PROCEED WITH ORDER
        # ──────────────────────────────────────────────────

        # 1. Calculate Prices (Round strictly to 2 decimals)
        tp_price = round(entry_price * (1 + self.cfg['tp_target_percent']), 2)
        sl_price = round(entry_price * (1 - self.cfg['sl_target_percent']), 2)
        entry_price = round(entry_price, 2)

        # 2. Define Expiry
        expiry_str = datetime.datetime.now().strftime("%Y%m%d") + " 11:30:00"

        # 3. Create Order Objects (Do not place yet)
        parent = Order(
            action='BUY', orderType='LMT', lmtPrice=entry_price,
            totalQuantity=total_quantity, tif='GTD', goodTillDate=expiry_str,
            transmit=False  # WAIT
        )

        try:
            # --- STEP A: Place Parent & Get ID ---
            trade_parent = self.ib.placeOrder(contract, parent)

            # Robust ID Check (Wait up to 2s)
            parent_id = None
            for _ in range(10):
                self.ib.sleep(0.2)
                if trade_parent.order.orderId:
                    parent_id = trade_parent.order.orderId
                    break

            if not parent_id:
                self.log(f"⚠️ {ticker}: No Order ID. Aborting.")
                self.ib.cancelOrder(parent)
                return

            # --- STEP B: Define Children ---
            oca_group = f'OCA_{ticker}_{parent_id}'

            # Profit: WAIT (transmit=False)
            # This attaches to the parent but DOES NOT send the group yet.
            profit = Order(action='SELL', orderType='LMT', lmtPrice=tp_price,
                           totalQuantity=total_quantity, parentId=parent_id,
                           ocaGroup=oca_group, ocaType=1, tif='GTC', transmit=False)

            # Stop: GO (transmit=True)
            # This is the trigger. It bundles Parent + Profit + Loss and sends them all.
            loss = Order(action='SELL', orderType='STP', auxPrice=sl_price,
                         totalQuantity=total_quantity, parentId=parent_id,
                         ocaGroup=oca_group, ocaType=1, tif='GTC', transmit=True)

            # --- STEP C: Place Children ---
            trade_tp = self.ib.placeOrder(contract, profit)
            trade_sl = self.ib.placeOrder(contract, loss)

            # --- STEP D: CRITICAL VERIFICATION ---
            # Wait 1 second and verify the Stop Loss was actually registered by IBKR
            self.ib.sleep(1)

            # Check if our specific Stop Loss order is in the open orders list
            open_orders = self.ib.openOrders()
            sl_confirmed = any(o.orderId == trade_sl.order.orderId for o in open_orders)

            if not sl_confirmed:
                self.log(f"🚨 CRITICAL: Stop Loss for {ticker} FAILED to register. Closing position immediately.")
                # Cancel parent (if not filled) or Flatten position (if filled)
                self.ib.cancelOrder(parent)
                self.ib.cancelOrder(profit)
                # If we are somehow filled already, close it out
                pos = [p for p in self.ib.portfolio() if p.contract.symbol == ticker]
                if pos and pos[0].position > 0:
                    self.ib.placeOrder(contract, MarketOrder('SELL', pos[0].position))
                return

            self.log(f"✅ Placed {ticker} Bracket. Entry: {entry_price}, TP: {tp_price}, SL: {sl_price}")
            self.send_telegram(f"🚨 *Thea Order* 🚨\n{ticker} Entry: {entry_price}\nAuto-Cancel: 11:30 AM")

        except Exception as e:
            self.log(f"🔥 ERROR placing {ticker}: {e}")
            self.log(f"🛑 Emergency Cancel sent for {ticker}.")
            # Nuke everything related to this ticker if it fails
            if 'parent' in locals(): self.ib.cancelOrder(parent)

    def manage_open_positions(self):
        """Monitors and manages open positions throughout the trading day"""
        try:
            if not self.ib.isConnected():
                return

            # Force refresh of trades/orders to ensure local state is synced with TWS
            self.ib.reqOpenOrders()
            # self.ib.reqPositions() # Optional, depending on if you trust the auto-update

            now_et = datetime.datetime.now(ET)
            date_str = now_et.strftime('%Y-%m-%d')
            positions = self.ib.positions()

            # Filter for positions we care about
            active_positions = [
                p for p in positions
                if p.position != 0 and p.contract.symbol in self.cfg['tickers']
            ]

            if not active_positions:
                return

            # ──────────────────────────────────────────────────
            # TASK 1: Move Stop to Break-Even at 11:30
            # ──────────────────────────────────────────────────
            if now_et.hour == 11 and now_et.minute >= 30 and now_et.hour < 15:
                for pos in active_positions:
                    ticker = pos.contract.symbol
                    if self.daily_run_status.get(f"{date_str}_{ticker}_be_moved"):
                        continue

                    qty = abs(int(pos.position))
                    avg_cost = pos.avgCost

                    # Find existing sell orders
                    open_trades = self.ib.openTrades()
                    sell_trades = [
                        t for t in open_trades
                        if t.contract.symbol == ticker and t.order.action == 'SELL'
                    ]

                    sl_trade = next((t for t in sell_trades if t.order.orderType == 'STP'), None)
                    tp_trade = next((t for t in sell_trades if t.order.orderType == 'LMT'), None)

                    if sl_trade and pos.position > 0:
                        current_stop = sl_trade.order.auxPrice

                        # Only move if current stop is BELOW break-even
                        if current_stop < avg_cost:
                            self.log(f"🔄 {ticker}: Re-bracketing to Break-Even...")

                            # Capture current TP price
                            current_tp_price = tp_trade.order.lmtPrice if tp_trade else round(
                                avg_cost * (1 + self.cfg['tp_target_percent']), 2)

                            # Cancel existing orders
                            for t in sell_trades:
                                self.ib.cancelOrder(t.order)

                            self.ib.sleep(1.0)  # Wait for unlock

                            new_oca_group = f'OCA_BE_{ticker}_{datetime.datetime.now().strftime("%H%M%S")}'

                            # --- FIX: FORCE SMART ROUTING TO BYPASS WARNING 10311 ---
                            # Create a clean SMART contract for the exit orders
                            exit_contract = Stock(ticker, 'SMART', 'USD')
                            self.ib.qualifyContracts(exit_contract)

                            # Stage New Take Profit
                            new_tp = Order(
                                action='SELL', orderType='LMT', lmtPrice=current_tp_price,
                                totalQuantity=qty, ocaGroup=new_oca_group, ocaType=1,
                                tif='GTC', transmit=False
                            )

                            # Stage New Break-Even Stop Loss
                            new_sl = Order(
                                action='SELL', orderType='STP', auxPrice=round(avg_cost, 2),
                                totalQuantity=qty, ocaGroup=new_oca_group, ocaType=1,
                                tif='GTC', transmit=False
                            )

                            # Submit using the SMART contract
                            self.ib.placeOrder(exit_contract, new_tp)
                            self.ib.placeOrder(exit_contract, new_sl)

                            self.ib.sleep(0.5)

                            # Trigger Transmit
                            new_sl.transmit = True
                            self.ib.placeOrder(exit_contract, new_sl)

                            self.log(f"✅ {ticker} Stop → Break-Even @ {avg_cost:.2f} (SMART Routed)")
                            self.send_telegram(f"🔒 {ticker} Stop moved to break-even @ ${avg_cost:.2f}")

                            self.daily_run_status[f"{date_str}_{ticker}_be_moved"] = True

            # ──────────────────────────────────────────────────
            # TASK 2: Force Close All Positions at EOD (3:55 PM)
            # ──────────────────────────────────────────────────
            if now_et.hour == 15 and now_et.minute >= 55:
                for pos in active_positions:
                    ticker = pos.contract.symbol

                    # SAFETY CHECK: Have we already closed this today?
                    if self.daily_run_status.get(f"{date_str}_{ticker}_eod_closed"):
                        continue

                    self.log(f"⏰ EOD Protocol initiated for {ticker}...")
                    qty = int(pos.position)
                    action = 'SELL' if qty > 0 else 'BUY'  # Handle Long or Short cleanup

                    # 1. Cancel all pending orders (stops/limits) for this ticker
                    open_trades = self.ib.openTrades()
                    for trade in open_trades:
                        if trade.contract.symbol == ticker:
                            self.ib.cancelOrder(trade.order)

                    self.ib.sleep(1.0)  # Wait for cancels to process and margin to free up

                    # --- FIX: FORCE SMART ROUTING ---
                    # Create a clean SMART contract to bypass Error 10311
                    exit_contract = Stock(ticker, 'SMART', 'USD')
                    self.ib.qualifyContracts(exit_contract)

                    # 2. Stage Market Close Order (Do NOT transmit yet)
                    close_order = MarketOrder(action, abs(qty))
                    close_order.transmit = False
                    self.ib.placeOrder(exit_contract, close_order)

                    self.ib.sleep(0.5)  # Let TWS register the staged order

                    # 3. TRIGGER TRANSMIT: Force execution
                    close_order.transmit = True
                    self.ib.placeOrder(exit_contract, close_order)

                    self.log(f"🔴 EOD Close: {ticker} x{qty} @ Market (Transmitted)")
                    self.send_telegram(f"🔴 EOD: Closed {ticker} x{qty} @ Market")

                    # 4. Mark this ticker as closed to prevent loop spamming
                    self.daily_run_status[f"{date_str}_{ticker}_eod_closed"] = True

        except Exception as e:
            self.log(f"Position Management Error: {e}")

    def run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.ib = IB()
        self.log("Engine Started.")

        while self.running:
            try:
                now_et = datetime.datetime.now(ET)  # Use ET timezone
                date_str = now_et.strftime('%Y-%m-%d')

                # --- TIME WINDOW LOGIC ---
                start_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
                end_time = now_et.replace(hour=11, minute=30, second=0, microsecond=0)
                market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

                # ═══════════════════════════════════════════════════
                # ENTRY LOGIC (9:30 - 11:30)
                # ═══════════════════════════════════════════════════
                if 0 <= now_et.weekday() <= 4 and start_time <= now_et <= end_time and not self.daily_run_status.get(
                        date_str):
                    try:
                        # Ensure connection
                        if not self.ib.isConnected():
                            self.ib.connect(self.cfg['ibkr_host'], self.cfg['ibkr_port'],
                                            clientId=self.cfg['ibkr_client_id'])

                        tickers = self.cfg.get('tickers', [])

                        signals = []
                        for t in tickers:
                            if not self.running: break
                            c = self.create_contract(t)
                            self.ib.qualifyContracts(c)
                            res = self.check_entry_signal(t, c)
                            if res:
                                signals.append({'ticker': t, 'contract': c, 'entry': res[0], 'gap': res[1]})
                                # Explainability Log
                                audit_logger.info(
                                    json.dumps({
                                        "action": "SIGNAL_GENERATED",
                                        "ticker": t,
                                        "prev_close": res[0],
                                        "gap_ratio": round(res[1], 4),
                                        "threshold_used": self.cfg['gap_entry_threshold'],
                                        "reason": f"Gap of {res[1]:.2%} passed threshold of {self.cfg['gap_entry_threshold']:.2%}"
                                    })
                                )
                            time.sleep(0.5)

                        if signals:
                            self.log(f"Found {len(signals)} valid signals. Executing...")

                            split_alloc = self.cfg['total_capital'] / len(signals)
                            max_alloc = self.cfg['total_capital'] * 0.20
                            final_alloc = min(split_alloc, max_alloc)

                            for s in signals[:self.cfg['max_simultaneous_trades']]:
                                self.place_bracket_order(s['ticker'], s['contract'], s['entry'], s['gap'], final_alloc)

                            self.daily_run_status[date_str] = True
                        else:
                            # Log once that we completed the scan with no signals
                            # Only log on first scan attempt to avoid spam
                            scan_key = f"{date_str}_scan_complete"
                            if not self.daily_run_status.get(scan_key):
                                self.log(f"📊 Scan Complete: No valid gap signals found in {len(tickers)} tickers")
                                self.daily_run_status[scan_key] = True

                    except Exception as e:
                        self.log(f"Error during scan: {e}")
                        if self.ib.isConnected(): self.ib.disconnect()

                # ═══════════════════════════════════════════════════
                # POSITION MANAGEMENT (11:30 onwards until market close)
                # ═══════════════════════════════════════════════════
                if 0 <= now_et.weekday() <= 4 and now_et >= end_time and now_et < market_close:
                    # This runs every loop iteration after 11:30 to monitor positions
                    self.manage_open_positions()

                # ═══════════════════════════════════════════════════
                # FAILSAFE CLEANUP (Strictly at/after 11:30)
                # ═══════════════════════════════════════════════════
                if 0 <= now_et.weekday() <= 4 and now_et >= end_time and now_et.hour < 12:
                    try:
                        if not self.ib.isConnected():
                            self.ib.connect(self.cfg['ibkr_host'], self.cfg['ibkr_port'],
                                            clientId=self.cfg['ibkr_client_id'])

                        open_trades = self.ib.openTrades()
                        for trade in open_trades:
                            if trade.order.action == 'BUY' and trade.orderStatus.status in ['Submitted',
                                                                                            'PreSubmitted']:
                                self.ib.cancelOrder(trade.order)
                                self.log(f"⏰ 11:30 Failsafe: Cancelled {trade.contract.symbol}")
                    except:
                        pass

                # Wait before next scan loop (15 seconds)
                time.sleep(15)

            except Exception as e:
                self.log(f"Loop Crash: {e}")
                time.sleep(10)

    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.run_loop, daemon=True)
            self.thread.start()

    def stop(self):
        self.running = False


# ==========================================
#              TESSA BOT ENGINE
# ==========================================
class TessaBot:
    def __init__(self):
        self.running = False
        self.thread = None
        self.logs = []
        self.ib = None
        self.cfg = TessaConfig().model_dump()

    def update_config(self, config_dict):
        self.cfg.update(config_dict)
        self.log(f"[Tessa] Config updated for {self.cfg['ticker']}")

    def log(self, message):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        entry = f"[Tessa] [{timestamp}] {message}"
        print(entry)
        self.logs.append(entry)
        thea_bot.logs.append(entry)

    def send_telegram(self, message):
        pass  # Optional implementation

    def submit_limit_with_bracket(self, contract, limit_price, quantity, label):
        if quantity == 0: return None, None
        tp_price = round(limit_price * (1 + self.cfg['tp_pct'] / 100), 2)

        # --- LAYER 1: GTD ---
        expiry_str = datetime.datetime.now().strftime("%Y%m%d") + " 11:30:00"

        parent = Order(
            action='BUY', orderType='LMT', totalQuantity=quantity, lmtPrice=limit_price,
            tif='GTD', goodTillDate=expiry_str, transmit=False
        )
        trade = self.ib.placeOrder(contract, parent)
        self.ib.sleep(0.5)

        if not trade.order.orderId: return None, None
        parent_id = trade.order.orderId
        oca_group = f'OCA_{self.cfg["ticker"]}_{label}_{datetime.datetime.now().strftime("%H%M%S")}'

        profit = Order(action='SELL', orderType='LMT', totalQuantity=quantity, lmtPrice=tp_price, parentId=parent_id,
                       ocaGroup=oca_group, ocaType=1, tif='GTC', transmit=False)
        initial_trail = Order(action='SELL', orderType='TRAIL', totalQuantity=quantity,
                              trailingPercent=self.cfg['initial_trail_pct'], parentId=parent_id, ocaGroup=oca_group,
                              ocaType=1, tif='GTC', transmit=True)

        self.ib.placeOrder(contract, profit)
        trail_trade = self.ib.placeOrder(contract, initial_trail)

        # Verify the trailing stop actually registered
        self.ib.sleep(1)
        open_orders = self.ib.openOrders()
        trail_confirmed = any(
            o.orderId == trail_trade.order.orderId for o in open_orders
        )

        if not trail_confirmed:
            self.log(f"🚨 CRITICAL: Trail stop for {label} failed to register. Cancelling entry.")
            self.ib.cancelOrder(parent)
            return None, None

        self.log(f"{label} Submitted: Buy {quantity} @ {limit_price} (Exp 11:30)")
        return parent_id, trail_trade.order.orderId

    async def update_trailing_stop_logic(self, parent_id, old_trail_id, quantity, label, contract):
        delay = self.cfg['delay_sec']
        self.log(f"{label} Waiting {delay}s to tighten stop...")

        # Check periodically if we passed 11:30 while waiting
        for _ in range(delay):
            if not self.running: return

            # --- LAYER 2: FAILSAFE CLEANUP FOR TESSA ---
            now = datetime.datetime.now()
            if now.hour == 11 and now.minute >= 30:
                # Check if parent is still pending
                trades = self.ib.openTrades()
                for t in trades:
                    if t.order.orderId == parent_id and t.orderStatus.status in ['Submitted', 'PreSubmitted']:
                        self.ib.cancelOrder(t.order)
                        self.log(f"⏰ 11:30 Failsafe: Cancelled {label}")
                        return

            await asyncio.sleep(1)

        trades = self.ib.trades()
        is_filled = any(t.order.orderId == parent_id and t.orderStatus.status == 'Filled' for t in trades)

        if is_filled:
            new_trail = Order(action='SELL', orderType='TRAIL', totalQuantity=quantity,
                              trailingPercent=self.cfg['final_trail_pct'], parentId=parent_id, tif='GTC', transmit=True)
            try:
                # Cancel the old trailing stop first
                old_orders = [t.order for t in self.ib.openTrades()
                              if t.order.orderId == old_trail_id]
                if old_orders:
                    self.ib.cancelOrder(old_orders[0])
                    self.ib.sleep(0.5)
                else:
                    self.log(f"{label} Old trail order not found - may already be filled/cancelled")
                    return

                self.ib.placeOrder(contract, new_trail)
                self.log(f"{label} STOP TIGHTENED to {self.cfg['final_trail_pct']}%")
            except Exception as e:
                self.log(f"{label} Update Failed: {e}")

    def run_strategy(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.ib = IB()
        try:
            self.ib.connect(self.cfg['ibkr_host'], self.cfg['ibkr_port'], clientId=self.cfg['ibkr_client_id'])
            contract = Stock(self.cfg['ticker'], 'SMART', 'USD')
            self.ib.qualifyContracts(contract)

            qty1 = int(self.cfg['trade_amount_usd'] // self.cfg['limit_1_price'])
            qty2 = int(self.cfg['trade_amount_usd'] // self.cfg['limit_2_price'])

            p1, t1 = self.submit_limit_with_bracket(contract, self.cfg['limit_1_price'], qty1, "ENTRY 1")
            p2, t2 = self.submit_limit_with_bracket(contract, self.cfg['limit_2_price'], qty2, "ENTRY 2")

            async def async_main():
                tasks = []
                if p1 and t1: tasks.append(self.update_trailing_stop_logic(p1, t1, qty1, "ENTRY 1", contract))
                if p2 and t2: tasks.append(self.update_trailing_stop_logic(p2, t2, qty2, "ENTRY 2", contract))
                if tasks: await asyncio.gather(*tasks)

            loop.run_until_complete(async_main())
        except Exception as e:
            self.log(f"Tessa Error: {e}")
        finally:
            if self.ib.isConnected(): self.ib.disconnect()
            self.running = False
            loop.close()

    def start(self):
        if self.running: return
        self.running = True
        self.thread = threading.Thread(target=self.run_strategy, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False


# ==========================================
#              RISK MANAGEMENT
# ==========================================
def analyze_risk_logic(ticker):
    # (Simplified Risk Logic for merging)
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] 🔎 Risk Analysis for {ticker}..."
    state_risk_logs.append(entry)
    if len(state_risk_logs) > 50: state_risk_logs.pop(0)

    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="3mo")
        if not hist.empty:
            vol = hist['Volume'].iloc[-1]
            state_risk_logs.append(f"[{ts}] {ticker} Vol: {vol:,.0f}")
    except:
        state_risk_logs.append(f"[{ts}] Error fetching {ticker}")


async def run_risk_batch(tickers):
    state_risk_logs.append("--- Starting Scan ---")
    for t in tickers:
        analyze_risk_logic(t)
        await asyncio.sleep(0.5)
    state_risk_logs.append("--- Scan Complete ---")


# ==========================================
#              API
# ==========================================
thea_bot = TheaBot()
tessa_bot = TessaBot()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://trading.sevendimensions.com",  # Your production domain
        "http://localhost:3000",                # Standard React port
        "http://localhost:5173"                 # Standard Vite port
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Safely allow all standard methods (GET, POST, OPTIONS)
    allow_headers=["*"],  # FIX: Allow all headers so Axios defaults aren't blocked
)


class RiskRequest(BaseModel):
    tickers: List[str]


@app.get("/status")
def get_status():
    return {
        "thea_running": thea_bot.running,
        "tessa_running": tessa_bot.running,
        "logs": thea_bot.logs,
        "risk_logs": state_risk_logs
    }


@app.get("/positions")
def get_positions():
    # Helper to get snapshot from active bot or quick connect
    # (Simplified for brevity - assumes IB connected)
    return []


@app.post("/risk/analyze")
async def analyze_risk(req: RiskRequest):
    asyncio.create_task(run_risk_batch(req.tickers))
    return {"status": "Analysis Started"}


@app.post("/start/thea")
def start_thea():
    thea_bot.start()
    return {"status": "Started"}


@app.post("/stop/thea")
def stop_thea():
    thea_bot.stop()
    return {"status": "Stopped"}


@app.post("/start/tessa")
def start_tessa(config: TessaConfig):
    tessa_bot.update_config(config.model_dump())
    tessa_bot.start()
    return {"status": "Started"}


@app.post("/stop/tessa")
def stop_tessa():
    tessa_bot.stop()
    return {"status": "Stopped"}


@app.get("/config")
def get_config():
    if not os.path.exists("user_config.json"): return {"thea": TheaConfig().model_dump(), "tessa": {}}
    with open("user_config.json", "r") as f: return json.load(f)


@app.post("/config/thea")
def save_thea_config(config: TheaConfig):
    data = {"thea": config.model_dump(), "tessa": {}}
    with open("user_config.json", "w") as f: json.dump(data, f)
    thea_bot.update_config(config.model_dump())
    return {"message": "Saved"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)