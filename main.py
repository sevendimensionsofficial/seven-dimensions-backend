"""
=============================================================================
PROPRIETARY TRADING SYSTEM: SEVEN DIMENSIONS PTF
MODULE: ALGORITHMIC EXECUTION ENGINE (THEA / TESSA)
OWNER: Joel Faucher
COMPLIANCE STATUS: DMCC / JCA AUDIT READY
DESCRIPTION: Automated equity execution with hard-coded pre-trade controls.
=============================================================================
"""
import os, sys, json, datetime, logging, threading, time, requests, asyncio, nest_asyncio
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
from ib_insync import *
from zoneinfo import ZoneInfo
import jwt
import boto3
import uvicorn
from botocore.exceptions import ClientError


def get_secrets_by_path():
    """
    Fetches all parameters from the SSM hierarchy (Auth, Compliance, Engine).
    This is the 'Free' and 'Path-Compatible' method for your audit setup.
    """
    # Initialize the SSM client (ensure region matches your EC2)
    ssm = boto3.client('ssm', region_name="us-east-1")

    # The root path you've organized in the AWS Console
    base_path = "/SevenDimensions/PROD/"

    try:
        # 'Recursive=True' allows it to find /Auth, /Compliance, and /TradingEngine
        response = ssm.get_parameters_by_path(
            Path=base_path,
            Recursive=True,
            WithDecryption=True
        )

        # Mapping logic: Removes the path prefix so main.py sees the raw Key
        # Example: '/SevenDimensions/PROD/Auth/AUTH0_DOMAIN' becomes 'AUTH0_DOMAIN'
        secrets = {p['Name'].split('/')[-1]: p['Value'] for p in response['Parameters']}

        if not secrets:
            print(f"⚠️ WARNING: No parameters found at {base_path}")

        return secrets

    except Exception as e:
        print(f"❌ SSM Path Fetch Error: {e}")
        raise e

# --- 2. DUAL-MODE CONFIGURATION ---
ENV_MODE = os.getenv("ENV_MODE", "TEST").upper()

if ENV_MODE == "PROD":
    print("🚀 PROD MODE: Fetching secrets from AWS Secrets Manager...")
    try:
        secrets = get_secrets_by_path()
        for key, value in secrets.items():
            os.environ[key] = str(value)
    except Exception as e:
        print(f"❌ SECRET FETCH FAILED: {e}")
        sys.exit(1)
else:
    # This only prints ONCE now
    print("🛠️ TEST MODE: Loading local trading.env...")
    load_dotenv('trading.env')

# --- 3. DYNAMIC CONSTANTS (Loaded after environment is set) ---
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
API_AUDIENCE = os.getenv("API_AUDIENCE")
ALGORITHMS = os.getenv("ALGORITHMS", "RS256").split(",")
SYSTEM_OWNER = os.getenv('SYSTEM_OWNER', 'Joel Faucher')

security = HTTPBearer()

if ENV_MODE == "PROD":
    print("🚀 PROD MODE: Fetching secrets from AWS Secrets Manager...")
    # Here you would call a helper function to fetch secrets and
    # inject them into os.environ.
    # example_fetch_aws_secrets()
else:
    print("🛠️ TEST MODE: Loading local trading.env...")
    load_dotenv('trading.env')

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Validates the JWT token against Auth0's public keys."""
    token = credentials.credentials
    try:
        # Fetch Auth0 Public Keys
        jwks_url = f'https://{AUTH0_DOMAIN}/.well-known/jwks.json'
        jwks = requests.get(jwks_url).json()
        unverified_header = jwt.get_unverified_header(token)

        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]
                }
                break

        if rsa_key:
            # Decode and validate the token
            payload = jwt.decode(
                token,
                jwt.algorithms.RSAAlgorithm.from_jwk(rsa_key),
                algorithms=ALGORITHMS,
                audience=API_AUDIENCE,
                issuer=f"https://{AUTH0_DOMAIN}/"
            )
            return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTClaimsError:
        raise HTTPException(status_code=401, detail="Invalid claims (check audience/issuer)")
    except Exception as e:
        raise HTTPException(status_code=401, detail="Unable to parse authentication token.")

    raise HTTPException(status_code=401, detail="Invalid token.")

ET = ZoneInfo("America/New_York")

# --- 1. SETUP & CONFIG ---
nest_asyncio.apply()

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
    ibkr_host: str = os.getenv('IBKR_HOST', '127.0.0.1')
    ibkr_port: int = int(os.getenv('IBKR_PORT', 7497))
    ibkr_client_id: int = 2
    scanner_gap_down_threshold: float = float(os.getenv('SCANNER_GAP_DOWN_THRESHOLD', -4.5))
    allocation_per_order: float = float(os.getenv('ALLOCATION_PER_ORDER', 25000))
    entry_2_offset_pct: float = float(os.getenv('ENTRY_2_OFFSET_PCT', 0.985))
    tessa_take_profit_pct: float = float(os.getenv('TESSA_TAKE_PROFIT_PCT', 1.035))
    tessa_stop_loss_pct: float = float(os.getenv('TESSA_STOP_LOSS_PCT', 0.97))
    min_avg_volume: int = int(os.getenv('MIN_AVG_VOLUME', 500000))
    min_volatility_pct: float = float(os.getenv('MIN_VOLATILITY_PCT', 1.5))
    pilot_mode: bool = True
    max_trades: int = int(os.getenv('TESSA_MAX_TRADES', 10))


# --- RISK MGMT GLOBALS ---
state_risk_logs = []

BAD_NEWS_KEYWORDS = [
    'lawsuit', 'fraud', 'investigation', 'subpoena', 'bankruptcy', 'insolvency',
    'misses', 'missed', 'lower', 'downgrade', 'cut', 'warning', 'weak',
    'disappointing', 'halt', 'breach', 'recall', 'plunge', 'crash', 'sell-off',
    'offering', 'pricing', 'priced', 'public offering', 'private placement', 'dilution',
    'clinical hold', 'crl', 'rejected', 'failed', 'negative data', 'suspended'
]

GOOD_NEWS_KEYWORDS = [
    'beat', 'beats', 'higher', 'upgrade', 'raised', 'strong', 'record',
    'partnership', 'agreement', 'approval', 'launch', 'buyback',
    'dividend', 'surges', 'jumps', 'awarded', 'fast track', 'orphan drug', 'clearance'
]

# ==========================================
#              KILL SWITCH
# ==========================================

class TradingGuard:
    def __init__(self, broker_api, bot_name):
        self.api = broker_api
        self.bot_name = bot_name
        self.kill_signal_path = "/tmp/kill_switch.signal"

    def check_kill_switch(self):
        # As a Linux Admin, you can trigger this via: touch /tmp/kill_switch.signal
        if os.path.exists(self.kill_signal_path):
            print(f"!!! {self.bot_name} KILL SWITCH ACTIVATED !!!")
            self.execute_emergency_halt()

    def execute_emergency_halt(self):
        # 1. Cancel all outstanding orders immediately
        try:
            if self.api.isConnected():
                self.api.reqGlobalCancel()
                print("🚨 Global Cancel Sent.")
        except Exception as e:
            print(f"Emergency Cancel Error: {e}")

        # 2. Flatten all open positions
        try:
            if self.api.isConnected():
                positions = self.api.positions()
                for pos in positions:
                    if pos.position != 0:
                        ticker = pos.contract.symbol
                        qty = abs(int(pos.position))
                        action = 'SELL' if pos.position > 0 else 'BUY'

                        # Create a clean SMART contract to bypass routing errors
                        contract = Stock(ticker, 'SMART', 'USD')
                        self.api.qualifyContracts(contract)

                        # Place Market Order
                        market_order = MarketOrder(action, qty)
                        self.api.placeOrder(contract, market_order)
                        print(f"🚨 EMERGENCY LIQUIDATION: {action} {qty} {ticker} @ MARKET")

                # CRITICAL: Wait 2 seconds to ensure IBKR receives the market orders before the thread dies
                self.api.sleep(2.0)
        except Exception as e:
            print(f"Emergency Liquidation Error: {e}")

        # 3. Log the event for the compliance audit trail
        audit_logger.critical(json.dumps({
            "event": "EMERGENCY_KILL_SWITCH_ACTIVATED",
            "bot": self.bot_name,
            "action": "reqGlobalCancel sent. All positions flattened. Thread forcefully terminated."
        }))

        # 4. Hard Exit (Kills the background thread entirely)
        sys.exit(1)

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
        with open("thea_terminal.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d')} {entry}\n")

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

            self.ib.reqOpenOrders()

            now_et = datetime.datetime.now(ET)
            date_str = now_et.strftime('%Y-%m-%d')

            # --- UPDATED: Switch from positions() to portfolio() to get live PnL ---
            portfolio = self.ib.portfolio()

            active_positions = [
                p for p in portfolio
                if p.position != 0 and p.contract.symbol in self.cfg['tickers']
            ]

            if not active_positions:
                return

            # ──────────────────────────────────────────────────
            # TASK 1: 11:30 AM Time-Based Risk Management
            # ──────────────────────────────────────────────────
            if now_et.hour == 11 and now_et.minute >= 30 and now_et.hour < 15:
                for pos in active_positions:
                    ticker = pos.contract.symbol

                    # Skip if already managed today
                    if self.daily_run_status.get(f"{date_str}_{ticker}_be_moved") or \
                            self.daily_run_status.get(f"{date_str}_{ticker}_1130_closed"):
                        continue

                    qty = abs(int(pos.position))
                    avg_cost = pos.averageCost  # Note: PortfolioItem uses 'averageCost'
                    unrealized_pnl = pos.unrealizedPNL

                    open_trades = self.ib.openTrades()
                    sell_trades = [t for t in open_trades if t.contract.symbol == ticker and t.order.action == 'SELL']

                    # --- NEW: LIQUIDATE IF AT A LOSS ---
                    if unrealized_pnl is not None and unrealized_pnl < 0:
                        self.log(f"📉 {ticker} is at a loss (${unrealized_pnl:.2f}). Liquidating at 11:30 AM failsafe.")

                        # Cancel existing brackets
                        for t in sell_trades: self.ib.cancelOrder(t.order)
                        self.ib.sleep(1.0)

                        exit_contract = Stock(ticker, 'SMART', 'USD')
                        self.ib.qualifyContracts(exit_contract)
                        action = 'SELL' if pos.position > 0 else 'BUY'

                        close_order = MarketOrder(action, qty)
                        self.ib.placeOrder(exit_contract, close_order)

                        self.send_telegram(f"📉 11:30 Failsafe: Liquidated {ticker} at a loss.")
                        self.daily_run_status[f"{date_str}_{ticker}_1130_closed"] = True
                        continue  # Skip break-even logic, move to next ticker

                    # --- EXISTING: MOVE STOP TO BREAK-EVEN IF PROFITABLE ---
                    sl_trade = next((t for t in sell_trades if t.order.orderType == 'STP'), None)
                    tp_trade = next((t for t in sell_trades if t.order.orderType == 'LMT'), None)

                    if sl_trade and pos.position > 0:
                        current_stop = sl_trade.order.auxPrice

                        if current_stop < avg_cost:
                            self.log(f"🔄 {ticker}: Re-bracketing to Break-Even...")
                            current_tp_price = tp_trade.order.lmtPrice if tp_trade else round(
                                avg_cost * (1 + self.cfg['tp_target_percent']), 2)

                            for t in sell_trades: self.ib.cancelOrder(t.order)
                            self.ib.sleep(1.0)

                            new_oca_group = f'OCA_BE_{ticker}_{datetime.datetime.now().strftime("%H%M%S")}'
                            exit_contract = Stock(ticker, 'SMART', 'USD')
                            self.ib.qualifyContracts(exit_contract)

                            new_tp = Order(action='SELL', orderType='LMT', lmtPrice=current_tp_price, totalQuantity=qty,
                                           ocaGroup=new_oca_group, ocaType=1, tif='GTC', transmit=False)
                            new_sl = Order(action='SELL', orderType='STP', auxPrice=round(avg_cost, 2),
                                           totalQuantity=qty, ocaGroup=new_oca_group, ocaType=1, tif='GTC',
                                           transmit=False)

                            self.ib.placeOrder(exit_contract, new_tp)
                            self.ib.placeOrder(exit_contract, new_sl)
                            self.ib.sleep(0.5)

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

        # 1. Instantiate the Guard outside the loop
        guard = TradingGuard(self.ib, "THEA")

        # 2. SINGLE UNIFIED LOOP
        while self.running:
            try:
                # --- KILL SWITCH CHECK ---
                guard.check_kill_switch()

                now_et = datetime.datetime.now(ET)
                date_str = now_et.strftime('%Y-%m-%d')

                # --- TIME WINDOW LOGIC ---
                start_time = now_et.replace(
                    hour=self.cfg.get('trade_hour', 9),
                    minute=self.cfg.get('trade_minute', 47),
                    second=0, microsecond=0
                )
                end_time = now_et.replace(hour=11, minute=30, second=0, microsecond=0)
                market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

                # ═══════════════════════════════════════════════════
                # ENTRY LOGIC (9:30 - 11:30)
                # ═══════════════════════════════════════════════════
                if 0 <= now_et.weekday() <= 4 and start_time <= now_et <= end_time and not self.daily_run_status.get(
                        date_str):
                    try:
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
                                audit_logger.info(json.dumps({
                                    "action": "SIGNAL_GENERATED", "ticker": t, "prev_close": res[0],
                                    "gap_ratio": round(res[1], 4), "threshold_used": self.cfg['gap_entry_threshold'],
                                    "reason": f"Gap passed threshold"
                                }))
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
                            scan_key = f"{date_str}_scan_complete"
                            if not self.daily_run_status.get(scan_key):
                                self.log(f"📊 Scan Complete: No valid gap signals found in {len(tickers)} tickers")
                                self.daily_run_status[scan_key] = True

                    except Exception as e:
                        self.log(f"Error during scan: {e}")
                        if self.ib.isConnected(): self.ib.disconnect()

                # ═══════════════════════════════════════════════════
                # POSITION MANAGEMENT (11:30 onwards)
                # ═══════════════════════════════════════════════════
                if 0 <= now_et.weekday() <= 4 and now_et >= end_time and now_et < market_close:
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

                self.ib.sleep(15)

            except Exception as e:
                self.log(f"Loop Crash: {e}")
                self.ib.sleep(10)
        # ──────────────────────────────────────────────────
        # CLEANUP: Executes only when self.running becomes False
        # ──────────────────────────────────────────────────
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            self.log("🛑 Engine Stopped. Disconnected from IBKR successfully.")

    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.run_loop, daemon=True)
            self.thread.start()

    def stop(self):
        self.running = False


# ==========================================
#              TESSA BOT ENGINE (AUTONOMOUS)
# ==========================================
def get_gap_down_stocks(threshold_pct):
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"formatted": "false", "lang": "en-US", "region": "US", "scrIds": "day_losers", "count": "50"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        quotes = response.json()['finance']['result'][0]['quotes']
        signals = []
        for q in quotes:
            change_pct = q.get('regularMarketChangePercent', 0)
            if change_pct <= threshold_pct:
                signals.append(
                    {'ticker': q.get('symbol'), 'price': q.get('regularMarketPrice', 0), 'change_pct': change_pct})
        return signals
    except Exception as e:
        print(f"⚠️ Scanner Error: {e}")
        return []


def fetch_finviz_news(ticker):
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'lxml')
        news_table = soup.find(id='news-table')
        if not news_table: return []
        return [{'title': row.a.text} for row in news_table.find_all('tr')[:5] if row.a]
    except:
        return []


def assess_stock(ticker, cfg, log_func) -> bool:
    log_func(f"🔎 Analyzing Risk Profile: {ticker}")
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="3mo")
        if hist.empty or len(hist) < 2:
            log_func(f"   ❌ Rejected: Insufficient historical data")
            return False

        if hist['Close'].iloc[-1] < 10.0:
            log_func(f"   ❌ Rejected: Price below $10 (${hist['Close'].iloc[-1]:.2f})")
            return False

        yellow_flags = 0
        median_vol = hist.iloc[:-1]['Volume'].median()
        if median_vol < cfg['min_avg_volume']:
            log_func(f"   ❌ Rejected: Low Volume ({median_vol:,.0f})")
            return False
        elif median_vol <= 1_000_000:
            yellow_flags += 1

        hist['Range_Pct'] = (hist['High'] - hist['Low']) / hist['Open'] * 100
        avg_vol = hist['Range_Pct'].mean()
        if avg_vol < cfg['min_volatility_pct']:
            log_func(f"   ❌ Rejected: Low Volatility ({avg_vol:.2f}%)")
            return False
        elif avg_vol < 2.5:
            yellow_flags += 1

        try:
            earnings = t.earnings_history
            if earnings is not None and not earnings.empty:
                surprises = earnings.tail(4)['surprisePercent'].dropna()
                if not surprises.empty:
                    beats = sum(surprises > 0)
                    if beats <= 1:
                        log_func(f"   ❌ Rejected: Weak Earnings History (<=1 Beat)")
                        return False
                    elif beats == 2:
                        yellow_flags += 1
        except:
            pass

        news = fetch_finviz_news(ticker)
        if sum(1 for item in news for kw in BAD_NEWS_KEYWORDS if kw in item.get('title', '').lower()) > 0:
            log_func(f"   ❌ Rejected: Dangerous Dilution/Negative News Detected.")
            return False

        if yellow_flags >= 2:
            log_func(f"   ❌ Rejected: Accumulated {yellow_flags} mediocre (yellow) metrics.")
            return False

        log_func(f"   ✅ APPROVED: {ticker} passed strict LOW RISK filters.")
        return True
    except Exception as e:
        log_func(f"   ⚠️ Assessment Error for {ticker}: {e}")
        return False


class TessaBot:
    def __init__(self):
        self.ib = None
        self.running = False
        self.thread = None
        self.daily_run_status = {}
        self.logs = []
        self.cfg = TessaConfig().model_dump()

    def update_config(self, config_dict):
        self.cfg.update(config_dict)
        self.log("Config updated.")

    def connect(self):
        if not self.ib.isConnected():
            self.ib.connect(self.cfg['ibkr_host'], self.cfg['ibkr_port'], clientId=self.cfg['ibkr_client_id'])

    def log(self, msg):
        ts = datetime.datetime.now(ET).strftime('%H:%M:%S')
        entry = f"[Tessa] [{ts}] {msg}"
        print(entry)
        self.logs.append(entry)
        if len(self.logs) > 100: self.logs.pop(0)
        with open("tessa_terminal.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.datetime.now(ET).strftime('%Y-%m-%d')} {entry}\n")

    def place_dual_bracket(self, ticker, current_price):
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        entry1 = round(current_price, 2)
        qty1 = int(self.cfg['allocation_per_order'] // entry1)

        entry2 = round(current_price * self.cfg['entry_2_offset_pct'], 2)
        qty2 = int(self.cfg['allocation_per_order'] // entry2)

        # --- NEW: ENFORCE PILOT MODE ---
        if self.cfg.get('pilot_mode'):
            self.log(f"🛡️ PILOT MODE ACTIVE: Reducing {ticker} orders to 1 share.")
            qty1 = 1
            qty2 = 1

        if qty1 > 0: self._submit_bracket(contract, ticker, entry1, qty1, "LMT1_OPEN")
        if qty2 > 0: self._submit_bracket(contract, ticker, entry2, qty2, "LMT2_DIP")

    def _submit_bracket(self, contract, ticker, entry_price, qty, label):
        tp_price = round(entry_price * self.cfg['tessa_take_profit_pct'], 2)
        sl_price = round(entry_price * self.cfg['tessa_stop_loss_pct'], 2)

        # --- NEW: Native 11:30 AM Auto-Cancel (Matches Thea exactly) ---
        expiry_str = datetime.datetime.now(ET).strftime("%Y%m%d") + " 11:30:00"

        parent = Order(
            action='BUY', orderType='LMT', totalQuantity=qty, lmtPrice=entry_price,
            tif='GTD', goodTillDate=expiry_str, transmit=False
        )
        trade = self.ib.placeOrder(contract, parent)
        self.ib.sleep(0.5)

        if not trade.order.orderId: return
        parent_id = trade.order.orderId
        oca = f"OCA_{ticker}_{label}_{parent_id}"

        # Ensure children are GTC so they remain active after the parent fills
        profit = Order(action='SELL', orderType='LMT', totalQuantity=qty, lmtPrice=tp_price, parentId=parent_id, ocaGroup=oca, ocaType=1, tif='GTC', transmit=False)
        loss = Order(action='SELL', orderType='STP', auxPrice=sl_price, totalQuantity=qty, parentId=parent_id, ocaGroup=oca, ocaType=1, tif='GTC', transmit=True)

        self.ib.placeOrder(contract, profit)
        self.ib.placeOrder(contract, loss)
        self.log(f"🟢 Placed {label} for {ticker}: Entry ${entry_price} | TP ${tp_price} | SL ${sl_price}")

    def manage_positions(self):
        if not self.ib.isConnected(): return
        now = datetime.datetime.now(ET)
        date_str = now.strftime('%Y-%m-%d')

        traded_today = self.daily_run_status.get(f"{date_str}_traded_tickers", set())

        # --- UPDATED: Switch from positions() to portfolio() to get live PnL ---
        portfolio = self.ib.portfolio()
        positions = [p for p in portfolio if p.position != 0 and p.contract.symbol in traded_today]

        if now.hour == 11 and now.minute >= 30 and now.hour < 15:
            # --- EXISTING: CANCEL UNFILLED BUY ORDERS ---
            if not self.daily_run_status.get(f"{date_str}_unfilled_cancelled"):
                cancelled_count = 0
                for trade in self.ib.openTrades():
                    if trade.contract.symbol in traded_today and trade.order.action == 'BUY' and trade.orderStatus.status in [
                        'Submitted', 'PreSubmitted']:
                        self.ib.cancelOrder(trade.order)
                        self.log(f"⏰ 11:30 Failsafe: Cancelled unfilled entry for {trade.contract.symbol}")
                        cancelled_count += 1

                if cancelled_count > 0: self.ib.sleep(2.0)
                self.daily_run_status[f"{date_str}_unfilled_cancelled"] = True

            # --- 11:30 AM POSITION LOGIC ---
            for pos in positions:
                ticker = pos.contract.symbol

                # Skip if already managed today
                if self.daily_run_status.get(f"{date_str}_{ticker}_be_moved") or \
                        self.daily_run_status.get(f"{date_str}_{ticker}_1130_closed"):
                    continue

                avg_cost = pos.averageCost  # Note: PortfolioItem uses 'averageCost'
                unrealized_pnl = pos.unrealizedPNL

                sell_trades = [t for t in self.ib.openTrades() if
                               t.contract.symbol == ticker and t.order.action == 'SELL']

                # --- NEW: LIQUIDATE IF AT A LOSS ---
                if unrealized_pnl is not None and unrealized_pnl < 0:
                    self.log(f"📉 {ticker} is at a loss (${unrealized_pnl:.2f}). Liquidating at 11:30 AM failsafe.")
                    for t in sell_trades: self.ib.cancelOrder(t.order)
                    self.ib.sleep(1.0)

                    exit_contract = Stock(ticker, 'SMART', 'USD')
                    self.ib.qualifyContracts(exit_contract)
                    action = 'SELL' if pos.position > 0 else 'BUY'

                    self.ib.placeOrder(exit_contract, MarketOrder(action, abs(pos.position)))
                    self.daily_run_status[f"{date_str}_{ticker}_1130_closed"] = True
                    continue  # Skip break-even logic, move to next ticker

                # --- EXISTING: MOVE STOP TO BREAK-EVEN IF PROFITABLE ---
                sl_trade = next((t for t in sell_trades if t.order.orderType == 'STP'), None)
                if sl_trade and sl_trade.order.auxPrice < avg_cost:
                    self.log(f"🔄 Moving {ticker} Stop Loss to Break-Even (${avg_cost:.2f})")
                    for t in sell_trades: self.ib.cancelOrder(t.order)
                    self.ib.sleep(1.0)

                    exit_contract = Stock(ticker, 'SMART', 'USD')
                    self.ib.qualifyContracts(exit_contract)
                    new_sl = Order(action='SELL', orderType='STP', auxPrice=round(avg_cost, 2),
                                   totalQuantity=abs(pos.position), transmit=True)
                    self.ib.placeOrder(exit_contract, new_sl)
                    self.daily_run_status[f"{date_str}_{ticker}_be_moved"] = True

        # ... [Keep the 15:55 EOD Liquidation block exactly as it is below this] ...

        if now.hour == 15 and now.minute >= 55:
            for pos in positions:
                ticker = pos.contract.symbol
                if self.daily_run_status.get(f"{date_str}_{ticker}_eod_closed"): continue

                self.log(f"⏰ EOD Liquidation: Flattening {ticker}")
                for t in self.ib.openTrades():
                    if t.contract.symbol == ticker: self.ib.cancelOrder(t.order)
                self.ib.sleep(1.0)

                exit_contract = Stock(ticker, 'SMART', 'USD')
                self.ib.qualifyContracts(exit_contract)
                action = 'SELL' if pos.position > 0 else 'BUY'
                self.ib.placeOrder(exit_contract, MarketOrder(action, abs(pos.position)))
                self.daily_run_status[f"{date_str}_{ticker}_eod_closed"] = True

    def run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.ib = IB()
        self.connect()
        self.log("Autonomous Engine Started.")

        # 1. Instantiate the Guard outside the loop
        guard = TradingGuard(self.ib, "TESSA")

        # 2. SINGLE UNIFIED LOOP
        while self.running:
            try:
                # --- KILL SWITCH CHECK ---
                guard.check_kill_switch()

                now = datetime.datetime.now(ET)
                date_str = now.strftime('%Y-%m-%d')
                market_open = now.replace(hour=9, minute=30, second=15, microsecond=0)
                scan_cutoff = now.replace(hour=10, minute=0, second=0, microsecond=0)

                # --- 1. MORNING SCAN & EXECUTION ---
                if 0 <= now.weekday() <= 4 and market_open <= now <= scan_cutoff:
                    if not self.daily_run_status.get(f"{date_str}_scanned"):

                        gappers = get_gap_down_stocks(self.cfg['scanner_gap_down_threshold'])
                        self.log(
                            f"📡 Market Scan: Found {len(gappers)} stocks gapping down worse than {self.cfg['scanner_gap_down_threshold']}%.")

                        if not gappers:
                            self.log("📊 Scan Complete: 0 gap-down candidates found. No trades today.")
                        else:
                            # Pass self.log into the assess_stock function
                            approved_tickers = [s for s in gappers if assess_stock(s['ticker'], self.cfg, self.log)]

                            if approved_tickers:
                                approved_tickers = sorted(approved_tickers, key=lambda x: x['change_pct'])[
                                    :self.cfg['max_trades']]
                                self.log(f"Executing trades for top {len(approved_tickers)} approved targets.")

                                approved_symbols = [t['ticker'] for t in approved_tickers]
                                self.log(f"✅ Approved targets for {date_str}: {', '.join(approved_symbols)}")

                                if f"{date_str}_traded_tickers" not in self.daily_run_status:
                                    self.daily_run_status[f"{date_str}_traded_tickers"] = set()

                                for target in approved_tickers:
                                    self.daily_run_status[f"{date_str}_traded_tickers"].add(target['ticker'])
                                    self.place_dual_bracket(target['ticker'], target['price'])
                            else:
                                self.log("📊 Scan Complete: 0 stocks passed the strict risk filters today.")

                        # Mark scan as complete so it doesn't loop again today
                        self.daily_run_status[f"{date_str}_scanned"] = True

                # --- 2. POSITION MANAGEMENT ---
                if 0 <= now.weekday() <= 4 and now.hour >= 11:
                    self.manage_positions()

                self.ib.sleep(15)

            except Exception as e:
                self.log(f"Loop Error: {e}")
                self.ib.sleep(10)

        # ──────────────────────────────────────────────────
        # CLEANUP: Executes only when self.running becomes False
        # ──────────────────────────────────────────────────
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            self.log("🛑 Engine Stopped. Disconnected from IBKR successfully.")

    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.run_loop, daemon=True)
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
        "https://trading.sevendimensions.com",
        "http://3.236.9.252",  # Add your EC2 IP here
        "http://localhost:3000",
        "http://localhost:5173"
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
        "thea_logs": thea_bot.logs,
        "tessa_logs": tessa_bot.logs,
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


@app.post("/start/thea", dependencies=[Depends(verify_token)])
def start_thea():
    thea_bot.start()
    return {"status": "Started"}


@app.post("/stop/thea", dependencies=[Depends(verify_token)])
def stop_thea():
    thea_bot.stop()
    return {"status": "Stopped"}


@app.post("/start/tessa", dependencies=[Depends(verify_token)])
def start_tessa(config: TessaConfig):
    tessa_bot.update_config(config.model_dump())
    tessa_bot.start()
    return {"status": "Started"}


@app.post("/stop/tessa", dependencies=[Depends(verify_token)])
def stop_tessa():
    tessa_bot.stop()
    return {"status": "Stopped"}


@app.get("/config", dependencies=[Depends(verify_token)])
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