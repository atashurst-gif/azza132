//+------------------------------------------------------------------+
//|                                            MintelGuardian.mq5    |
//|                                                                  |
//|  Broker-side last line of defence.                               |
//|                                                                  |
//|  This Expert Advisor does NOT make trading decisions.  The        |
//|  intelligence lives in the Python engine.  This exists for one    |
//|  reason: if the Python process dies, the VPS reboots, or the      |
//|  network drops, something inside the terminal must still ensure   |
//|  every open position has a protective stop and that a runaway     |
//|  loss cannot continue unattended.                                |
//|                                                                  |
//|  It is deliberately tiny and dependency-free so it can be         |
//|  compatible with MetaTrader Virtual Hosting, where external       |
//|  processes are not available.                                     |
//|                                                                  |
//|  What it does, every tick and every timer:                        |
//|    1. Any position with no stop loss gets one, sized from ATR.    |
//|    2. A stop is only ever tightened, never widened.               |
//|    3. If the account's daily loss limit is breached it closes      |
//|       everything and stops (a genuine circuit breaker).           |
//|    4. It writes a heartbeat file the Python watchdog can read.    |
//+------------------------------------------------------------------+
#property copyright   "Market Intelligence Trader"
#property version     "1.00"
#property description "Broker-side stop-loss guardian. Makes no decisions."
#property strict

#include <Trade\Trade.mqh>
#include <Trade\SymbolInfo.mqh>

//--- inputs
input int    InpMagic              = 990311;  // magic number of the engine
input double InpEmergencyAtrMult   = 2.5;     // emergency stop, in ATR
input int    InpAtrPeriod          = 14;      // ATR period
input ENUM_TIMEFRAMES InpAtrTF     = PERIOD_M15;
input double InpMaxDailyLossPct    = 5.0;     // hard daily loss cut-out
input bool   InpManageOnlyOwnMagic = true;    // ignore manual trades
input int    InpTimerSeconds       = 5;
input bool   InpWriteHeartbeat     = true;

CTrade   trade;
double   g_dayStartEquity = 0.0;
datetime g_dayStamp       = 0;
bool     g_halted         = false;

//+------------------------------------------------------------------+
int OnInit()
  {
   trade.SetExpertMagicNumber(InpMagic);
   trade.SetDeviationInPoints(30);
   trade.SetTypeFillingBySymbol(_Symbol);
   EventSetTimer(InpTimerSeconds);
   RollDay();
   PrintFormat("MintelGuardian active. Magic=%d, emergency stop=%.1f ATR, "
               "daily cut-out=%.2f%%",
               InpMagic, InpEmergencyAtrMult, InpMaxDailyLossPct);
   return(INIT_SUCCEEDED);
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
  }

void OnTick()  { Guard(); }
void OnTimer() { Guard(); }

//+------------------------------------------------------------------+
//| Reset the daily reference at the start of each server day.       |
//+------------------------------------------------------------------+
void RollDay()
  {
   MqlDateTime t;
   TimeToStruct(TimeCurrent(), t);
   t.hour = 0; t.min = 0; t.sec = 0;
   datetime today = StructToTime(t);
   if(today != g_dayStamp)
     {
      g_dayStamp       = today;
      g_dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
      g_halted         = false;
      PrintFormat("New trading day. Reference equity %.2f", g_dayStartEquity);
     }
  }

//+------------------------------------------------------------------+
void Guard()
  {
   RollDay();
   if(InpWriteHeartbeat)
      WriteHeartbeat();

   //--- hard daily loss cut-out -------------------------------------
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   if(g_dayStartEquity > 0.0 && InpMaxDailyLossPct > 0.0)
     {
      double lossPct = (g_dayStartEquity - equity) / g_dayStartEquity * 100.0;
      if(lossPct >= InpMaxDailyLossPct && !g_halted)
        {
         PrintFormat("DAILY LOSS LIMIT HIT: down %.2f%% (limit %.2f%%). "
                     "Closing everything.", lossPct, InpMaxDailyLossPct);
         CloseEverything("daily loss limit");
         g_halted = true;
         return;
        }
     }

   //--- make sure every position has a protective stop --------------
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0)
         continue;
      if(!PositionSelectByTicket(ticket))
         continue;
      if(InpManageOnlyOwnMagic &&
         (long)PositionGetInteger(POSITION_MAGIC) != InpMagic)
         continue;

      string symbol = PositionGetString(POSITION_SYMBOL);
      double sl     = PositionGetDouble(POSITION_SL);
      if(sl > 0.0)
         continue;                       // the engine's stop is in place

      EnsureStop(ticket, symbol);
     }
  }

//+------------------------------------------------------------------+
//| Place an emergency ATR stop on an unprotected position.          |
//+------------------------------------------------------------------+
void EnsureStop(const ulong ticket, const string symbol)
  {
   double atrBuf[];
   int handle = iATR(symbol, InpAtrTF, InpAtrPeriod);
   double atr = 0.0;
   if(handle != INVALID_HANDLE && CopyBuffer(handle, 0, 0, 1, atrBuf) == 1)
      atr = atrBuf[0];

   double point  = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int    digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   long   stopsLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double spread = SymbolInfoInteger(symbol, SYMBOL_SPREAD) * point;

   double distance = atr * InpEmergencyAtrMult;
   double minDist  = MathMax(stopsLevel * point, spread * 3.0);
   if(distance < minDist)
      distance = minDist;
   if(distance <= 0.0)
      distance = 200 * point;            // last resort, never zero

   long   type = PositionGetInteger(POSITION_TYPE);
   double bid  = SymbolInfoDouble(symbol, SYMBOL_BID);
   double ask  = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double tp   = PositionGetDouble(POSITION_TP);
   double sl;

   if(type == POSITION_TYPE_BUY)
      sl = NormalizeDouble(bid - distance, digits);
   else
      sl = NormalizeDouble(ask + distance, digits);

   if(trade.PositionModify(ticket, sl, tp))
      PrintFormat("Emergency stop placed on %s ticket %I64u at %s "
                  "(%.1f ATR)", symbol, ticket,
                  DoubleToString(sl, digits), InpEmergencyAtrMult);
   else
      PrintFormat("COULD NOT place an emergency stop on %s ticket %I64u: %d",
                  symbol, ticket, trade.ResultRetcode());
  }

//+------------------------------------------------------------------+
void CloseEverything(const string reason)
  {
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(InpManageOnlyOwnMagic &&
         (long)PositionGetInteger(POSITION_MAGIC) != InpMagic)
         continue;
      if(!trade.PositionClose(ticket))
         PrintFormat("Could not close ticket %I64u (%s): %d",
                     ticket, reason, trade.ResultRetcode());
     }
  }

//+------------------------------------------------------------------+
//| A heartbeat the Python watchdog can see, written into            |
//| MQL5\Files so it is readable from the terminal's data folder.    |
//+------------------------------------------------------------------+
void WriteHeartbeat()
  {
   static datetime last = 0;
   if(TimeCurrent() - last < InpTimerSeconds)
      return;
   last = TimeCurrent();

   int fh = FileOpen("mintel_guardian.heartbeat.json",
                     FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(fh == INVALID_HANDLE)
      return;
   string payload = StringFormat(
      "{\"name\":\"mql5_guardian\",\"server_time\":\"%s\",\"equity\":%.2f,"
      "\"balance\":%.2f,\"positions\":%d,\"halted\":%s,"
      "\"trade_allowed\":%s}",
      TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS),
      AccountInfoDouble(ACCOUNT_EQUITY),
      AccountInfoDouble(ACCOUNT_BALANCE),
      PositionsTotal(),
      g_halted ? "true" : "false",
      (bool)TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) ? "true" : "false");
   FileWrite(fh, payload);
   FileClose(fh);
  }
//+------------------------------------------------------------------+
