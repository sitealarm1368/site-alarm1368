//+------------------------------------------------------------------+
//|  JournalCollector.mq5  — نسخه 5.0                               |
//|  - کندل داینامیک تا 3R یا SL                                    |
//|  - watching update هر بار ران                                    |
//|  - تایم بروکر بدون تبدیل                                        |
//+------------------------------------------------------------------+
#property copyright "PriceAlert Journal"
#property version   "5.00"
#property strict

//===================================================================
input string SERVER_URL     = "https://YOUR-APP.onrender.com";
input string API_SECRET     = "";
input string FROM_DATE      = "2026.05.01";
input string TO_DATE        = "";
input int    CANDLES_BEFORE = 30;
input int    MAX_POST_CANDLES = 500;  // حداکثر کندل بعد از خروج
input bool   LOG_VERBOSE    = true;

// تایم‌فریم per symbol
input string TF_XAUUSD   = "5m";
input string TF_BTC      = "15m";
input string TF_MAJORS   = "15m";
input string TF_OTHER    = "1h";
//===================================================================

string g_sentFile = "JC_v5_sent.txt";

//+------------------------------------------------------------------+
int OnInit()
  {
   Print("=== JournalCollector v5.0 ===");
   Print("FROM:", FROM_DATE, "  TO:", TO_DATE==""?"امروز":TO_DATE);
   DrawButton();
   return INIT_SUCCEEDED;
  }
void OnDeinit(const int r) { ObjectDelete(0,"JC_SendBtn"); ObjectDelete(0,"JC_StatusLbl"); }
void OnTick() {}

//+------------------------------------------------------------------+
datetime ParseDate(string s)
  {
   if(s=="") return TimeCurrent();
   string p[]; if(StringSplit(s,'.',p)<3) return TimeCurrent();
   MqlDateTime m={}; m.year=(int)StringToInteger(p[0]);
   m.mon=(int)StringToInteger(p[1]); m.day=(int)StringToInteger(p[2]);
   return StructToTime(m);
  }

ENUM_TIMEFRAMES StrToTF(string tf)
  {
   if(tf=="1m"||tf=="M1")  return PERIOD_M1;
   if(tf=="5m"||tf=="M5")  return PERIOD_M5;
   if(tf=="15m"||tf=="M15")return PERIOD_M15;
   if(tf=="30m"||tf=="M30")return PERIOD_M30;
   if(tf=="1h"||tf=="H1")  return PERIOD_H1;
   if(tf=="4h"||tf=="H4")  return PERIOD_H4;
   if(tf=="1d"||tf=="D1")  return PERIOD_D1;
   return PERIOD_H1;
  }
string TFToStr(ENUM_TIMEFRAMES tf)
  {
   switch(tf) {
    case PERIOD_M1:  return "1m";  case PERIOD_M5:  return "5m";
    case PERIOD_M15: return "15m"; case PERIOD_M30: return "30m";
    case PERIOD_H1:  return "1h";  case PERIOD_H4:  return "4h";
    case PERIOD_D1:  return "1d";  default: return "1h";
   }
  }

string GetTFForSymbol(string sym)
  {
   string su=sym; StringToUpper(su);
   if(StringFind(su,"XAU")>=0) return TF_XAUUSD;
   if(StringFind(su,"BTC")>=0) return TF_BTC;
   string majors[]={"EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","NZDUSD","USDCAD"};
   for(int i=0;i<7;i++) if(StringFind(su,majors[i])>=0) return TF_MAJORS;
   return TF_OTHER;
  }

// تایم بروکر بدون تبدیل
string FormatBroker(datetime t)
  {
   MqlDateTime m; TimeToStruct(t,m);
   return StringFormat("%04d-%02d-%02d %02d:%02d:%02d",m.year,m.mon,m.day,m.hour,m.min,m.sec);
  }

string EscJ(string s)
  { StringReplace(s,"\\","\\\\"); StringReplace(s,"\"","\\\""); return s; }
string GetOutcome(double p) { return p>0.01?"win":p<-0.01?"loss":"breakeven"; }

//+------------------------------------------------------------------+
// کندل‌ها — داینامیک تا 3R یا SL یا MAX_POST_CANDLES
//+------------------------------------------------------------------+
string GetCandlesDynamic(string sym, ENUM_TIMEFRAMES tf,
                          datetime entry_time, datetime exit_time,
                          double entry_price, double sl_price, double tp_price,
                          bool is_buy, double mul)
  {
   SymbolSelect(sym, true);
   datetime from = entry_time - (datetime)(CANDLES_BEFORE * PeriodSeconds(tf));

   // محاسبه 3R
   double risk = sl_price > 0 ? MathAbs(entry_price - sl_price) : 0;
   double tp3  = 0;
   if(risk > 0)
      tp3 = is_buy ? entry_price + 3*risk : entry_price - 3*risk;

   // از ورود تا الان بگیر
   MqlRates r[]; ArraySetAsSeries(r, false);
   int n = CopyRates(sym, tf, from, TimeCurrent(), r);
   if(n <= 0) { Print("[EA] CopyRates 0 ",sym); return "[]"; }

   // پیدا کردن ایندکس خروج
   int exit_idx = n - 1;
   for(int i=0; i<n; i++)
      if(r[i].time >= exit_time) { exit_idx = i; break; }

   // بعد از خروج، تا 3R یا SL یا MAX کندل
   int stop_idx = MathMin(exit_idx + MAX_POST_CANDLES, n-1);
   for(int i = exit_idx; i < n; i++)
     {
      double h = r[i].high, l = r[i].low;
      if(tp3 > 0)
        {
         if(is_buy  && h >= tp3) { stop_idx = i; break; }
         if(!is_buy && l <= tp3) { stop_idx = i; break; }
        }
      if(sl_price > 0)
        {
         if(is_buy  && l <= sl_price) { stop_idx = i; break; }
         if(!is_buy && h >= sl_price) { stop_idx = i; break; }
        }
      if(i - exit_idx >= MAX_POST_CANDLES) { stop_idx = i; break; }
     }

   Print("[EA] Candles: before=",exit_idx," post=",stop_idx-exit_idx," total=",stop_idx+1," sym=",sym);

   string a = "[";
   for(int i=0; i<=stop_idx; i++)
     {
      if(i>0) a+=",";
      a+=StringFormat("{\"t\":%d,\"o\":%.5f,\"h\":%.5f,\"l\":%.5f,\"c\":%.5f,\"v\":%d}",
                      (long)r[i].time,r[i].open,r[i].high,r[i].low,r[i].close,(long)r[i].tick_volume);
     }
   return a+"]";
  }

// کندل‌های جدید از یه تایم به بعد (برای watching update)
string GetCandlesFrom(string sym, ENUM_TIMEFRAMES tf, datetime from_time)
  {
   SymbolSelect(sym, true);
   MqlRates r[]; ArraySetAsSeries(r, false);
   int n = CopyRates(sym, tf, from_time, TimeCurrent(), r);
   if(n <= 0) return "[]";
   string a="[";
   for(int i=0;i<n;i++)
     {
      if(i>0) a+=",";
      a+=StringFormat("{\"t\":%d,\"o\":%.5f,\"h\":%.5f,\"l\":%.5f,\"c\":%.5f,\"v\":%d}",
                      (long)r[i].time,r[i].open,r[i].high,r[i].low,r[i].close,(long)r[i].tick_volume);
     }
   return a+"]";
  }

//+------------------------------------------------------------------+
bool PostJSON(string endpoint, string body, string &resp)
  {
   string url=SERVER_URL+endpoint;
   string hdrs="Content-Type: application/json\r\n";
   if(StringLen(API_SECRET)>0) hdrs+="X-API-Secret: "+API_SECRET+"\r\n";
   uchar pd[],rd[]; string rh;
   StringToCharArray(body,pd,0,StringLen(body),CP_UTF8);
   int sz=ArraySize(pd); if(sz>0&&pd[sz-1]==0) ArrayResize(pd,sz-1);
   int res=-1;
   for(int i=0;i<2&&res==-1;i++){if(i>0)Sleep(2000);ResetLastError();res=WebRequest("POST",url,hdrs,30000,pd,rd,rh);}
   if(res==-1){int e=GetLastError();Print("[EA] err:",e);if(e==4014)Print("[EA] URL اضافه کن: ",SERVER_URL);return false;}
   resp=CharArrayToString(rd,0,WHOLE_ARRAY,CP_UTF8);
   if(res!=200&&res!=201){Print("[EA] HTTP ",res," ",StringSubstr(resp,0,100));return false;}
   if(LOG_VERBOSE) Print("[EA] OK → ",StringSubstr(resp,0,80));
   return true;
  }

string GetJSON(string endpoint, string &resp)
  {
   string url=SERVER_URL+endpoint;
   string hdrs="Content-Type: application/json\r\n";
   uchar rd[]; string rh;
   uchar empty[1]; empty[0]=0;
   ResetLastError();
   int res=WebRequest("GET",url,hdrs,15000,empty,rd,rh);
   if(res==-1){Print("[EA] GET err:",GetLastError());return "";}
   resp=CharArrayToString(rd,0,WHOLE_ARRAY,CP_UTF8);
   return resp;
  }

bool IsSent(long pid)
  {
   int h=FileOpen(g_sentFile,FILE_READ|FILE_TXT|FILE_SHARE_READ|FILE_ANSI);
   if(h==INVALID_HANDLE) return false;
   string c=""; while(!FileIsEnding(h)) c+=FileReadString(h); FileClose(h);
   return StringFind(c,"|"+IntegerToString(pid)+"|")>=0;
  }
void MarkSent(long pid)
  {
   int h=FileOpen(g_sentFile,FILE_READ|FILE_WRITE|FILE_TXT|FILE_SHARE_READ|FILE_ANSI);
   if(h==INVALID_HANDLE) h=FileOpen(g_sentFile,FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(h==INVALID_HANDLE) return;
   FileSeek(h,0,SEEK_END); FileWriteString(h,"|"+IntegerToString(pid)+"|"); FileClose(h);
  }

//+------------------------------------------------------------------+
bool SendPosition(long pos_id, int total_deals)
  {
   string sym="",direction="",comment="";
   double entry_price=0,exit_price=0,lots=0,total_profit=0,sl_price=0,tp_price=0;
   datetime entry_time=0,exit_time=0;
   ulong entry_ticket=0;
   bool found_exit=false;

   for(int i=0;i<total_deals;i++)
     {
      ulong d=HistoryDealGetTicket(i);
      if((long)HistoryDealGetInteger(d,DEAL_POSITION_ID)!=pos_id) continue;
      ENUM_DEAL_TYPE  dt=(ENUM_DEAL_TYPE) HistoryDealGetInteger(d,DEAL_TYPE);
      ENUM_DEAL_ENTRY de=(ENUM_DEAL_ENTRY)HistoryDealGetInteger(d,DEAL_ENTRY);
      if(dt!=DEAL_TYPE_BUY&&dt!=DEAL_TYPE_SELL) continue;
      if(de==DEAL_ENTRY_IN)
        {
         entry_ticket=d; sym=HistoryDealGetString(d,DEAL_SYMBOL);
         direction=(dt==DEAL_TYPE_BUY)?"BUY":"SELL";
         entry_price=HistoryDealGetDouble(d,DEAL_PRICE);
         entry_time=(datetime)HistoryDealGetInteger(d,DEAL_TIME);
         lots=HistoryDealGetDouble(d,DEAL_VOLUME);
         comment=HistoryDealGetString(d,DEAL_COMMENT);
         double ds=HistoryDealGetDouble(d,DEAL_SL),dt2=HistoryDealGetDouble(d,DEAL_TP);
         if(ds>0) sl_price=ds; if(dt2>0) tp_price=dt2;
        }
      else if(de==DEAL_ENTRY_OUT||de==DEAL_ENTRY_INOUT)
        {
         exit_price=HistoryDealGetDouble(d,DEAL_PRICE);
         exit_time=(datetime)HistoryDealGetInteger(d,DEAL_TIME);
         total_profit+=HistoryDealGetDouble(d,DEAL_PROFIT)+HistoryDealGetDouble(d,DEAL_SWAP)+HistoryDealGetDouble(d,DEAL_COMMISSION);
         double ds=HistoryDealGetDouble(d,DEAL_SL),dt2=HistoryDealGetDouble(d,DEAL_TP);
         if(ds>0&&sl_price==0) sl_price=ds; if(dt2>0&&tp_price==0) tp_price=dt2;
         found_exit=true;
        }
     }

   if(!found_exit){if(LOG_VERBOSE)Print("[EA] pos=",pos_id," باز — skip");return false;}
   if(sym==""||entry_price==0) return false;

   string tf_str=GetTFForSymbol(sym);
   ENUM_TIMEFRAMES tf=StrToTF(tf_str);
   string outcome=GetOutcome(total_profit);
   string exit_type=(outcome=="loss")?"sl":"tp";
   bool is_buy=(direction=="BUY");

   double mul=10000.0; string su=sym; StringToUpper(su);
   if(StringFind(su,"JPY")>=0) mul=100.0;
   if(StringFind(su,"XAU")>=0||StringFind(su,"XAG")>=0) mul=10.0;
   if(StringFind(su,"BTC")>=0||StringFind(su,"ETH")>=0) mul=1.0;

   double sl_pips=sl_price>0?MathAbs(entry_price-sl_price)*mul:0;
   double tp_pips=tp_price>0?MathAbs(tp_price-entry_price)*mul:0;

   // کندل داینامیک تا 3R یا SL
   string candles=GetCandlesDynamic(sym,tf,entry_time,exit_time,entry_price,sl_price,tp_price,is_buy,mul);

   Print("[EA] ",sym," ",direction," ",outcome," SL=",sl_price," TP=",tp_price," tf=",tf_str);

   string json="{";
   json+="\"sym\":\""          +EscJ(sym)+"\",";
   json+="\"tf\":\""           +tf_str+"\",";
   json+="\"direction\":\""    +direction+"\",";
   json+="\"entry\":"          +DoubleToString(entry_price,5)+",";
   json+="\"exit\":"           +DoubleToString(exit_price,5)+",";
   json+="\"sl_price\":"       +(sl_price>0?DoubleToString(sl_price,5):"null")+",";
   json+="\"tp_price\":"       +(tp_price>0?DoubleToString(tp_price,5):"null")+",";
   json+="\"sl_pips\":"        +(sl_pips>0?DoubleToString(sl_pips,1):"null")+",";
   json+="\"tp_pips\":"        +(tp_pips>0?DoubleToString(tp_pips,1):"null")+",";
   json+="\"size\":"           +DoubleToString(lots,2)+",";
   json+="\"entryTime\":\""    +FormatBroker(entry_time)+"\",";
   json+="\"exitTime\":\""     +FormatBroker(exit_time)+"\",";
   json+="\"outcome\":\""      +outcome+"\",";
   json+="\"exit_type\":\""    +exit_type+"\",";
   json+="\"pnl\":"            +DoubleToString(total_profit,2)+",";
   json+="\"mt4_ticket\":"     +IntegerToString(entry_ticket)+",";
   json+="\"mt4_position_id\":"+IntegerToString(pos_id)+",";
   json+="\"mt4_profit\":"     +DoubleToString(total_profit,2)+",";
   json+="\"note\":\""         +EscJ(comment)+"\",";
   json+="\"source\":\"mt5_ea\",";
   json+="\"candle_snapshot\":"+candles;
   json+="}";

   string resp="";
   bool ok=PostJSON("/api/journal/mt4",json,resp);
   if(ok) Print("[EA] ✅ ",sym," ",direction," ",outcome);
   else   Print("[EA] ❌ ",sym," pos=",pos_id);
   return ok;
  }

//+------------------------------------------------------------------+
// آپدیت تریدهای watching
//+------------------------------------------------------------------+
void UpdateWatchingTrades()
  {
   Print("[EA] === بررسی تریدهای در جریان ===");
   string resp="";
   GetJSON("/api/journal/watching", resp);
   if(StringLen(resp)==0) { Print("[EA] watching: سرور جواب نداد"); return; }

   // parse ساده — دنبال "id":"..." و "exitTime":"..." بگرد
   // چون MQL5 JSON parser ندارد، با StringFind کار میکنیم
   int watching_count=0;
   int search_pos=0;

   while(true)
     {
      // پیدا کردن "id":"
      int id_pos=StringFind(resp,"\"id\":\"",search_pos);
      if(id_pos<0) break;
      id_pos+=6;
      int id_end=StringFind(resp,"\"",id_pos);
      if(id_end<0) break;
      string trade_id=StringSubstr(resp,id_pos,id_end-id_pos);

      // پیدا کردن "exitTime":"
      int et_pos=StringFind(resp,"\"exitTime\":\"",id_end);
      if(et_pos<0) break;
      et_pos+=12;
      int et_end=StringFind(resp,"\"",et_pos);
      if(et_end<0) break;
      string exit_time_str=StringSubstr(resp,et_pos,et_end-et_pos);

      // پیدا کردن "sym":"
      int sym_pos=StringFind(resp,"\"sym\":\"",id_end);
      if(sym_pos<0) break;
      sym_pos+=7;
      int sym_end=StringFind(resp,"\"",sym_pos);
      string sym=StringSubstr(resp,sym_pos,sym_end-sym_pos);

      // پیدا کردن "tf":"
      int tf_pos=StringFind(resp,"\"tf\":\"",id_end);
      if(tf_pos<0) { search_pos=et_end; continue; }
      tf_pos+=6;
      int tf_end=StringFind(resp,"\"",tf_pos);
      string tf_str=StringSubstr(resp,tf_pos,tf_end-tf_pos);

      search_pos=et_end+1;
      watching_count++;

      Print("[EA] watching: ",trade_id," ",sym," tf=",tf_str," exit=",exit_time_str);

      // تبدیل exit_time به datetime بروکر
      // فرمت: "2026-05-21 10:00:00"
      StringReplace(exit_time_str,"-",".");
      StringReplace(exit_time_str," ",".");
      StringReplace(exit_time_str,":",".");
      string parts[]; StringSplit(exit_time_str,'.',parts);
      datetime from_dt=TimeCurrent()-86400; // fallback
      if(ArraySize(parts)>=6)
        {
         MqlDateTime mdt={};
         mdt.year=(int)StringToInteger(parts[0]); mdt.mon=(int)StringToInteger(parts[1]);
         mdt.day=(int)StringToInteger(parts[2]);  mdt.hour=(int)StringToInteger(parts[3]);
         mdt.min=(int)StringToInteger(parts[4]);  mdt.sec=(int)StringToInteger(parts[5]);
         from_dt=StructToTime(mdt);
        }

      // کندل‌های جدید از exitTime به بعد
      ENUM_TIMEFRAMES tf=StrToTF(tf_str);
      string new_candles=GetCandlesFrom(sym,tf,from_dt);
      if(new_candles=="[]") { Print("[EA] watching: ",sym," کندل جدید نداشت"); continue; }

      // ارسال به سرور
      string upd_json="{\"id\":\""+trade_id+"\",\"candle_snapshot\":"+new_candles+"}";
      string upd_resp="";
      bool ok=PostJSON("/api/journal/mt4/update-watching",upd_json,upd_resp);
      if(ok) Print("[EA] watching ✅ ",sym," آپدیت شد");
      Sleep(500);
     }

   Print("[EA] watching: ",watching_count," ترید بررسی شد");
  }

//+------------------------------------------------------------------+
void CollectAndSend()
  {
   datetime from=ParseDate(FROM_DATE);
   datetime to=(TO_DATE=="")?TimeCurrent():ParseDate(TO_DATE)+86400;
   Print("=== CollectAndSend از ",FROM_DATE," تا ",TO_DATE==""?"امروز":TO_DATE," ===");
   UpdateStatus("در حال ارسال...",clrYellow);

   if(!HistorySelect(from,to)){Print("[EA] HistorySelect ناموفق");UpdateStatus("خطا",clrRed);return;}

   int total_deals=HistoryDealsTotal();
   Print("[EA] deals: ",total_deals);

   long positions[]; int pos_count=0;
   for(int i=0;i<total_deals;i++)
     {
      ulong d=HistoryDealGetTicket(i);
      long pid=HistoryDealGetInteger(d,DEAL_POSITION_ID);
      if(pid==0) continue;
      bool dup=false;
      for(int x=0;x<pos_count;x++) if(positions[x]==pid){dup=true;break;}
      if(dup) continue;
      ArrayResize(positions,pos_count+1); positions[pos_count++]=pid;
     }

   Print("[EA] positions: ",pos_count);
   int sent=0,skip=0;
   for(int i=0;i<pos_count;i++)
     {
      long pid=positions[i];
      if(IsSent(pid)){skip++;continue;}
      bool ok=SendPosition(pid,total_deals);
      if(ok){MarkSent(pid);sent++;Sleep(600);}
      else skip++;
     }

   Print("=== تریدهای جدید: ارسال=",sent," skip=",skip," ===");

   // آپدیت watching ها
   Sleep(1000);
   UpdateWatchingTrades();

   string msg=StringFormat("✅ جدید:%d | watching آپدیت شد",sent);
   UpdateStatus(msg,clrLime);
  }

//+------------------------------------------------------------------+
void DrawButton()
  {
   string b="JC_SendBtn";
   ObjectDelete(0,b); ObjectCreate(0,b,OBJ_BUTTON,0,0,0);
   ObjectSetInteger(0,b,OBJPROP_XDISTANCE,15); ObjectSetInteger(0,b,OBJPROP_YDISTANCE,30);
   ObjectSetInteger(0,b,OBJPROP_XSIZE,240);    ObjectSetInteger(0,b,OBJPROP_YSIZE,36);
   ObjectSetString(0,b,OBJPROP_TEXT,"📤  Journal v5  ["+FROM_DATE+"]");
   ObjectSetInteger(0,b,OBJPROP_COLOR,clrWhite);
   ObjectSetInteger(0,b,OBJPROP_BGCOLOR,C'40,40,55');
   ObjectSetInteger(0,b,OBJPROP_FONTSIZE,9);
   ObjectSetInteger(0,b,OBJPROP_CORNER,CORNER_LEFT_UPPER);
   ObjectSetInteger(0,b,OBJPROP_SELECTABLE,false);
   string l="JC_StatusLbl";
   ObjectDelete(0,l); ObjectCreate(0,l,OBJ_LABEL,0,0,0);
   ObjectSetInteger(0,l,OBJPROP_XDISTANCE,15); ObjectSetInteger(0,l,OBJPROP_YDISTANCE,72);
   ObjectSetString(0,l,OBJPROP_TEXT,"آماده — دکمه رو بزن");
   ObjectSetInteger(0,l,OBJPROP_COLOR,clrSilver); ObjectSetInteger(0,l,OBJPROP_FONTSIZE,9);
   ObjectSetInteger(0,l,OBJPROP_CORNER,CORNER_LEFT_UPPER);
   ObjectSetInteger(0,l,OBJPROP_SELECTABLE,false);
   ChartRedraw(0);
  }
void UpdateStatus(string msg,color clr)
  { ObjectSetString(0,"JC_StatusLbl",OBJPROP_TEXT,msg); ObjectSetInteger(0,"JC_StatusLbl",OBJPROP_COLOR,clr); ChartRedraw(0); }
void OnChartEvent(const int id,const long &lp,const double &dp,const string &sp)
  { if(id==CHARTEVENT_OBJECT_CLICK&&sp=="JC_SendBtn"){ObjectSetInteger(0,"JC_SendBtn",OBJPROP_STATE,false);CollectAndSend();} }
//+------------------------------------------------------------------+
