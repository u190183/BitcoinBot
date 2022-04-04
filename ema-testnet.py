from calendar import calendar
import pandas as pd
from datetime import datetime, time
import time
import os
from dotenv import load_dotenv
import calendar
from pybit import HTTP

load_dotenv()

apiPub = os.getenv('PUB')
apiSec = os.getenv('SEC')

minSec=3*500*60
candleSec=3*60

def fetch_df(tf):
    now=pd.Timestamp(datetime.utcnow())
    unixtime=calendar.timegm(now.utctimetuple())
    since = unixtime - minSec
    session = HTTP("https://api-testnet.bybit.com",
            api_key=apiPub, api_secret=apiSec, max_retries=10, retry_delay=15,
            )
    msg=session.query_kline(
        symbol="BTCUSDT",
        interval=tf,
        limit=198,
        from_time=since
    )
    close=[]
    for key in msg['result']:
        close.append(key['close'])
    
    since=msg['result'][197]['start_at'] 
    msg=session.query_kline(
        symbol="BTCUSDT",
        interval=tf,
        limit=202,
        from_time=since
    )  
    for key in msg['result']:
        close.append(key['close'])
    
    since=msg['result'][198]['start_at'] 

    msg=session.query_kline(
        symbol="BTCUSDT",
        interval=tf,
        limit=102,
        from_time=since
    )
    for key in msg['result']:
        close.append(key['close'])
    
    df = pd.DataFrame(close, columns = ['close'])
    return df
    
def fetch_close(close,tf):
    df = fetch_df(tf)
    if(close == 'old'):
        val = df['close'].iloc[-3]  # second last closing price
    if(close == 'new'):
        val = df['close'].iloc[-2]  # last closing price
    if(close == 'current'):
        val = df['close'].iloc[-1]  # current price
    return val

def fetch_ema(emaVal,tf):
    df = fetch_df(tf)
    df['EMA'] = df['close'].ewm(span=emaVal, adjust=False).mean()
    ema = df['EMA'].iloc[-1] #get last value
    return round(ema,4)

def testBuy(): 
    session = HTTP("https://api-testnet.bybit.com",
            api_key=apiPub, api_secret=apiSec)
    
    msg=session.get_wallet_balance(coin="USDT")
    bal=round(msg['result']['USDT']['available_balance'])-1000
    price=fetch_close("current",3)
    quantity=round(bal/price,6)
         
    buy=session.place_active_order(
        symbol="BTCUSDT",
        side="Buy",
        order_type="Market",
        qty=quantity,
        time_in_force="GoodTillCancel",
        reduce_only=False,
        close_on_trigger=False,
    )
    id=buy['result']['order_id']
    query=session.query_active_order(
        symbol="BTCUSDT",
        order_id=id
    )
    entry=query['result']['last_exec_price']

    tp1=round((entry*1.005)*2)/2
    tp2=round((entry*1.01)*2)/2
    sl=round((entry*0.995)*2)/2
        
    session.set_trading_stop(
    symbol="BTCUSDT",
        side="Buy",
        take_profit=tp1,
        stop_loss=sl,
        tp_trigger_by="Last",
        sl_trigger_by="Last",
        tp_size=quantity/2,
        sl_size=quantity
    )
    session.set_trading_stop(
        symbol="BTCUSDT",
        side="Buy",
        take_profit=tp2,
        tp_trigger_by="Last",
        tp_size=quantity/2,
    )
    print("LONGED")
    in_position=True
    while(in_position):
        pos=session.my_position(
        symbol="BTCUSDT"
        )
        buy_pos=pos['result'][0]['size']
        if(buy_pos <= quantity/2 and buy_pos != 0):
            query=session.query_conditional_order(
                symbol="BTCUSDT"
            )
            stopid=query['result'][0]['stop_order_id']
            session.cancel_conditional_order(
                symbol="BTCUSDT",
                stop_order_id=stopid
            )
            session.set_trading_stop(
                symbol="BTCUSDT",
                side="Buy",
                stop_loss=entry,
                sl_trigger_by="Last",
                sl_size=quantity/2
            )
            in_position=False
        time.sleep(3)
    print("EXITED LONG")

def testShort():   
    session = HTTP("https://api-testnet.bybit.com",
            api_key=apiPub, api_secret=apiSec)
    
    msg=session.get_wallet_balance(coin="USDT")
    bal=round(msg['result']['USDT']['available_balance'])-1000
    price=fetch_close("current",3)
    quantity=round(bal/price,6)
         
    sell=session.place_active_order(
        symbol="BTCUSDT",
        side="Sell",
        order_type="Market",
        qty=quantity,
        time_in_force="GoodTillCancel",
        reduce_only=False,
        close_on_trigger=False,
    )
    id=sell['result']['order_id']
    query=session.query_active_order(
        symbol="BTCUSDT",
        order_id=id
    )
    entry=query['result']['last_exec_price']

    tp1=round((entry*0.995)*2)/2
    tp2=round((entry*0.99)*2)/2
    sl=round((entry*1.005)*2)/2
        
    session.set_trading_stop(
        symbol="BTCUSDT",
        side="Sell",
        take_profit=tp1,
        stop_loss=sl,
        tp_trigger_by="Last",
        sl_trigger_by="Last",
        tp_size=quantity/2,
        sl_size=quantity
    )
    session.set_trading_stop(
        symbol="BTCUSDT",
        side="Sell",
        take_profit=tp2,
        tp_trigger_by="Last",
        tp_size=quantity/2,
    )
    in_position=True
    print("SHORTED")
    while(in_position):
        pos=session.my_position(
        symbol="BTCUSDT"
        )
        sell_pos=pos['result'][1]['size']
        if(sell_pos <= quantity/2 and sell_pos != 0):
            query=session.query_conditional_order(
                symbol="BTCUSDT"
            )
            stopid=query['result'][0]['stop_order_id']
            session.cancel_conditional_order(
                symbol="BTCUSDT",
                stop_order_id=stopid
            )
            session.set_trading_stop(
                symbol="BTCUSDT",
                side="Sell",
                stop_loss=entry,
                sl_trigger_by="Last",
                sl_size=quantity/2
            )
            in_position=False
        time.sleep(3)
    print("EXITED SHORT")
            
def testnet_ema():
    price=fetch_close("current",3)
    ema500=fetch_ema(500,3)
    print(price,ema500)
    if(price<ema500):
        print("Looking to Long")
        validLong=True
        while(validLong):
            ema500 = fetch_ema(500, 3)
            price = fetch_close('current', 3)        
            if(price>ema500):
                time.sleep(180)
                ema500 = fetch_ema(500, 3)
                price = fetch_close('current', 3)
                if(price>ema500):
                    validLong = False
                    print("GO LONG",price,ema500)
                    testBuy()
            time.sleep(5)

    if(price>ema500):
        print("Looking to Short")
        validShort=True
        while(validShort):
            ema500 = fetch_ema(500, 3)
            price = fetch_close('current', 3)        
            if(price<ema500):
                time.sleep(180)
                ema500 = fetch_ema(500, 3)
                price = fetch_close('current', 3)
                if(price<ema500):
                    validShort = False
                    print("GO SHORT",price,ema500)
                    testShort()
            time.sleep(5)
         
run = True 
def main():
    while(run):
        testnet_ema()
        time.sleep(1)
main()