from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

import pandas as pd
import threading
import time


# Test by codelover
import pyodbc
import configparser
import sys
import pytz
from datetime import datetime, timedelta
import math

# loading configuration file
config = configparser.ConfigParser()
config.read('config.ini')

g_connection = None

g_symbols = []
g_contracts = []
g_account_id = 0


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bardata = {}  # Initialize dictionary to store bar data

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print('The next valid order id is: ', self.nextorderId)

    def tick_df(self, reqId, contract):
        ''' custom function to init DataFrame and request Tick Data '''
        self.bardata[reqId] = pd.DataFrame(columns=['time', 'price'])
        self.bardata[reqId].set_index('time', inplace=True)
        self.reqTickByTickData(reqId, contract, "Last", 0, True)
        return self.bardata[reqId]

    def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAtrribLast, exchange, specialConditions):
        if tickType == 1:
            print("currentPrice+++++++++", price)

            self.bardata[reqId].loc[pd.to_datetime(time, unit='s')] = price

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled',
              filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange,
              ':', order.action, order.orderType, order.totalQuantity, orderState.status)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency,
              execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)

    def Stock_contract(self, symbol, secType='STK', exchange='ISLAND', currency='USD'):
        ''' custom function to create contract '''
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

# connect to database


def connect_db(dbserver, dbname, dbuser, dbpass):
    # connection = pypyodbc.connect('Driver={SQL Server};'
    #                                 'Server=' + dbserver + ';'
    #                                 'Database=' + dbname + ';'
    #                                 'uid=' + dbuser + ';pwd=' + dbpass)
    conn = pyodbc.connect(
        'Driver={SQL Server};' 'Server=DESKTOP-MEL3TAC;' 'Database=StockAlgo;')
    return conn

# disconnect from database


def disconnect_db():
    print("pre db disconnected")
    g_connection.close()
    print("db disconnected")


# get symbol data form tblSymbolWatchList
# we get only records that have Order_Type = 'L' and Manual_Auto_flag is not null at this point
# Automatical limit order placement is high priority so we use order by Manual_Auto_flag
# Broker: E - ETrade, IB - IB, ALL - both of ETrade and IB

def get_symbols():

    SQLCommand = ("SELECT Symbol, NoOfTrades, OrderQuantity, TradingFlag, Manual_Auto_flag, AvgSpread_Change, "
                  "Reg_Flag, sellpriceChange, IIF([CustomAvgSpread] is NULL, 0, [CustomAvgSpread]) AS AvgSpread, CustomBuyPrice, CustomSellPrice, "
                  "symbol_id, symbol_ord, Order_Type, SizeFlag, Size, PriceDown FROM tblSymbolWatchList (nolock) "
                  "WHERE Order_Type = 'L' AND Manual_Auto_flag IS NOT NULL AND (Broker = 'IB' OR Broker = 'ALL') AND TradingFlag = 'Y' "
                  "ORDER BY Manual_Auto_flag, symbol_id")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result


def run_loop():
    app.run()


def submit_order(contract, direction, qty, ordertype, transmit=True):
    # app.reqOpenOrders()

    # Create order object
    order = Order()
    order.action = direction
    order.totalQuantity = qty
    order.orderType = ordertype
    order.transmit = transmit
    # submit order
    app.placeOrder(app.nextorderId, contract, order)
    app.nextorderId += 1

# get open orders from [tbl_Python_stock_OrderInfo] table


def get_recent_orders():

    SQLCommand = ("SELECT [OrderId],[Symbol],[OrderInstruction],[OrderStatus],[Manual_Auto_flag],[OrderPrice],[symbol_ord] "
                  "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Platform = 'E' "
                  "AND AccountId = " + str(g_account_id) + " AND cast(UpdateDate as date) = cast(GETDATE() as date) ORDER BY OrderId DESC")
    # SQLCommand = ("SELECT [OrderId],[Symbol],[OrderInstruction],[OrderStatus],[Manual_Auto_flag],[OrderPrice],[symbol_ord] "
    #         "FROM [tbl_Python_stock_OrderInfo] WHERE Platform = 'E' "
    #         "AND AccountId = " + str(g_account_id) + " ORDER BY OrderId DESC")

    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)

    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result

# get the last order data of the [tbl_Python_stock_OrderInfo] table for the specified symbol


def get_order_last_status(symbol, account_id):

    SQLCommand = ("SELECT Top(1) [OrderId],[Platform],[Symbol],[OrderInstruction],[OrderPrice],[OrderDate],[OrderStatus],[OrderPlaceJson],"
                  "[LatestFlag],[UpdateDate],[NextFlag],[MailSent],[Manual_Auto_flag],[Reg_Flag],[symbol_ord],[OrderQty] "
                  "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Symbol = '" +
                  symbol + "' AND Platform = 'E' "
                  "AND AccountId = " + str(account_id) + " AND CAST(UpdateDate AS date) = CAST(GETDATE() AS date) ORDER BY OrderId DESC ")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    results = cursor.fetchone()
    return results


# insert order data to [tbl_Python_stock_OrderInfo] table
def insert_order_data(order_data):

    SQLCommand = ("INSERT INTO tbl_Python_stock_OrderInfo ([OrderId], [Platform], [Symbol], [OrderInstruction], [OrderPrice], [OrderDate], [OrderStatus], "
                  "[OrderPlaceJson], [LatestFlag], [UpdateDate], [MailSent], [Manual_Auto_flag], [Reg_Flag], [symbol_ord], [OrderQty], [MarketSession], [AccountId]) "
                  "VALUES (" + str(order_data["client_order_id"]) + ",'E','" + order_data["symbol"] + "','" + order_data["order_action"] + "'," + str(order_data["limit_price"]) + ",'" +
                  order_data["order_date"] + "','" + order_data["status"] + "','" + order_data["json"] + "','','" + order_data["order_date"] + "','Y','" + order_data["manual_auto_flag"] + "','" +
                  order_data["price_type"] + "','" + order_data["symbol_ord"] + "'," + str(order_data["quantity"]) + ", '" + order_data["market_session"] + "', " + order_data["account_id"] + ")")

    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    g_connection.commit()

# get open orders from [tbl_Python_stock_OrderInfo] table


def get_order_infor_buy(symbol):
    SQLCommand = ("SELECT [OrderId],[Symbol],[OrderInstruction],[OrderStatus],[Manual_Auto_flag],[OrderPrice],[symbol_ord],[PriceDownStep]"
                  "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Platform = 'IB' "
                  "AND OrderInstruction = 'Buy' AND Symbol = '" + str(symbol) + "' AND PriceDownStep != 0 ORDER BY OrderId DESC")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result


def get_order_infor_sell(symbol):
    SQLCommand = ("SELECT [OrderId],[Symbol],[OrderInstruction],[OrderStatus],[Manual_Auto_flag],[OrderPrice],[symbol_ord],[PriceDownStep],[OrderQty]"
                  "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Platform = 'IB' "
                  "AND OrderInstruction = 'Buy' AND OrderStatus = 'Filled' AND Symbol = '" + str(symbol) + "' AND PriceDownStep != 0 AND  ORDER BY OrderId DESC")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result


def check_for_trade(df, contract, db_data):
	print(db_data)
	# switch buy order method
	# if (df.price.iloc[-1] <= db_data[9]):
	if (71 <= db_data[9]):
		order_infor_by_stock = get_order_infor(db_data[0], 'Buy')
		if len(order_infor_by_stock) == 0:
			submit_order(contract, 'BUY', int(db_data[15]), 'LMT')
		if len(order_infor_by_stock) > 0:
			last_price_down_step = (order_infor_by_stock)[0][7]
			total_step = math.ceil(int(db_data[2])/int(db_data[15]))
			print((db_data[9] - (int(last_price_down_step) + 1)*int(db_data[16])))
			# def submit_order(contract, direction, qty, ordertype, transmit=True):

			# if (total_step > last_price_down_step) and (df.price.iloc[-1] >= (db_data[9] - (int(last_price_down_step) + 1)*int(db_data[16])))
			if (total_step == (int(last_price_down_step) + 1):
				order_quantity = int(db_data[2]) - int(last_price_down_step) * int(db_data[15])
				submit_order(contract, 'BUY', order_quantity, 'LMT')
			elif (total_step > last_price_down_step) and (68 >= (db_data[9] - (int(last_price_down_step) + 1)*int(db_data[16]))):
				order_quantity = int(db_data[15])
				submit_order(contract, 'BUY', order_quantity, 'LMT')
	# switch sell order method
	if (76 >= db_data[10]):
		order_infor_by_stocks = get_order_infor_sell(db_data[0])
		if len(order_infor_by_stocks) > 0:
			for order_infor_by_stock in order_infor_by_stocks:
				order_quantity = order_infor_by_stock[8]			
				submit_order(contract, 'SELL', order_quantity, 'LMT')

	
    # tz_CT = pytz.timezone('America/Chicago')
    # datetime_CT = datetime.now(tz_CT)
    # cur_date = datetime_CT.strftime("%m%d%Y")
    # n_days_ago = datetime_CT - timedelta(days=1)
    # from_date = n_days_ago.strftime("%m%d%Y")

    # updated_orders = update_orders_status(from_date, to_date)
    # limit_order_place(market_session, updated_orders)

    # start_time = df.index[-1] - pd.Timedelta(minutes=5)
    # print(df.price.iloc[-1])
    # print("max_value:", max_value)

    # if df.price.iloc[-1] < max_value * 0.95:
    # 	submit_order(contract, 'SELL')
    # 	return True

    # elif df.price.iloc[-1]:
    # 	submit_order(contract, 'BUY')
    # 	return True

# thread start


def threading_start():
    g_symbols = get_symbols()
    for index, g_contract in enumerate(g_contracts):
        # Request tick data for google using custom function
        # df = app.tick_df(401, g_contract)
        df = 'test df'
        # Verify data stream
        # time.sleep(10)
        # for i in range(100):
        # 	if len(df) > 0:
        # 		break
        # 	time.sleep(0.3)

        # if i == 99:
        # 	app.disconnect()
        # 	raise Exception ('Error with Tick data stream')
        # Check if there is enough data
        # data_length = df.index[-1] - df.index[0]
        # if data_length.seconds < 300:
        # 	time.sleep(300 - data_length.seconds)
        check_for_trade(df, g_contract, g_symbols[index])
        time.sleep(0.1)

    return True


if __name__ == "__main__":
    app = IBapi()
    app.nextorderId = None
    if config['DEFAULT']['LIVE_FLAG'] == 1:
        app.connect('127.0.0.1', 4002, 123)
    else:
        app.connect('127.0.0.1', 7497, 123)

    # Start the socket in a thread that is for getting last price and order status
    api_thread = threading.Thread(target=run_loop)
    api_thread.start()

    # Check if the API is connected via orderid
    while True:
        if isinstance(app.nextorderId, int):
            print('connected')
            break
        else:
            print('waiting for connection')
            time.sleep(1)

    g_connection = connect_db(config["DEFAULT"]["DB_SERVER"], config["DEFAULT"]
                              ["DB_NAME"], config["DEFAULT"]["DB_USER"], config["DEFAULT"]["DB_PASS"])
    market_session = ''
    while True:
        tz_CT = pytz.timezone('America/Chicago')
        datetime_CT = datetime.now(tz_CT)
        cur_time = datetime_CT.strftime("%H:%M:%S")
        if cur_time >= config["DEFAULT"]["END_TIME"] or cur_time < config["DEFAULT"]["PRE_START"]:
            if (market_session != 'CLOSED'):
                print("Market was closed !!!")
            market_session = 'CLOSED'
            continue
        market_session = 'REGULAR'
        if cur_time < config["DEFAULT"]["START_TIME"]:
            market_session = 'PRE'

        if cur_time >= config["DEFAULT"]["EXTENDED_START"]:
            market_session = 'EXTENDED'
        # get all symbols
        g_symbols = get_symbols()
        if len(g_symbols) == 0:
            print("Please add symbols into the [tblSymbolWatchList] table")
            disconnect_db()
            sys.exit()

        # make list of stock contract
        for symbol in g_symbols:
            symbol_stock = app.Stock_contract(symbol[0])
            g_contracts.append(symbol_stock)

        threading_start()
    # app.disconnect()
