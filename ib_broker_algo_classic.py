from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

import pandas as pd
import threading
import time

import pypyodbc
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
g_symbols_container = []
g_contracts = []

lastPrice_dict = {}
orderStatus_dict = {}
g_stop_flag = False
market_session = ''
g_orderStatus = []

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bardata = {} #Initialize dictionary to store bar data
    
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
            self.bardata[reqId].loc[pd.to_datetime(time, unit='s')] = price
            global lastPrice_dict
            lastPrice_dict[reqId] = price
            print(lastPrice_dict)
            self.order_operation(reqId, price)

    def stock_contract(self, symbol, secType='STK', exchange='SMART', currency='USD'):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def request_tick_df(self, contract_obj):
        try:
            df = self.tick_df(contract_obj[1], contract_obj[0])
            # Verify data stream
            time.sleep(10)
            i = 0
            for i in range(100):
                if len(df) > 0:
                    break
                time.sleep(0.3)

            if i == 99:
                app.disconnect()
                # raise Exception ('Error with Tick data stream')
        except:
            print("Error with Tick data stream")


    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange,
                ':', order.action, order.orderType, order.totalQuantity, orderState.status)
        self.insert_orderinfo(orderId, contract.symbol, order.action, order.lmtPrice, orderState.status, order.totalQuantity)

        if orderState.status == 'Filled' and order.action == 'SELL':
            self.update_buy_status(contract.symbol, orderId)
    
    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):

        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)

        if status == 'Filled':
            self.update_orderinfo(orderId, status, filled, remaining, lastFillPrice)

    def remove_contract(self, req_id):
        self.cancelTickByTickData(req_id)
    
    # insert order data to [tbl_Python_stock_OrderInfo] table
    def insert_orderinfo(self, orderId, symbol, action, lmtPrice, status, quantity):
        global g_orderStatus
        isOrderID = False

        for index, status in enumerate(g_orderStatus):
            if status[0] == orderId:
                isOrderID = True
                g_orderStatus[index][3] = status
                g_orderStatus[index][8] = quantity
                break

        if isOrderID:
            SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET UpdateDate = GETDATE() , OrderStatus = '" + status + "', "
            "FilledQuantity = " + quantity + "' WHERE OrderID = " + orderId)

        else:
            for g_symbol in g_symbols:
                size_flag = g_symbol[14]
                tz_CT = pytz.timezone('America/Chicago')
                datetime_CT = datetime.now(tz_CT)
                cur_time = datetime_CT.strftime("%Y-%m-%d %H:%M:%S")

                if g_symbol[0] == symbol:
                    if size_flag == 'Y':
                        priceDownStep = 0
                        order_info_from_db = self.check_previous_status(symbol, 'Y', 'BUY')

                        if len(order_info_from_db) == 0:
                            priceDownStep = 1
                            
                        else:
                            priceDownStep = int(order_info_from_db[0][7]) + 1

                        SQLCommand = ("INSERT INTO tbl_Python_stock_OrderInfo ([OrderId], [Platform], [Symbol], [OrderInstruction], [OrderPrice], [OrderDate], [OrderStatus], "
                            "[OrderPlaceJson], [LatestFlag], [UpdateDate], [MailSent], [Manual_Auto_flag], [Reg_Flag], [symbol_ord], [OrderQty], [MarketSession], [PriceDownStep]) "
                            "VALUES (" + str(orderId) + ",'IB','" + symbol + "','" + action + "'," + str(lmtPrice) + ",'" + 
                            cur_time + "','" + str(status) + "','" + '{}' + "','','" + cur_time + "','Y','" + 'M' + "','" +
                            'LMT' + "','" + str(symbol) + str(orderId) + "'," + str(quantity) + ", '" + market_session + "', " + priceDownStep + ")")

                    elif size_flag == 'N':
                        SQLCommand = ("INSERT INTO tbl_Python_stock_OrderInfo ([OrderId], [Platform], [Symbol], [OrderInstruction], [OrderPrice], [OrderDate], [OrderStatus], "
                            "[OrderPlaceJson], [LatestFlag], [UpdateDate], [MailSent], [Manual_Auto_flag], [Reg_Flag], [symbol_ord], [OrderQty], [MarketSession], [PriceDownStep]) "
                            "VALUES (" + str(orderId) + ",'IB','" + symbol + "','" + action + "'," + str(lmtPrice) + ",'" + 
                            cur_time + "','" + str(status) + "','" + '{}' + "','','" + cur_time + "','Y','" + 'M' + "','" +
                            'LMT' + "','" + str(symbol) + str(orderId) + "'," + str(quantity) + ", '" + market_session + "', -1)")
            
            cursor = g_connection.cursor()
            cursor.execute(SQLCommand)
            insertData = cursor.fetchone()

            if isOrderID == False:
                g_orderStatus.insert(0, insertData)  

            g_connection.commit()
    
    # update order data to [tbl_Python_stock_OrderInfo] table
    def update_orderinfo(self, orderId, status, filled, remaining, lastFillPrice):
        tz_CT = pytz.timezone('America/Chicago')
        datetime_CT = datetime.now(tz_CT)
        cur_time = datetime_CT.strftime("%Y-%m-%d %H:%M:%S")
        global g_orderStatus

        for index, status in enumerate(g_orderStatus):
            if status[0] == orderId:
                g_orderStatus[index][3] = status

        SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET UpdateDate = GETDATE() , OrderStatus = '" + status + "', "
            "ExecutedPrice = " + str(lastFillPrice) + ", FilledQuantity = " + filled + ", ExecutedTime = '" + str(cur_time) + "' "
            "WHERE OrderID = " + orderId)

        cursor = g_connection.cursor()
        cursor.execute(SQLCommand)
        g_connection.commit()
    
    def update_buy_status(self, symbol, orderID):
        global g_orderStatus

        for index, status in enumerate(g_orderStatus):
            if status[0] == orderID:
                g_orderStatus[index][7] = 0

        SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET PlaceDownStep = 0 " 
            "WHERE Symbol = '" + str(symbol) + "' AND OrderInstruction = 'Buy'")
        cursor = g_connection.cursor()
        cursor.execute(SQLCommand)
        g_connection.commit()

    
    def order_operation(self, reqId, currentPrice):

        for index, symbol in enumerate(g_symbols):
            size_flag = symbol[14]
            custom_buy_price = symbol[9]
            custom_sell_price = symbol[10]
            symbol_name = symbol[0]
            stock_quantity = int(symbol[15])

            if reqId == symbol[11]:
                # Check if size flag is Y or N
                if size_flag == 'Y':
                    print(size_flag, custom_buy_price, symbol_name, currentPrice, "33333333333333333333333333333333333333333333333333")
                  
                    orderInfo_by_sizeFlag_orderMethod = self.check_previous_status(symbol_name, 'Y', 'BUY')
                    if currentPrice <= custom_buy_price:
                        print(size_flag, custom_buy_price, symbol_name, currentPrice, "44444444444444444444444444444444444444444444444444444444444444")

                        if len(orderInfo_by_sizeFlag_orderMethod) == 0:
                            print(size_flag, custom_buy_price, symbol_name, currentPrice, "55555555555555555555555555555555555555555555555")
                            self.submit_order(g_contracts[index][0], 'BUY', stock_quantity, 'LMT', custom_buy_price)

                        elif len(orderInfo_by_sizeFlag_orderMethod) > 0 and orderInfo_by_sizeFlag_orderMethod[0][3] == 'Filled':
                            print(size_flag, custom_buy_price, symbol_name, currentPrice, "66666666666666666666666666666666666666666666666666666666")
                          
                            last_price_down_step = orderInfo_by_sizeFlag_orderMethod[0][7]
                            total_step = math.ceil(int(symbol[2])/int(symbol[16]))

                            if (total_step > last_price_down_step) and currentPrice <= (custom_buy_price - int(last_price_down_step)*int(symbol[16])):
                                order_quantity = int(symbol[15])
                                if total_step == (int(last_price_down_step) + 1):
                                    order_quantity = int(symbol[2]) - int(last_price_down_step) * int(symbol[15])

                                self.submit_order(g_contracts[index][0], 'BUY', order_quantity, 'LMT', custom_buy_price)

                    elif currentPrice >= custom_sell_price: 
                        print(size_flag, custom_buy_price, symbol_name, currentPrice, "77777777777777777777777777777777777777777777777777777")
                        totalSellStocks = 0
                        if len(orderInfo_by_sizeFlag_orderMethod) > 0:
                            # summary all buy stocks
                            for order_info in orderInfo_by_sizeFlag_orderMethod:
                                if order_info[3] == 'Filled':
                                    totalSellStocks = totalSellStocks + order_info[8]
                           
                            if totalSellStocks > 0:
                                if len(self.check_previous_status(symbol_name, 'Y', 'SELL')) == 0 or (self.check_previous_status(symbol_name, 'Y', 'SELL'))[0][3] == 'Filled':
                                    self.submit_order(g_contracts[index][0], 'SELL', totalSellStocks, 'LMT', custom_sell_price)

                else:
                        orderInfo_by_sizeFlag_orderMethod = self.check_previous_status(symbol_name, 'N', 'BUY')
                        if currentPrice <= custom_buy_price:
                            if len(self.check_previous_status(symbol_name, 'N', 'BUY')) == 0 or (self.check_previous_status(symbol_name, 'N', 'BUY'))[0][3] == 'Filled':
                                self.submit_order(g_contracts[index][0], 'BUY', stock_quantity, 'LMT')

                        elif currentPrice >= custom_sell_price:
                            if len(self.check_previous_status(symbol_name, 'N', 'BUY')) > 0 and (self.check_previous_status(symbol_name, 'N', 'SELL'))[0][3] == 'Filled':
                                self.submit_order(g_contracts[index][0], 'SELL', orderInfo_by_sizeFlag_orderMethod[0][8], 'LMT')  
        
    # check if the previous status is Filled
    def check_previous_status(self, symbol_name, size_flag, action):
        global g_orderStatus
        orderInfo_by_sizeFlag_orderMethod = []

        for order_info in g_orderStatus:
            if action == 'BUY':
                if size_flag == 'Y':
                    if order_info[1] == symbol_name and order_info[2] == 'BUY' and order_info[7] > 0:
                        orderInfo_by_sizeFlag_orderMethod.append(order_info)

                elif size_flag == 'N':
                    if order_info[1] == symbol_name and order_info[2] == 'BUY' and order_info[7] == -1:
                        orderInfo_by_sizeFlag_orderMethod.append(order_info)

            elif action == 'SELL':
                if order_info[1] == symbol_name and order_info[2] == 'SELL':
                    orderInfo_by_sizeFlag_orderMethod.append(order_info)

        return orderInfo_by_sizeFlag_orderMethod

    # get open orders from [tbl_Python_stock_OrderInfo] table
    def get_order_info(self):
        SQLCommand = ("SELECT [OrderId],[Symbol],[OrderInstruction],[OrderStatus],[Manual_Auto_flag],[OrderPrice],[symbol_ord],[PriceDownStep],[OrderQty] "
            "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Platform = 'IB' and cast(UpdateDate as date) = cast(GETDATE() as date) ORDER BY OrderId DESC")
        cursor = g_connection.cursor()
        cursor.execute(SQLCommand)
        result = cursor.fetchall()
        final_result = [list(i) for i in result]
        return final_result
    
    # order of stock
    def submit_order(self, contract, direction, qty, ordertype, transmit=True, limitprice=0):
        # Create order object   
        app.reqOpenOrders()
        order = Order()
        order.action = direction
        order.totalQuantity = qty
        order.orderType = ordertype
        order.transmit = transmit
        if ordertype == 'LMT':
            order.lmtPrice = limitprice
        # submit order
        app.placeOrder(app.nextorderId, contract, order)
        orderID = app.nextorderId
        app.nextorderId += 1


def run_loop():
    app.run()

# connect to database
def connect_db(dbserver, dbname, dbuser, dbpass):
    # connection = pypyodbc.connect('Driver={SQL Server};'
    #                                 'Server=' + dbserver + ';'
    #                                 'Database=' + dbname + ';'
    #                                 'uid=' + dbuser + ';pwd=' + dbpass)
    # return connection

    conn = pypyodbc.connect(
        'Driver={SQL Server};' 'Server=DESKTOP-MEL3TAC;' 'Database=StockAlgo;')
    return conn

# disconnect from database
def disconnect_db():
    print("pre db disconnected")
    g_connection.close()
    print("db disconnected")

# get symbol data form tblSymbolWatchList
def get_symbols():
    SQLCommand = ("SELECT Symbol, NoOfTrades, OrderQuantity, TradingFlag, Manual_Auto_flag, AvgSpread_Change, "
                  "Reg_Flag, sellpriceChange, IIF([CustomAvgSpread] is NULL, 0, [CustomAvgSpread]) AS AvgSpread, CustomBuyPrice, CustomSellPrice, "
                  "symbol_id, symbol_ord, Order_Type, SizeFlag, Size, PriceDown FROM tblSymbolWatchList (nolock) "
                  "WHERE Order_Type = 'L' AND Manual_Auto_flag IS NOT NULL AND (Broker = 'IB' OR Broker = 'ALL') AND TradingFlag = 'Y'"
                  "ORDER BY Manual_Auto_flag, symbol_id")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result

# append contract object (pair of contract and req_id)
def append_contract_obj(symbol, req_id):
    contract = app.stock_contract(symbol)
    contract_obj = []
    contract_obj.append(contract)
    contract_obj.append(req_id)
    g_contracts.append(contract_obj)
    app.request_tick_df(contract_obj)

def main_thread():
    # check if market is opened
    while True:
        if g_stop_flag:
            break

        tz_CT = pytz.timezone('America/Chicago')
        datetime_CT = datetime.now(tz_CT)
        cur_time = datetime_CT.strftime("%H:%M:%S")
        if cur_time >= config["DEFAULT"]["END_TIME"] or cur_time < config["DEFAULT"]["PRE_START"]:
            print("Market was closed !!!")
            market_session = 'CLOSED'
            continue
        market_session = 'REGULAR'
        if cur_time < config["DEFAULT"]["START_TIME"]:
            market_session = 'PRE'

        if cur_time >= config["DEFAULT"]["EXTENDED_START"]:
            market_session = 'EXTENDED'

        print("Market Session is " + market_session)
        # get symbol updates and request tickData
        watch_symbols(market_session)
        time.sleep(int(config["DEFAULT"]["INTERVAL"]))

# get quote and update the tables
def watch_symbols(market_session):
    global g_symbols
    g_symbols = get_symbols()
    new_symbols = []
    current_contracts_symbols = []
    for contract_obj in g_contracts:
        current_contracts_symbols.append(contract_obj[0].symbol)
            
    for symbol in g_symbols:
        new_symbols.append(symbol[0])

        if symbol[0] not in current_contracts_symbols:
            append_contract_obj(symbol[0], symbol[11])

    for cur_contract in g_contracts:
        if cur_contract[0].symbol not in new_symbols:
            app.remove_contract(cur_contract[1])
    
    

if __name__ == "__main__":

    # Check if market time is opended
    while True:
        tz_CT = pytz.timezone('America/Chicago')
        datetime_CT = datetime.now(tz_CT)
        cur_time = datetime_CT.strftime("%H:%M:%S")
        print(cur_time)
        if cur_time >= config["DEFAULT"]["END_TIME"] or cur_time < config["DEFAULT"]["PRE_START"]:
            print("Market was closed !!!")
            market_session = 'CLOSED'
            continue
        elif cur_time < config["DEFAULT"]["START_TIME"]:
            market_session = 'PRE'
            print("Market was Pre !!!")
            continue
        elif cur_time >= config["DEFAULT"]["EXTENDED_START"]:
            market_session = 'EXTENDED'
            print("Market was Extended !!!")
            continue
        else:
            print("Market Session is Regular")
            break

    app = IBapi()
    app.nextorderId = None
    if config['DEFAULT']['DEV_ENV'] == '1':
        app.connect('127.0.0.1', 4002, 1)
    else:
        app.connect('127.0.0.1', 1234, 2)
    
    # Start the socket
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

    # database connect
    g_connection = connect_db(config["DEFAULT"]["DB_SERVER"], config["DEFAULT"]
                              ["DB_NAME"], config["DEFAULT"]["DB_USER"], config["DEFAULT"]["DB_PASS"])


    # get active symbols
    g_symbols = get_symbols()
    g_orderStatus = app.get_order_info()

    # check if the g_symbols data exists
    if len(g_symbols) == 0:
        print("Please add symbols into the [tblSymbolWatchList] table")
        disconnect_db()
        sys.exit()

    # creating initial contracts
    for symbol in g_symbols:
        append_contract_obj(symbol[0], symbol[11])
    
    # start the main threading
    main_thread = threading.Thread(target = main_thread)
    time.sleep(2)
    main_thread.start()

    # exception progress with Ctrl-C
    while True:
        try:
            time.sleep(1) #wait 1 second, then go back and ask if thread is still alive

        except KeyboardInterrupt: #if ctrl-C is pressed within that second,
            print('Exiting...')
            g_stop_flag = True
            disconnect_db()
            app.disconnect()
            break
        

