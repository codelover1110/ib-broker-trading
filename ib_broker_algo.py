from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

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
g_contracts = []

g_stop_flag = False
g_market_session = ''

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bardata = {} #Initialize dictionary to store bar data
        self.orderinfo = []
        self.connection = None
        self.submit_flag = False
    
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print('The next valid order id is: ', self.nextorderId)

    def tick_df(self, reqId, contract):
        ''' custom function to request Tick Data '''
        self.reqTickByTickData(reqId, contract, "Last", 0, True)
        return self.bardata[reqId]

    def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAtrribLast, exchange, specialConditions):
        if tickType == 1:
            self.bardata[reqId] = price
            if self.submit_flag == False:
                self.order_operation(reqId)
                print("__________", "reqId=", reqId, "||", "price=", price, "__________")

    def stock_contract(self, symbol, secType='STK', exchange='ISLAND', currency='USD'):
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
                self.disconnect()
                # raise Exception ('Error with Tick data stream')
        except:
            print("Error with Tick data stream")


    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange,
                ':', order.action, order.orderType, order.totalQuantity, orderState.status)
  
        self.insert_orderinfo(orderId, contract.symbol, order.action, order.lmtPrice, orderState.status, order.totalQuantity)
        if orderState.status == 'Filled' and order.action == 'SELL':
            self.update_buy_status(contract.symbol)
    
    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)
        
        if status == 'Filled':
            self.update_orderinfo(orderId, status, filled, remaining, lastFillPrice)

        self.submit_flag = False

    def remove_contract(self, req_id):
        self.cancelTickByTickData(req_id)
    
        
    # get open orders from [tbl_Python_stock_OrderInfo] table
    def get_orderinfo(self):
        SQLCommand = ("SELECT [OrderId], [Symbol], [OrderInstruction], [OrderStatus], [Manual_Auto_flag], "
            "[OrderPrice], [symbol_ord], [PriceDownStep], [OrderQty], [ExecutedPrice], [FilledQuantity] "
            "FROM [tbl_Python_stock_OrderInfo] (nolock) WHERE Platform = 'IB' and cast(UpdateDate as date) = cast(GETDATE() as date) and OrderStatus != 'Inactive' and OrderStatus != 'Canceled' ORDER BY OrderId DESC")

        cursor = self.connection.cursor()
        cursor.execute(SQLCommand)
        result = cursor.fetchall()
        final_result = [list(i) for i in result]
        
        return final_result

    def get_symbol_data(self, symbol):
        for symbol_data in g_symbols:
            if symbol_data[0] == symbol:
                return symbol_data
        return []
    
    def get_symbol_data_byid(self, symbol_id):
        for symbol_data in g_symbols:
            if symbol_data[11] == symbol_id:
                return symbol_data
        return []

    def get_contract_data(self, symbol):
        for contract_data in g_contracts:
            if contract_data[0].symbol == symbol:
                return contract_data
        return []

    
    def get_last_step(self, symbol):
        # return -2: not tradable, -1: sizeflag = 'N', 0-n: previous step
        symbol_data = self.get_symbol_data(symbol)

        for order_info in self.orderinfo:
            if order_info[1] == symbol: # corresponding symbol
                if symbol_data[14] == 'Y': # if sizeflag = true
                    if order_info[2] == 'BUY' and order_info[3] == 'Filled':
                        last_step = order_info[7]
                        
                        total_qty = symbol_data[2]
                        size = symbol_data[15]
                        if total_qty <= last_step * size:
                            return -2
                        else:
                            return last_step

                    elif order_info[2] == 'SELL' and order_info[3] == 'Filled':
                        return 0
                    
                    else:
                        return -2

                else:
                    return -1
        if symbol_data[14] == 'Y':
            return 0
        else:
            return -1


    def get_placing_quantity(self, symbol, buy_flag, buy_step = -1):
        symbol_data = self.get_symbol_data(symbol)
        if buy_flag:
            total_qty = symbol_data[2]
            if buy_step == -1:
                return total_qty
            else:
                size = symbol_data[15]
                if total_qty >= (buy_step + 1) * size:
                    return size
                else:
                    return (total_qty - buy_step * size)

                
        else:
            sell_qty = 0
            for orderinfo in self.orderinfo:
                if orderinfo[1] == symbol and orderinfo[3] == 'Filled' and orderinfo[7] > 0 and orderinfo[2] == 'BUY':
                    sell_qty += orderinfo[10]
            return sell_qty
            

    # insert/update order data to [tbl_Python_stock_OrderInfo] table
    def insert_orderinfo(self, order_id, symbol, action, lmt_price, status, quantity):
        print("Why not calling presumit---------------->", order_id)
        global g_market_session
 
        insert_flag = True
        
        index = -1
        for orderinfo in self.orderinfo:
            index += 1

            if order_id == orderinfo[0]:
                if status == orderinfo[3]:
                    return

                insert_flag = False

                break


        symbol_data = self.get_symbol_data(symbol)

        price_down_step = 0
        if action == 'BUY':
            price_down_step = self.get_last_step(symbol) + 1 

        if insert_flag:

            SQLCommand = ("INSERT INTO tbl_Python_stock_OrderInfo ([OrderId], [Platform], [Symbol], [OrderInstruction], [OrderPrice], [OrderDate], [OrderStatus], "
                "[OrderPlaceJson], [LatestFlag], [UpdateDate], [MailSent], [Manual_Auto_flag], [Reg_Flag], [symbol_ord], [OrderQty], [MarketSession], [PriceDownStep]) "
                "VALUES (" + str(order_id) + ",'IB','" + symbol + "','" + action + "'," + str(lmt_price) + "," + 
                "GETDATE(),'" + "status" + "','" + '{}' + "','' , GETDATE(), 'Y' , '" + 'M' + "','" +
                'LIMIT' + "','" + symbol_data[12] + "'," + str(quantity) + ", '" + g_market_session + "', " + str(price_down_step) + ")")
            print(SQLCommand)

        else:
            SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET UpdateDate = GETDATE() , OrderStatus = '" + status + "' "
                "WHERE OrderID = " + str(order_id))
            print(SQLCommand)

        # try:
        cursor = self.connection.cursor()
        cursor.execute(SQLCommand)
        self.connection.commit()

        if insert_flag:
            if status != 'Inactive' or status != 'Canceled':
                temp = [order_id, symbol, action, status, 'M', lmt_price, symbol_data[12], price_down_step, quantity, 0, 0]
                self.orderinfo.insert(0, temp)
        else:
            if status == 'Inactive' or status == 'Canceled':
                self.orderinfo.pop(index)
            else:
                self.orderinfo[index][3] = status


        # except:
        #     return


    def get_orderinfo_byid(self, order_id):
        for order_info in self.orderinfo:
            if order_info[0] == order_id:
                return order_info
        return []

    
    # update order data to [tbl_Python_stock_OrderInfo] table
    def update_orderinfo(self, order_id, status, filled, remaining, last_fill_price):

        SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET UpdateDate = GETDATE() , OrderStatus = '" + status + "', "
            "ExecutedPrice = " + str(last_fill_price) + ", FilledQuantity = " + str(filled) + ", ExecutedTime = GETDATE() "
            "WHERE OrderID = " + str(order_id))

        cursor = self.connection.cursor()
        cursor.execute(SQLCommand)
        self.connection.commit()
        print(self.orderinfo)

        for idx, order_info in enumerate(self.orderinfo):
            if order_info[0] == order_id:
                self.orderinfo[idx][3] = status
                self.orderinfo[idx][9] = last_fill_price
                self.orderinfo[idx][10] = filled


    
    def update_buy_status(self, symbol):

        SQLCommand = ("UPDATE tbl_Python_stock_OrderInfo SET PriceDownStep = 0 " 
            "WHERE Symbol = '" + str(symbol) + "' AND OrderInstruction = 'BUY' AND OrderStatus = 'Filled' ")
        cursor = self.connection.cursor()
        cursor.execute(SQLCommand)
        self.connection.commit()

        for idx, order_info in enumerate(self.orderinfo):
            if order_info[1] == symbol and order_info[2] == 'BUY' and order_info[3] == 'Filled':
                self.orderinfo[idx][7] = 0

    
    def order_operation(self, reqId):
        symbol_data = self.get_symbol_data_byid(reqId)
        currentPrice = self.bardata[reqId]
        custom_buy_price = symbol_data[9]
        custom_sell_price = symbol_data[10]
        symbol = symbol_data[0]

        print("__________", "custom_buy_price=", custom_buy_price, "||", "custom_sell_price=", custom_sell_price, "__________")

        contract_data = self.get_contract_data(symbol)
        if len(contract_data) > 0:
            last_price_down_step = self.get_last_step(symbol)
            if last_price_down_step > -2:
                if last_price_down_step == -1:
                    buy_price = custom_buy_price
                else:    
                    buy_price = custom_buy_price - last_price_down_step * symbol_data[16]
            
                if currentPrice <= buy_price:
                    order_quantity = self.get_placing_quantity(symbol, True, last_price_down_step)
                    if order_quantity > 0:
                        
                        self.submit_flag = True
                        self.submit_order(contract_data[0], 'BUY', order_quantity, 'LMT', True, currentPrice)
                        return

            if currentPrice >= custom_sell_price:
                order_quantity = self.get_placing_quantity(symbol, False)
                if order_quantity > 0:

                    self.submit_flag = True
                    self.submit_order(contract_data[0], 'SELL', order_quantity, 'LMT', True, currentPrice)
  
 
    
    # order of stock
    def submit_order(self, contract, direction, qty, ordertype, transmit=True, limitprice=0):
        # Create order object   
        app.reqOpenOrders()
        print("__________ START ORDER __________")
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
        print("__________ SUCCESS ORDER || orderID=", orderID, "__________")
        
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
    around = 0
    # check if market is opened
    while True:
        if g_stop_flag:
            break
        around += 1
        tz_CT = pytz.timezone('America/Chicago')
        datetime_CT = datetime.now(tz_CT)
        cur_time = datetime_CT.strftime("%H:%M:%S")

        
        if cur_time >= config["DEFAULT"]["END_TIME"] or cur_time < config["DEFAULT"]["PRE_START"]:
            print("Market was closed !!!")
            g_market_session = 'CLOSED'
            app.orderinfo.clear()

        else:
            g_market_session = 'REGULAR'
            if cur_time < config["DEFAULT"]["START_TIME"]:
                g_market_session = 'PRE'

            if cur_time >= config["DEFAULT"]["EXTENDED_START"]:
                g_market_session = 'EXTENDED'
            print("__________ Main Thread Around ||", around, "__________")
            print("Market Session is " + g_market_session)

            if len(app.orderinfo) == 0:
                app.orderinfo = app.get_orderinfo()

            # get symbol updates and request tickData
            watch_symbols(g_market_session)

        time.sleep(int(config["DEFAULT"]["INTERVAL"]))

# get quote and update the tables
def watch_symbols(g_market_session):
    global g_symbols
    
    temp_sybmols = g_symbols
    g_symbols = get_symbols()

    for prev_symbol_data in temp_sybmols:
        symbol_data = app.get_symbol_data(prev_symbol_data[0])
        if prev_symbol_data != symbol_data:
            app.order_operation(prev_symbol_data[11])

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

def start_thread():
    app = IBapi()
    app.nextorderId = None
    if config['DEFAULT']['DEV_ENV'] == '1':
        app.connect('127.0.0.1', 4002, 1)
    else:
        app.connect('127.0.0.1', 4001, 2)
    
    api_thread = threading.Thread(target=run_loop)
    api_thread.start()


if __name__ == "__main__":

    app = IBapi()
    app.nextorderId = None
    if config['DEFAULT']['DEV_ENV'] == '1':
        app.connect('127.0.0.1', 4002, 1)
    else:
        app.connect('127.0.0.1', 4001, 2)
    
 
    # database connect
    g_connection = connect_db(config["DEFAULT"]["DB_SERVER"], config["DEFAULT"]
                              ["DB_NAME"], config["DEFAULT"]["DB_USER"], config["DEFAULT"]["DB_PASS"])

    app.connection = g_connection

    # get active symbols
    g_symbols = get_symbols()

    # get order information
    app.orderinfo = app.get_orderinfo()
    print(app.orderinfo)
    
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
            api_thread.join()
            time.sleep(1)
            start_thread()

    for symbol in g_symbols:
        append_contract_obj(symbol[0], symbol[11])
 
    main_thread = threading.Thread(target = main_thread)
    time.sleep(2)
    main_thread.start()


    while True:
        try: time.sleep(1) #wait 1 second, then go back and ask if thread is still alive
        except KeyboardInterrupt: #if ctrl-C is pressed within that second,
                                #catch the KeyboardInterrupt exception
            print('Exiting...')
            g_stop_flag = True
            disconnect_db()
            app.disconnect()

