from __future__ import (absolute_import, division, print_function, unicode_literals)
import backtrader as bt
import pandas as pd
import os
from backtrader.feeds import PandasData
from datetime import datetime
from operator import attrgetter

data_folder = r"D:\Documents\trading\converts\data"


class AddMoredata(PandasData):
    lines = ('open', 'high', 'low', 'close', 'volume', 'money', 'double_low')
    params = (('open', 4), ('high', 5), ('low', 6), ('close', 7), ('volume', 8), ('money', 9), ('double_low', 20))


class ConvertBaseStrategy(bt.Strategy):

    def __init__(self, code2index, max_holding=10):
        self.code2index = code2index
        self.index2code = {v: k for k, v in code2index.items()}
        self.max_holding = max_holding
        self.candidates = []
        self.trade_counter = 0

    def notify_trade(self, trade):
        idx = self.datas.index(trade.data)
        action = 'BOUGHT' if trade.long else 'SELL'
        print(f"{action} {self.index2code[idx]} {trade.size} @ {trade.price}")
        super(ConvertBaseStrategy, self).notify_trade(trade)

    # def notify_order(self, order):
    #     idx = self.datas.index(order.data)
    #     action = 'BOUGHT' if order.long else 'SELL'
    #     print(f"{action} {self.index2code[idx]} {order.size} @ {order.price}")

    def prenext(self):
        self.next()

    def next(self):
        dt = self.lines.datetime[0]
        print(self.data.num2date(dt))
        # print(self.positions)
        for idx, data in enumerate(self.datas):
            if data.datetime[0] == dt and data.double_low[0] > 150 and data in self.positions \
                    and self.positions[data].size != 0:
                idx = self.datas.index(data)
                print(self.index2code[idx])
                self.trade_counter += 1
                order = self.sell(data, size=self.positions[data].size, exectype=bt.Order.Market,
                                  tradeid=self.trade_counter)  # self.sell(data, self.positions[data].size,
                                                               # exectype=bt.Order.Market)
                print(order)

        vacancy = self.max_holding - len(self.positions)
        for data in self.candidates:
            if vacancy > 0 and data.double_low < 130 and data.close < 110:
                self.trade_counter += 1
                self.buy(data, size=100, exectype=bt.Order.Market, tradeid=self.trade_counter)
                vacancy -= 1

        self.candidates = sorted([data for data in self.datas if dt == data.datetime[0]], key=attrgetter('double_low'))


if __name__ == "__main__":
    info_df = pd.read_csv(os.path.join(data_folder, "converts_info.csv"), index_col='code', encoding='utf-8')
    all_data = {code: pd.read_csv(os.path.join(data_folder, "{}.csv".format(code)), parse_dates=[0], index_col=0,
                                  encoding='utf-8') for code in info_df.index}
    # 双低
    for cdf in all_data.values():
        cdf['premium'] = cdf.close_stock / cdf.new_convert_price
        cdf['double_low'] = cdf.close + cdf.premium

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(120000.0)

    code2index = {}
    for idx, (code, df) in enumerate(all_data.items()):
        code2index[code] = idx
        feed = AddMoredata(dataname=df, fromdate=datetime(2018, 1, 1), todate=datetime(2021, 8, 26))
        cerebro.adddata(feed)
        if idx > 10:
            break
    cerebro.addstrategy(ConvertBaseStrategy, code2index)
    results = cerebro.run()

    strat = results[0]

