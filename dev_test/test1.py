from __future__ import (absolute_import, division, print_function, unicode_literals)
import backtrader as bt
import pandas as pd
import os
from pandas.tseries.offsets import DateOffset
from datetime import datetime
from operator import attrgetter
from backtrader.converts.convertsfeed import ConvertInfo, ConvertPandasData
from backtrader.converts.strategy import ConvertStrategy


data_folder = r"D:\Documents\trading\converts\data"


class AddMoredata(ConvertPandasData):
    lines = ('open', 'high', 'low', 'close', 'volume', 'money', 'double_low')
    params = (('open', 4), ('high', 5), ('low', 6), ('close', 7), ('volume', 8), ('money', 9), ('double_low', 20))


class ConvertDoubleLowStrategy(ConvertStrategy):

    def __init__(self, code2index, max_holding=10):
        self.code2index = code2index
        self.index2code = {v: k for k, v in code2index.items()}
        self.max_holding = max_holding
        self.candidates = []
        self.trade_counter = 0

    def notify_trade(self, trade):
        idx = self.datas.index(trade.data)
        action = 'BOUGHT' if trade.long else 'SELL'
        print(f"{trade.data.num2date(trade.dtopen).isoformat()} - {action} {self.index2code[idx]} {abs(trade.size)} @ {trade.price}")
        super(ConvertDoubleLowStrategy, self).notify_trade(trade)

    def prenext(self):
        self.next()

    def next(self):
        def _check_suspension(data):
            try:
                return data.open == 0 or data.volume == 0
            except:
                print('here')
                raise

        dt = self.lines.datetime[0]
        print(self.data.num2date(dt))
        # print(self.positions)
        has_position = 0
        for idx, data in enumerate(self.datas):
            if not _check_suspension(data) and data in self.positions and self.positions[data].size != 0:
                # print(self.candidates.index(data))
                if not data in self.candidates:
                    print("not in candidate", self.index2code[idx])
                if data.datetime[0] == dt and (data.double_low[0] > 150 or self.candidates.index(data) > 2*self.max_holding):
                    self.trade_counter += 1
                    self.sell(data, size=self.positions[data].size, exectype=bt.Order.Market,
                              tradeid=self.trade_counter)  # self.sell(data, self.positions[data].size,
                                                           # exectype=bt.Order.Market)
                else:
                    has_position += 1

        vacancy = self.max_holding - has_position
        for data in self.candidates:
            if vacancy < 1:
                break
            if data.double_low < 130 and data.close < 110 and not _check_suspension(data):
                amount = 100 - self.positions[data].size if data in self.positions else 100
                if amount <= 0:
                    continue
                self.trade_counter += 1
                self.buy(data, size=amount, exectype=bt.Order.Market, tradeid=self.trade_counter)
                vacancy -= 1

        self.candidates = sorted([data for data in self.datas if dt == data.datetime[0]], key=attrgetter('double_low'))


if __name__ == "__main__":
    info_df = pd.read_csv(os.path.join(data_folder, "converts_info.csv"), index_col='code', encoding='utf-8')
    all_data = {code: pd.read_csv(os.path.join(data_folder, "{}.csv".format(code)), parse_dates=[0], index_col=0,
                                  encoding='utf-8') for code in info_df.index}
    info_df2 = pd.read_excel(os.path.join(data_folder, "convert_redemption2.xlsx"), index_col='code')
    # 双低
    for cdf in all_data.values():
        cdf['premium'] = cdf.close_stock / cdf.new_convert_price - 1
        cdf['double_low'] = cdf.close + cdf.premium * 100

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.001)

    code2index = {}
    fd = datetime(2019, 10, 1)
    for idx, (code, df) in enumerate(all_data.items()):
        try:
            if info_df2.loc[code, 'maturity_date'] <= fd:
                continue
            code2index[code] = idx
            coupons = [float(x) for x in info_df2.loc[code, 'interest_schedule'].split('@@')]
            coupons[-1] = info_df2.loc[code, 'redeem_price'] - 100
            coupon_times = pd.date_range(info_df2.loc[code, 'interest_begin_date'], periods=len(coupons) + 1,
                                         freq=DateOffset(years=1), closed='right')
            coupon_schedule = pd.Series(coupons, index=coupon_times)
            convert_info = ConvertInfo(info_df2.loc[code, 'maturity_date'], coupon_schedule)
            feed = AddMoredata(convert_info, dataname=df, fromdate=fd, todate=datetime(2021, 8, 26))
            cerebro.adddata(feed)
        except:
            print(code)
    cerebro.addstrategy(ConvertDoubleLowStrategy, code2index, 10)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    result = cerebro.run()


