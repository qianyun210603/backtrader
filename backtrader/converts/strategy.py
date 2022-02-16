from ..strategy import Strategy
from ..lineiterator import LineIterator


class ConvertStrategy(Strategy):

    def handle_payment(self):
        dt = self.datetime[0]
        for data in list(self.positions):
            position = self.positions[data]
            if position.size == 0:
                continue
            if data.convert_info.is_coupon_date(data.num2date(dt)):
                total_coupon = data.convert_info.get_coupon(data.num2date(dt)) * position.size
                self.broker.add_cash(total_coupon)
                print(f"{position.size} bonds pay coupon {total_coupon}")
            if data.convert_info.is_maturity(data.num2date(dt)):
                total_principle = data.convert_info.principle * position.size
                self.broker.add_cash(total_principle)
                del self.position[data]
                print(f"{position.size} bonds repay principle {total_principle}")

    def _oncepost(self, dt):
        for indicator in self._lineiterators[LineIterator.IndType]:
            if len(indicator._clock) > len(indicator):
                indicator.advance()

        if self._oldsync:
            # Strategy has not been reset, the line is there
            self.advance()
        else:
            # strategy has been reset to beginning. advance step by step
            self.forward()

        self.lines.datetime[0] = dt
        self._notify()
        self.handle_payment()

        minperstatus = self._getminperstatus()
        if minperstatus < 0:
            self.next()
        elif minperstatus == 0:
            self.nextstart()  # only called for the 1st value
        else:
            self.prenext()

        self._next_analyzers(minperstatus, once=True)
        self._next_observers(minperstatus, once=True)

        self.clear()
