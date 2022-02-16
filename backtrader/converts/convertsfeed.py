from ..feeds import PandasData, PandasDirectData, GenericCSVData


class ConvertInfo(object):

    def __init__(self, maturity, coupon_schedule, principle=100):
        self.maturity = maturity
        self.principle = principle
        self.coupon_schedule = coupon_schedule

    def is_maturity(self, dt):
        return dt == self.maturity

    def is_coupon_date(self, dt):
        return dt in self.coupon_schedule

    def get_coupon(self, dt):
        return self.coupon_schedule.get(dt, 0)

    def __str__(self):
        return f"Face Value: {self.principle}\nMaturity: {self.maturity.strftime('%Y-%m-%d')}\n" \
               f"Coupon Schedule:\n{self.coupon_schedule.to_string()}"


class ConvertPandasData(PandasData):

    def __init__(self, convert_info: ConvertInfo, *args, **kwargs):
        super(ConvertPandasData, self).__init__(*args, **kwargs)
        self.convert_info = convert_info


class ConvertPandasDirectData(PandasDirectData):

    def __init__(self, convert_info: ConvertInfo, *args, **kwargs):
        super(ConvertPandasDirectData, self).__init__(*args, **kwargs)
        self.convert_info = convert_info


class ConvertGenericCSVData(GenericCSVData):

    def __init__(self, convert_info: ConvertInfo, *args, **kwargs):
        super(ConvertGenericCSVData, self).__init__(*args, **kwargs)
        self.convert_info = convert_info
