import pandas as pd
import numpy as np
import ngshare as ng

body = {
    "table": 'QT_TradingDayNew',
    "field_list": ['TradingDate',
                   'IfTradingDay', 'SecuMarket', 'IfWeekEnd', 'IfMonthEnd', 'IfQuarterEnd', 'IfYearEnd'
                   ],
    "alterField": 'TradingDate',
    "startDate": '2026-01-01',
    "endDate": '2030-01-01',
}
dfs = ng.get_fromDate(body)
print(dfs)