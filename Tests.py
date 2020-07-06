from Views.MainView import MainView
from Controller.Controller_Main import Controller

from datetime import date, datetime
from bizdays import *

import pandas as pd
import numpy as np

Refdate = 20200706
Controller = Controller(Refdate=Refdate)


price_DF = Controller.Views.UpdatePrices_DF(requestDate=Refdate, instrument='BMF DI1 Future (OD)')


Controller.PricesUpdateByInstrument(priceDate=Refdate)


dtList = list(Controller.Views.cal.seq('2010-01-01', '2020-07-06'))
dtList.reverse()

for dt in dtList:
    try:
        print(dt)
        Controller.PricesUpdateByInstrument(priceDate=dt.strftime('%Y%m%d'))
    except:
        pass




dtList[0].strftime('%Y%m%d')

Controller.Views.API_BBG.BBG_POST(bbg_request='BDP', tickers=['ODF21 Comdty'], fields='PX_LAST')