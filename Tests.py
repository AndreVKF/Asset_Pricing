from Views.MainView import MainView

from datetime import date, datetime
from bizdays import *

import pandas as pd
import numpy as np

Refdate = 20200703
Views = MainView(Refdate=Refdate)

Views.addProductToDB(BBG_Ticker='GDG10 Curncy', Instrument='FRA CUPOM CAMBIAL (GD)', IsGeneric=True, Generic_Maturity=12, Maturity_Type='Months')

# Views.updateProducts('FRA CUPOM CAMBIAL (GD)')
ListCols = list(Views.bbgDict[Views.keys_bbgDict['Nota do Tesouro Nacional (NTNB)']]['BBG_Fields'])


Views.API_BBG.BBG_POST(bbg_request='BDP', fields=['DUR_ADJ_MID', 'PX_LAST'], tickers=['ODF21 Comdty'])
Views.API_BBG.BBG_POST(bbg_request='BDP', fields=['YAS_RISK'], tickers=['US71647NAN93 Corp'], overrides={'PX_CLOSE_DT': '20190630'})

# Refdate
# Id_Product
# PU
# Yield
# Duration

# BMF DI1 Future (OD)
# Nota do Tesouro Nacional (NTNB)
# USSW - USD SWAP (LIB)
# BRAZIL CDS USD (CBRZ)
# FRA CUPOM CAMBIAL (GD)
# BMF US Dollar Fut (UC)
# ARGENTINA CDS USD (CARG)
# MEXICO CDS USD (CMEX)
# CHILE CDS USD (CCHI)
# PERU CDS USD (CPER)
# PANAMA CDS USD (CPAN)
# COLOMBIA CDS USD (CCOM)
# US 10YR NOTE FUT (TY)

instrument = 'BMF DI1 Future (OD)'
requestDate = 20200702

# Set for requesting data from BBG
keys_bbgPxRequest = {
    'BMF DI1 Future (OD)': 'set1'
}

keys_calculateDuration = {
    'BMF DI1 Future (OD)': 'calcDur1'
}

keys_bbgPxBDP = {
    'set1': {
        'Yield': 'PX_LAST',
        'PU': 'CONTRACT_VALUE'
    }
}

keys_bbgPxBDH = {
    'set1': {
        'Yield': 'PX_LAST',
        'PU': 'CONTRACT_VALUE'
    }
}


# Tickers to update
products = Views.AP_Connection.getData(query=f"SELECT Id AS Id_Product, BBG_Ticker, Expiration FROM Products WHERE Id_Instrument=(SELECT Id FROM Instruments WHERE Name='{instrument}')")
products['Expiration'] = pd.to_datetime(products['Expiration'])

pricesDBColList = Views.AP_Connection.getData(query=Views.Queries.columnNamesFromTable(tableName='Prices'))

# Update Prices
pxSource = 'BDH'
if datetime.strptime(str(requestDate), '%Y%m%d').date() == date.today():
    pxSource = 'BDP'

# Request Data
if pxSource == 'BDH':
    BBG_Fields = keys_bbgPxBDH[keys_bbgPxRequest[instrument]]
    BBG_Data = Views.API_BBG.BBG_POST(bbg_request=pxSource, tickers=products['BBG_Ticker'].to_list(), fields=list(BBG_Fields.values()), date_start=requestDate, date_end=requestDate).dropna()
else:
    BBG_Fields = keys_bbgPxBDP[keys_bbgPxRequest[instrument]]
    BBG_Data = Views.API_BBG.BBG_POST(bbg_request=pxSource, tickers=products['BBG_Ticker'].to_list(), fields=list(BBG_Fields.values())).dropna()

# Invert keys to items
colNames = dict((v,k) for k,v in BBG_Fields.items())
Insert_DataFrame = BBG_Data.rename(columns=colNames).reset_index().rename(columns={'index': 'BBG_Ticker'})

Insert_DataFrame = Insert_DataFrame.merge(products, how='left', on='BBG_Ticker')
Insert_DataFrame['Refdate'] = pd.to_datetime(datetime.strptime(str(requestDate), '%Y%m%d').date())

# Check if needs to calculate duration
if instrument in list(keys_calculateDuration.keys()):
    # Calculate duration by calculateDuration dictionary
    if keys_calculateDuration[instrument] == 'calcDur1':
        # BMF DI1
        # Adjust dataframe to show only values with open expiration
        Insert_DataFrame = Insert_DataFrame[(Insert_DataFrame['Expiration']>=datetime.strptime(str(requestDate), '%Y%m%d').date())]
        Insert_DataFrame['BizDays'] = Insert_DataFrame.apply(lambda x: Views.cal.bizdays(x.Refdate.strftime('%Y-%m-%d'), x.Expiration.strftime('%Y-%m-%d')), axis=1)
        Insert_DataFrame['BaseValue'] = 100000/((1+Insert_DataFrame['Yield']/100)**(Insert_DataFrame['BizDays']/252))
        Insert_DataFrame['DiscountValue'] = 100000/((1+Insert_DataFrame['Yield']/100+0.01/100)**(Insert_DataFrame['BizDays']/252))
        Insert_DataFrame['Duration'] = (np.abs(Insert_DataFrame['BaseValue']-Insert_DataFrame['DiscountValue'])/Insert_DataFrame['BaseValue'])*10000
        
Insert_DataFrame = Insert_DataFrame[[col for col in pricesDBColList['COLUMN_NAME'].to_list() if col in list(Insert_DataFrame.columns)]]

        
