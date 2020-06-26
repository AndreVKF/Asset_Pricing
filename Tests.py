from Views.MainView import MainView

from datetime import date, datetime
import pandas as pd

Refdate = 20200624
Views = MainView(Refdate=Refdate)

Views.updateProducts('US 10YR NOTE FUT (TY)')



Future_Months = {
    '1': 'F',
    '2': 'G',
    '3': 'H',
    '4': 'J',
    '5': 'K',    
    '6': 'M',
    '7': 'N',
    '8': 'Q',
    '9': 'U',
    '10': 'V',
    '11': 'X',
    '12': 'Z'
}

instrument = 'BMF DI1 Future (OD)'

# Produtos ja inseridos no BD
insertedProducts = Views.AP_Connection.getData(query=Views.Queries.selectProductsByInstruments(instrument=instrument))

# Atributes/Arguments
# Fields to request from Bloomberg
fields = [
    'SECURITY_NAME',
    'PARSEKYABLE_DES',
    'CRNCY',
    'FUTURES_VALUATION_DATE',
    'FUT_NOTICE_FIRST',
    'FUT_FIRST_TRADE_DT']
    
baseYear = Views.dtRefdate.year
Id_Instrument = Views.AP_Connection.getValue(query=f"SELECT Id FROM Instruments WHERE Name='{instrument}'")
prefixBBG = Views.AP_Connection.getValue(query=f"SELECT PrefixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')
sufixBBG = Views.AP_Connection.getValue(query=f"SELECT SufixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')

tickerList = []

# Loop to create tickers
for i in range(baseYear-15, baseYear+15):
    for key, value in Future_Months.items():
        if i==2020:
            BBG_Ticker = f"{prefixBBG}{value}{str(i)[-1:]} {sufixBBG}"
        else:
            BBG_Ticker = f"{prefixBBG}{value}{str(i)[-2:]} {sufixBBG}"
        tickerList.append(BBG_Ticker)

# Request from BBG
Prod_BBGData = Views.API_BBG.BBG_POST(bbg_request='BDP', tickers=tickerList, fields=fields)
Prod_BBGData.reset_index(inplace=True)

# Adjust columns
Prod_BBGData['FUTURES_VALUATION_DATE'] = pd.to_datetime(Prod_BBGData['FUTURES_VALUATION_DATE']/1000, unit='s')
Prod_BBGData['FUT_FIRST_TRADE_DT'] = pd.to_datetime(Prod_BBGData['FUT_FIRST_TRADE_DT']/1000, unit='s')
Prod_BBGData['FUT_NOTICE_FIRST'] = pd.to_datetime(Prod_BBGData['FUT_NOTICE_FIRST']/1000, unit='s')

Prod_BBGData.rename(columns={
    'index': 'Name',
    'CRNCY': 'Currency',
    'FUTURES_VALUATION_DATE': 'Valuation',
    'FUT_NOTICE_FIRST': 'Expiration',
    'FUT_FIRST_TRADE_DT': 'First_Trade_Date',
    'index': 'Name',
    'PARSEKYABLE_DES': 'BBG_Ticker',
    'SECURITY_NAME': 'Description'
}, inplace=True)

Prod_BBGData.dropna(inplace=True)
Prod_BBGData.drop_duplicates(subset ="BBG_Ticker", keep = False, inplace = True)

# Products to be inserted
Prod_Insert_DF = Prod_BBGData.loc[~Prod_BBGData['BBG_Ticker'].isin(insertedProducts['BBG_Ticker'])]
Prod_Insert_DF = Prod_Insert_DF.merge(Views.DF_Currencies, how='left', on='Currency')
Prod_Insert_DF['Id_Instrument'] = Id_Instrument

Insert_DF = Prod_Insert_DF[Views.Products_colList]

# Insert into DataBase
Views.AP_Connection.insertDataFrame(tableDB='Products', df=Insert_DF)



