from Classes.Generic import Generic
from Classes.Queries import Queries

import pandas as pd

class MainView(Generic):
    ''' Constructor '''
    def __init__(self, Refdate):
        ''' Init Arguments '''
        Generic.__init__(self)
        
        # Attributes
        self.Refdate = Refdate
        self.dtRefdate = pd.to_datetime(str(self.Refdate), format="%Y%m%d")
        self.Queries = Queries()
        
        self.Future_Months = Future_Months = {
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

        # DataFrames from DB
        self.DF_Currencies = self.AP_Connection.getData(query=self.Queries.currenciesDF())
        self.DF_Instruments = self.AP_Connection.getData(query=self.Queries.instrumentsDF())


    ''' Functions '''
    def updateProducts(self, instrument):
        bbgList1 = ['US 10YR NOTE FUT (TY)']
        
        if instrument in bbgList1:
            # BBG Request Arguments
            fields = [
                'SECURITY_NAME',
                'PARSEKYABLE_DES',
                'CRNCY',
                'FUT_NOTICE_FIRST',
                'FUT_FIRST_TRADE_DT']

            Products_colList = ['Name',
                'Description',
                'BBG_Ticker',
                'Id_Instrument',
                'Expiration',
                'Id_Currency',
                'First_Trade_Date']

        else:
               # BBG Request Arguments
                fields = [
                    'SECURITY_NAME',
                    'PARSEKYABLE_DES',
                    'CRNCY',
                    'FUTURES_VALUATION_DATE',
                    'FUT_NOTICE_FIRST',
                    'FUT_FIRST_TRADE_DT']

                Products_colList = ['Name',
                    'Description',
                    'BBG_Ticker',
                    'Id_Instrument',
                    'Expiration',
                    'Valuation',
                    'Id_Currency',
                    'First_Trade_Date']

        insertedProducts = self.AP_Connection.getData(query=self.Queries.selectProductsByInstruments(instrument=instrument))

        baseYear = self.dtRefdate.year
        Id_Instrument = self.AP_Connection.getValue(query=f"SELECT Id FROM Instruments WHERE Name='{instrument}'")
        prefixBBG = self.AP_Connection.getValue(query=f"SELECT PrefixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')
        sufixBBG = self.AP_Connection.getValue(query=f"SELECT SufixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')

        tickerList = []

        # Loop to create tickers
        for i in range(baseYear-15, baseYear+15):
            for key, value in self.Future_Months.items():
                if i==2020:
                    BBG_Ticker = f"{prefixBBG}{value}{str(i)[-1:]} {sufixBBG}"
                else:
                    BBG_Ticker = f"{prefixBBG}{value}{str(i)[-2:]} {sufixBBG}"
                tickerList.append(BBG_Ticker)

        # Request from BBG
        Prod_BBGData = self.API_BBG.BBG_POST(bbg_request='BDP', tickers=tickerList, fields=fields)
        Prod_BBGData.reset_index(inplace=True)

        if instrument in bbgList1:
            # Adjust columns
            Prod_BBGData['FUT_FIRST_TRADE_DT'] = pd.to_datetime(Prod_BBGData['FUT_FIRST_TRADE_DT']/1000, unit='s')
            Prod_BBGData['FUT_NOTICE_FIRST'] = pd.to_datetime(Prod_BBGData['FUT_NOTICE_FIRST']/1000, unit='s')
        else:
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
        Prod_Insert_DF = Prod_Insert_DF.merge(self.DF_Currencies, how='left', on='Currency')
        Prod_Insert_DF['Id_Instrument'] = Id_Instrument

        Insert_DF = Prod_Insert_DF[Products_colList]

        if len(Insert_DF)>0:
            print(Insert_DF[['Name', 'BBG_Ticker']])
        else:
            print('No new products to be added !!')

        # Insert into DataBase
        self.AP_Connection.insertDataFrame(tableDB='Products', df=Insert_DF)

                    