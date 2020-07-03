from Classes.Generic import Generic
from Classes.Queries import Queries

import pandas as pd
from bizdays import *

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
        
        # Create Calendar
        self.DF_Feriados_BRA = self.AP_Connection.getData(query = self.Queries.QFeriados_BRA())
        self.Create_Calendar()

        self.DateColumns = ['MATURITY', 'FUTURES_VALUATION_DATE', 'FUT_NOTICE_FIRST', 'FUT_FIRST_TRADE_DT']

        # Ticker Set for Products Table
        self.keys_bbgDict = {
            'US 10YR NOTE FUT (TY)': 'set1',
            'Nota do Tesouro Nacional (NTNB)': 'set2',
            'BMF DI1 Future (OD)': 'set3',
            'FRA CUPOM CAMBIAL (GD)': 'set3',
            'USSW - USD SWAP (LIB)': 'set4',
            'BRAZIL CDS USD (CBRZ)': 'set4',
            'ARGENTINA CDS USD (CARG)': 'set4',
            'MEXICO CDS USD (CMEX)': 'set4',
            'CHILE CDS USD (CCHI)': 'set4',
            'PERU CDS USD (CPER)': 'set4',
            'PANAMA CDS USD (CPAN)': 'set4',
            'COLOMBIA CDS USD (CCOM)': 'set4'
        }

        self.bbgDict = {
            # US Treasury
            'set1' : {
                'BBG_Fields' : {
                    'SECURITY_NAME',
                    'PARSEKYABLE_DES',
                    'CRNCY',
                    'FUT_NOTICE_FIRST',
                    'FUT_FIRST_TRADE_DT'
                    },
                },
            # NTNB
            'set2' : {
                'BBG_Fields' : {
                    'SECURITY_NAME',
                    'PARSEKYABLE_DES',
                    'CRNCY',
                    'MATURITY'
                    },
                },
            # BMF DI1 Future (OD)
            'set3': {
                'BBG_Fields': {
                    'SECURITY_NAME',
                    'PARSEKYABLE_DES',
                    'CRNCY',
                    'FUTURES_VALUATION_DATE',
                    'FUT_NOTICE_FIRST',
                    'FUT_FIRST_TRADE_DT'}
                },
            # USD SWAP LIBOR
            # BZ CDS
            'set4': {
                'BBG_Fields': {
                    'SHORT_NAME',
                    'PARSEKYABLE_DES',
                    'CRNCY'}
            }
        }

        # Rename Columns to Insert Products        
        self.renameProdColumns = {
            'index': 'Name',
            'CRNCY': 'Currency',
            'FUTURES_VALUATION_DATE': 'Valuation',
            'FUT_NOTICE_FIRST': 'Expiration',
            'MATURITY': 'Expiration',
            'FUT_FIRST_TRADE_DT': 'First_Trade_Date',
            'PARSEKYABLE_DES': 'BBG_Ticker',
            'SECURITY_NAME': 'Description',
            'SHORT_NAME': 'Description'
        }

        # DataFrames from DB
        self.DF_Currencies = self.AP_Connection.getData(query=self.Queries.currenciesDF())
        self.DF_Instruments = self.AP_Connection.getData(query=self.Queries.instrumentsDF())

    '''
    ##################################### MAIN FUNCTIONS #####################################
    '''
    def updateProductsIntoDB(self, instrument, dtRange=15):
        '''
        Function to insert new products into database
        '''
        fields = list(self.bbgDict[self.keys_bbgDict[instrument]]['BBG_Fields'])

        baseYear = self.dtRefdate.year
        insertedProducts = self.AP_Connection.getData(query=self.Queries.selectProductsByInstruments(instrument=instrument))
        productsDBColList = self.AP_Connection.getData(query=self.Queries.columnNamesFromTable(tableName='Products'))
        Id_Instrument = self.AP_Connection.getValue(query=f"SELECT Id FROM Instruments WHERE Name='{instrument}'")
        prefixBBG = self.AP_Connection.getValue(query=f"SELECT PrefixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')
        sufixBBG = self.AP_Connection.getValue(query=f"SELECT SufixBBG FROM Instruments WHERE Name='{instrument}'", vlType='str')

        tickerList = []

        # Loop to create tickers
        for i in range(baseYear-dtRange, baseYear+dtRange):
            for key, value in self.Future_Months.items():
                BBG_Ticker = f"{prefixBBG}{value}{str(i)[-2:]} {sufixBBG}"
                tickerList.append(BBG_Ticker)

        # Request from BBG
        Prod_BBGData = self.API_BBG.BBG_POST(bbg_request='BDP', tickers=tickerList, fields=fields)
        Prod_BBGData.reset_index(inplace=True)

        # Adjust Date Columns
        for DateCol in [x for x in self.DateColumns if x in list(Prod_BBGData.columns)]:
            Prod_BBGData[DateCol] = pd.to_datetime(Prod_BBGData[DateCol]/1000, unit='s')

        # Rename Columns
        Prod_BBGData.rename(columns=self.renameProdColumns, inplace=True)

        # Drop NA/Duplicates
        Prod_BBGData.dropna(inplace=True)
        Prod_BBGData.drop_duplicates(subset="BBG_Ticker", keep=False, inplace=True)

        # Products to be inserted
        Prod_Insert_DF = Prod_BBGData.loc[~Prod_BBGData['BBG_Ticker'].isin(insertedProducts['BBG_Ticker'])]
        Prod_Insert_DF = Prod_Insert_DF.merge(self.DF_Currencies, how='left', on='Currency')
        Prod_Insert_DF['Id_Instrument'] = Id_Instrument

        Insert_DF = Prod_Insert_DF[[col for col in productsDBColList['COLUMN_NAME'].to_list() if col in list(Prod_Insert_DF.columns)]]

        if len(Insert_DF)>0:
            print(Insert_DF[['BBG_Ticker', 'Description']])
        else:
            print('No new products to be added !!')
            return None

        # Insert into DataBase
        self.AP_Connection.insertDataFrame(tableDB='Products', df=Insert_DF)


    def addProductToDB(self, BBG_Ticker, Instrument, IsGeneric=False, Generic_Maturity=None, Maturity_Type='Months'):
        '''
        Function to add a single product into database
        '''
        checkIfAlreadyInserted = self.AP_Connection.getValue(query=f"SELECT COUNT(*) FROM Products WHERE BBG_Ticker='{BBG_Ticker}'", vlType='int')
        # Check if product is already inserted on DataBase
        if checkIfAlreadyInserted:
            print('Product already inserted on DB.')
            return None

        fields = list(self.bbgDict[self.keys_bbgDict[Instrument]]['BBG_Fields'])
        productsDBColList = self.AP_Connection.getData(query=self.Queries.columnNamesFromTable(tableName='Products'))
        Id_Instrument = self.AP_Connection.getValue(query=f"SELECT Id FROM Instruments WHERE Name='{Instrument}'")
        prefixBBG = self.AP_Connection.getValue(query=f"SELECT PrefixBBG FROM Instruments WHERE Name='{Instrument}'", vlType='str')
        sufixBBG = self.AP_Connection.getValue(query=f"SELECT SufixBBG FROM Instruments WHERE Name='{Instrument}'", vlType='str')

        # Request from BBG
        Prod_BBGData = self.API_BBG.BBG_POST(bbg_request='BDP', tickers=[BBG_Ticker], fields=fields)
        Prod_BBGData.reset_index(inplace=True)

        # Adjust Date Columns
        for DateCol in [x for x in self.DateColumns if x in list(Prod_BBGData.columns)]:
            Prod_BBGData[DateCol] = pd.to_datetime(Prod_BBGData[DateCol]/1000, unit='s')

        # Rename Columns
        Prod_BBGData.rename(columns=self.renameProdColumns, inplace=True)

        # Drop NA/Duplicates
        Prod_BBGData.dropna(inplace=True)
        Prod_BBGData.drop_duplicates(subset="BBG_Ticker", keep=False, inplace=True)

        # Products to be inserted
        Prod_Insert_DF = Prod_BBGData.merge(self.DF_Currencies, how='left', on='Currency')
        Prod_Insert_DF['Id_Instrument'] = Id_Instrument
        Prod_Insert_DF['BBG_Ticker'] = BBG_Ticker

        # Generic Adjustments
        if IsGeneric:
            Prod_Insert_DF['IsGeneric'] = 1
            Prod_Insert_DF['Generic_Maturity'] = Generic_Maturity
            Prod_Insert_DF['Id_Generic_Maturity_Types'] = self.AP_Connection.getValue(query=f"SELECT Id FROM Generic_Maturity_Types WHERE Range='{Maturity_Type}'", vlType='int')

        Insert_DF = Prod_Insert_DF[[col for col in productsDBColList['COLUMN_NAME'].to_list() if col in list(Prod_Insert_DF.columns)]]
        print(Insert_DF[['BBG_Ticker', 'Description']])

        # Insert into DataBase
        self.AP_Connection.insertDataFrame(tableDB='Products', df=Insert_DF)


    '''
    ##################################### AUXILIAR FUNCTIONS #####################################
    '''
    def Create_Calendar(self):
        """Function to create self.calendar base
        """
        self.DF_Feriados_BRA['Data'].to_csv('Holidays.csv', header=False, index=None)
        self.holidays = load_holidays('Holidays.csv')

        self.cal = Calendar(self.holidays, ['Sunday', 'Saturday'], name='Brazil')