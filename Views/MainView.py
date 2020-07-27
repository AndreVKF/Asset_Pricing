from Classes.Generic import Generic
from Classes.Queries import Queries

import pandas as pd
import numpy as np
from datetime import datetime
from bizdays import *

class MainView(Generic):
    ''' Constructor '''
    def __init__(self, Refdate=int(datetime.today().strftime("%Y%m%d"))):
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

        # Misc Variables
        self.DateColumns = ['MATURITY', 'FUTURES_VALUATION_DATE', 'FUT_NOTICE_FIRST', 'FUT_FIRST_TRADE_DT']
        self.dailyCDSIndexes = [5,6,8,9,10,11]

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
            'COLOMBIA CDS USD (CCOM)': 'set4',
            'BMF US Dollar Fut (UC)': 'set5'
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
            },
            # BMF US Dollar FUT
            'set5': {
                'BBG_Fields': {
                    'SECURITY_NAME',
                    'CRNCY',
                    'FUTURES_VALUATION_DATE',
                    'FUT_NOTICE_FIRST',
                    'FUT_FIRST_TRADE_DT'}
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

        # Set for requesting data from BBG
        self.keys_bbgPxRequest = {
            'BMF DI1 Future (OD)': 'set1',
            'FRA CUPOM CAMBIAL (GD)': 'set1',
            'Nota do Tesouro Nacional (NTNB)': 'set2',
            'US 10YR NOTE FUT (TY)': 'set2',
            'USSW - USD SWAP (LIB)': 'set3',
            'BRAZIL CDS USD (CBRZ)': 'set3',
            'ARGENTINA CDS USD (CARG)': 'set3',
            'MEXICO CDS USD (CMEX)': 'set3',
            'CHILE CDS USD (CCHI)': 'set3',
            'PERU CDS USD (CPER)': 'set3',
            'PANAMA CDS USD (CPAN)': 'set3',
            'COLOMBIA CDS USD (CCOM)': 'set3',
            'BMF US Dollar Fut (UC)': 'set4'
        }

        self.keys_calculateDuration = {
            'BMF DI1 Future (OD)': 'calcDur1',
            'Nota do Tesouro Nacional (NTNB)': 'calcDur2',
            'FRA CUPOM CAMBIAL (GD)': 'calcDur3'
        }

        self.keys_bbgPxBDP = {
            'set1': {
                'Yield': 'PX_LAST',
                'PU': 'CONTRACT_VALUE'
            },
            'set2': {
                'Yield': 'YLD_YTM_MID',
                'PU': 'PX_LAST',
                'RISK_MID': 'RISK_MID'
            },
            'set3': {
                'Yield': 'PX_LAST'
            },
            'set4': {
                'PU': 'PX_LAST'
            }
        }

        self.keys_bbgPxBDH = {
            'set1': {
                'Yield': 'PX_LAST',
                'PU': 'CONTRACT_VALUE'
            },
            'set2': {
                'Yield': 'YLD_YTM_MID',
                'PU': 'PX_LAST',
                'RISK_MID': 'RISK_MID'
            },
            'set3': {
                'Yield': 'PX_LAST'
            },
            'set4': {
                'PU': 'PX_LAST'
            }
        }

        # DataFrames from DB
        self.DF_Currencies = self.AP_Connection.getData(query=self.Queries.currenciesDF())
        self.DF_Instruments = self.AP_Connection.getData(query=self.Queries.instrumentsDF())
        self.DF_Indexes = self.AP_Connection.getData(query=self.Queries.indexesDF())

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

    def UpdatePrices_DF(self, requestDate, instrument):
        # Tickers to update
        products = self.AP_Connection.getData(query=f"SELECT Id AS Id_Product, BBG_Ticker, Expiration FROM Products WHERE Id_Instrument=(SELECT Id FROM Instruments WHERE Name='{instrument}')")
        products['Expiration'] = pd.to_datetime(products['Expiration'])

        pricesDBColList = self.AP_Connection.getData(query=self.Queries.columnNamesFromTable(tableName='Prices'))

        # Update Prices
        pxSource = 'BDH'
        if datetime.strptime(str(requestDate), '%Y%m%d').date() == date.today():
            pxSource = 'BDP'

        # Request Data
        # Try to get BBG Data
        try:
            if pxSource == 'BDH':
                BBG_Fields = self.keys_bbgPxBDH[self.keys_bbgPxRequest[instrument]]
                BBG_Data = self.API_BBG.BBG_POST(bbg_request=pxSource, tickers=products['BBG_Ticker'].to_list(), fields=list(BBG_Fields.values()), date_start=requestDate, date_end=requestDate).dropna()
                BBG_Data.rename(columns={'Ticker': 'BBG_Ticker'}, inplace=True)
                BBG_Data['Refdate'] = pd.to_datetime(BBG_Data['Refdate'])

            else:
                BBG_Fields = self.keys_bbgPxBDP[self.keys_bbgPxRequest[instrument]]
                BBG_Data = self.API_BBG.BBG_POST(bbg_request=pxSource, tickers=products['BBG_Ticker'].to_list(), fields=list(BBG_Fields.values())).dropna()
                BBG_Data = BBG_Data.reset_index().rename(columns={'index': 'BBG_Ticker'})
                BBG_Data['Refdate'] = pd.to_datetime(datetime.strptime(str(requestDate), '%Y%m%d').date())
        except:
            # Error while requesting BBG data
            print('Not possible to get BBG data !')
            return pd.DataFrame(columns=pricesDBColList['COLUMN_NAME'].to_list())
        
        else:
            # Process requested BBG data
            # Invert keys to items
            colNames = dict((v,k) for k,v in BBG_Fields.items())
            Insert_DataFrame = BBG_Data.rename(columns=colNames)

            # Join to get Id_Products
            Insert_DataFrame = Insert_DataFrame.merge(products, how='left', on='BBG_Ticker')

            # Add Last_Update
            Insert_DataFrame['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Check if needs to calculate duration
            if instrument in list(self.keys_calculateDuration.keys()):
                # Calculate duration by calculateDuration dictionary
                if self.keys_calculateDuration[instrument] == 'calcDur1':
                    # BMF DI1
                    # Adjust dataframe to show only values with open expiration
                    Insert_DataFrame = Insert_DataFrame[(Insert_DataFrame['Expiration']>=pd.Timestamp(str(requestDate)))]
                    Insert_DataFrame['BizDays'] = Insert_DataFrame.apply(lambda x: self.cal.bizdays(x.Refdate.strftime('%Y-%m-%d'), x.Expiration.strftime('%Y-%m-%d')), axis=1)
                    Insert_DataFrame['BaseValue'] = 100000/((1+Insert_DataFrame['Yield']/100)**(Insert_DataFrame['BizDays']/252))
                    Insert_DataFrame['DiscountValue'] = 100000/((1+Insert_DataFrame['Yield']/100+0.01/100)**(Insert_DataFrame['BizDays']/252))
                    Insert_DataFrame['Duration'] = (np.abs(Insert_DataFrame['BaseValue']-Insert_DataFrame['DiscountValue'])/Insert_DataFrame['BaseValue'])*10000

                elif self.keys_calculateDuration[instrument] == 'calcDur2':
                    # BNTNB
                    Insert_DataFrame['Duration'] = Insert_DataFrame['RISK_MID']/Insert_DataFrame['PU']*100

                elif self.keys_calculateDuration[instrument] == 'calcDur3':
                    # Adjust dataframe to show only values with open expiration
                    Insert_DataFrame = Insert_DataFrame[(Insert_DataFrame['Expiration']>=pd.Timestamp(str(requestDate)))]
                    Insert_DataFrame['Days360'] = Insert_DataFrame.apply(lambda x: self.days360(x.Refdate, x.Expiration), axis=1)
                    Insert_DataFrame['BaseValue'] = 50000/(1+(Insert_DataFrame['Yield']/100*Insert_DataFrame['Days360']/360))
                    Insert_DataFrame['DiscountValue'] = 50000/(1+((Insert_DataFrame['Yield']/100 + 0.01/100)*Insert_DataFrame['Days360']/360))
                    Insert_DataFrame['Duration'] = (np.abs(Insert_DataFrame['BaseValue']-Insert_DataFrame['DiscountValue'])/Insert_DataFrame['BaseValue'])*10000


            # DataFrame to be Inserted        
            Insert_DataFrame = Insert_DataFrame[[col for col in pricesDBColList['COLUMN_NAME'].to_list() if col in list(Insert_DataFrame.columns)]]

            # Adjust pending columns
            emptyCols = [col for col in pricesDBColList['COLUMN_NAME'].to_list() if col not in list(Insert_DataFrame.columns)]
            for col in emptyCols:
                Insert_DataFrame[col] = np.nan

            return Insert_DataFrame

    
    def indexesValueBBG(self, requestDate):
        '''
        Function to get IndexesValue from BBG
        '''
        # BBG Update
        indexesBBG = self.DF_Indexes[(self.DF_Indexes['Id_Source']==1)]

        # Check BDH/BDP
        pxSource = 'BDH'
        if datetime.strptime(str(requestDate), '%Y%m%d').date() == date.today():
            pxSource = 'BDP'

        # Get values from BBG
        indexesValue = self.API_BBG.BBG_POST(bbg_request=pxSource, tickers=indexesBBG['BBG_Ticker'].to_list(), fields='PX_LAST', date_start=requestDate, date_end=requestDate)

        # Adjust DataFrame
        if pxSource=='BDH':
            indexesValue.rename(columns={'PX_LAST': 'Value',
                'Ticker': 'BBG_Ticker'}, inplace=True)
        elif pxSource=='BDP':
            indexesValue.reset_index(inplace=True)
            indexesValue['Refdate'] = pd.to_datetime(datetime.strptime(str(requestDate), '%Y%m%d').date())
            indexesValue.rename(columns={'PX_LAST': 'Value',
                'index': 'BBG_Ticker'}, inplace=True)

        indexesValue_DF = indexesValue.merge(self.DF_Indexes, how='left', on='BBG_Ticker')

        return indexesValue_DF[self.AP_Connection.getData(query=self.Queries.columnNamesFromTable(tableName='IndexesValue'))['COLUMN_NAME'].to_list()]

    def indexesValueCalculated(self, requestDate):
        '''
        Function to generate DataFrame for calculated index value
        '''
        # Base DataFrame
        indexes_DF = self.DF_Indexes[(self.DF_Indexes['Id_Source']==2)]

        # I - Adjustment for daily CDS
        base_DF = indexes_DF.loc[indexes_DF['Id_Index'].isin(self.dailyCDSIndexes)]

        # Return DF
        return_DF = pd.DataFrame(base_DF['Id_Index'])
        return_DF['Value'] = 25
        return_DF['Refdate'] = pd.to_datetime(datetime.strptime(str(requestDate), '%Y%m%d').date())

        return return_DF[self.AP_Connection.getData(query=self.Queries.columnNamesFromTable(tableName='IndexesValue'))['COLUMN_NAME'].to_list()]

    '''
    ##################################### AUXILIAR FUNCTIONS #####################################
    '''
    def Create_Calendar(self):
        """
            Function to create self.calendar base
        """
        self.DF_Feriados_BRA['Data'].to_csv('Holidays.csv', header=False, index=None)
        self.holidays = load_holidays('Holidays.csv')

        self.cal = Calendar(self.holidays, ['Sunday', 'Saturday'], name='Brazil')

    def days360(self, start_date, end_date, method_eu=False):
        '''
            Function to calculate diff with days 360
        '''
        start_day = start_date.day
        start_month = start_date.month
        start_year = start_date.year
        end_day = end_date.day
        end_month = end_date.month
        end_year = end_date.year

        if (
            start_day == 31 or
            (
                method_eu is False and
                start_month == 2 and (
                    start_day == 29 or (
                        start_day == 28 and
                        start_date.is_leap_year is False
                    )
                )
            )
        ):
            start_day = 30

        if end_day == 31:
            if method_eu is False and start_day != 30:
                end_day = 1

                if end_month == 12:
                    end_year += 1
                    end_month = 1
                else:
                    end_month += 1
            else:
                end_day = 30

        return (
            end_day + end_month * 30 + end_year * 360 -
            start_day - start_month * 30 - start_year * 360)