from Classes.Generic import Generic
from Classes.Queries import Queries
from Views.MainView import MainView

from datetime import datetime
import pandas as pd

class Controller(Generic):
    ''' Constructor '''
    def __init__(self, Refdate=int(datetime.today().strftime("%Y%m%d"))):
        ''' Init Arguments '''
        # Attributes
        self.Refdate = Refdate
        self.dtRefdate = pd.to_datetime(str(self.Refdate), format="%Y%m%d")
        self.Queries = Queries()

        # Views
        self.Views = MainView(Refdate=Refdate)


    '''
    ##################################### MAIN FUNCTIONS #####################################
    '''

    def PricesUpdateByInstrument(self, priceDate=int(datetime.today().strftime("%Y%m%d")), instruments='all'):
        '''
        Function update price products
        '''
        # Update All Instruments
        if instruments=='all':
            instrumentList = self.Views.AP_Connection.getData(query=f"SELECT Name FROM Instruments WHERE UpdateBBG=1")
            
            # Loop through instruments
            for inst in instrumentList['Name'].to_list():
                price_DF = self.Views.UpdatePrices_DF(requestDate=priceDate, instrument=inst)
                self.UpdatePricesDB(Refdate=priceDate, UpdatePrices_DF=price_DF, instrument=inst)

        # Update One Instrument
        elif isinstance(instruments, str):
            price_DF = self.Views.UpdatePrices_DF(requestDate=priceDate, instrument=instruments)
            self.UpdatePricesDB(Refdate=priceDate, UpdatePrices_DF=price_DF, instrument=inst)

        # Update List of Instruments
        elif isinstance(instruments, list):
            for inst in instruments:
                price_DF = self.Views.UpdatePrices_DF(requestDate=priceDate, instrument=inst)
                self.UpdatePricesDB(Refdate=priceDate, UpdatePrices_DF=price_DF, instrument=inst)
                
    '''
    ##################################### AUXILIAR FUNCTIONS #####################################
    '''
    def UpdatePricesDB(self, Refdate, UpdatePrices_DF, instrument=None):
        if instrument is not None:
            print(f'Updating PricesDB {instrument}')

        ''' Check if not empty '''
        if not UpdatePrices_DF.empty:
            # Delete history
            self.Views.AP_Connection.execQuery(query=f"DELETE FROM Prices WHERE Refdate='{Refdate}' AND Id_Product IN ({','.join(UpdatePrices_DF['Id_Product'].astype(str).to_list())})")

            # Insert Prices_DF
            self.Views.AP_Connection.insertDataFrame(tableDB='Prices', df=UpdatePrices_DF)
        else:
            print('No data avilable in BBG.')
