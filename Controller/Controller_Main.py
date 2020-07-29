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
        self.dtRefdate = pd.Timestamp(str(self.Refdate))
        self.Queries = Queries()

        # Views
        self.Views = MainView(Refdate=Refdate)


    '''
    ##################################### MAIN FUNCTIONS #####################################
    '''

    def PricesUpdateByInstrument(self, instruments='all', RequestDate=None):
        '''
        Function update price products
        '''
        if RequestDate is None:
            RequestDate = self.Refdate

        # Update All Instruments
        if instruments=='all':
            instrumentList = self.Views.AP_Connection.getData(query=f"SELECT Name FROM Instruments WHERE UpdateBBG=1")
            
            # Loop through instruments
            for inst in instrumentList['Name'].to_list():
                price_DF = self.Views.UpdatePrices_DF(requestDate=RequestDate, instrument=inst)
                self.UpdatePricesDB(RequestDate=RequestDate, UpdatePrices_DF=price_DF, instrument=inst)

        # Update One Instrument
        elif isinstance(instruments, str):
            price_DF = self.Views.UpdatePrices_DF(requestDate=RequestDate, instrument=instruments)
            self.UpdatePricesDB(RequestDate=RequestDate, UpdatePrices_DF=price_DF, instrument=inst)

        # Update List of Instruments
        elif isinstance(instruments, list):
            for inst in instruments:
                price_DF = self.Views.UpdatePrices_DF(requestDate=RequestDate, instrument=inst)
                self.UpdatePricesDB(RequestDate=RequestDate, UpdatePrices_DF=price_DF, instrument=inst)

    def FRAsUpdateByInstrument(self, instruments='all', RequestDate=None):
        '''
        Function to update FRAs
        '''
        if RequestDate is None:
            RequestDate = self.Refdate

        # Update all instruments
        if instruments=='all':
            instrumentList = self.Views.AP_Connection.getData(query=f"SELECT Name FROM Instruments WHERE CalculateFRA=1")

            # Loop through instruments
            for inst in instrumentList['Name'].to_list():
                FRA_DF = self.Views.calculateFRAs(requestDate=RequestDate, Instrument=inst)
                self.UpdateFRAsDB(FRA_DF=FRA_DF, RequestDate=RequestDate, instrument=inst)

        # Update One Instrument
        elif isinstance(instruments, str):
            FRA_DF = self.Views.calculateFRAs(requestDate=RequestDate, Instrument=instruments)
            self.UpdateFRAsDB(FRA_DF=FRA_DF, RequestDate=RequestDate, instrument=instruments)

        # Update List of Instruments
        elif isinstance(instruments, list):
            for inst in instruments:
                FRA_DF = self.Views.calculateFRAs(requestDate=RequestDate, Instrument=inst)
                self.UpdateFRAsDB(FRA_DF=FRA_DF, RequestDate=RequestDate, instrument=inst)

    def IndexesValueUpdate(self, source='BBG', RequestDate=None):
        '''
        Function to update IndexValue
        '''
        if RequestDate is None:
            RequestDate = self.Refdate

        # Update From BBG        
        Update_DF = self.Views.indexesValueBBG(requestDate=RequestDate)

        if not Update_DF.empty:
            # Delete History
            self.Views.AP_Connection.execQuery(query=f"DELETE FROM IndexesValue WHERE Refdate='{RequestDate}' AND Id_Index IN ({','.join(Update_DF['Id_Index'].astype(str).to_list())})")

            # Update Values
            self.Views.AP_Connection.insertDataFrame(tableDB='IndexesValue', df=Update_DF)

        # Update Values Synthetic
        IndexesValue_Syn_DF = self.Views.indexesValueCalculated(requestDate=RequestDate)

        if not IndexesValue_Syn_DF.empty:
            # Delete History
            self.Views.AP_Connection.execQuery(query=f"DELETE FROM IndexesValue WHERE Refdate='{RequestDate}' AND Id_Index IN ({','.join(IndexesValue_Syn_DF['Id_Index'].astype(str).to_list())})")

            # Update Values
            self.Views.AP_Connection.insertDataFrame(tableDB='IndexesValue', df=IndexesValue_Syn_DF)

    '''
    ##################################### AUXILIAR FUNCTIONS #####################################
    '''
    def UpdatePricesDB(self, UpdatePrices_DF, instrument=None, RequestDate=None):
        '''
        Flow controller to update Prices Table
        '''
        if instrument is not None:
            print(f'Updating PricesDB {instrument}')

        ''' Check if not empty '''
        if not UpdatePrices_DF.empty:
            if RequestDate is None:
                RequestDate = self.Refdate
            
            # Delete history
            self.Views.AP_Connection.execQuery(query=f"DELETE FROM Prices WHERE Refdate='{RequestDate}' AND Id_Product IN ({','.join(UpdatePrices_DF['Id_Product'].astype(str).to_list())})")

            # Insert Prices_DF
            self.Views.AP_Connection.insertDataFrame(tableDB='Prices', df=UpdatePrices_DF)
        else:
            print('No data avilable in BBG.')

    def UpdateFRAsDB(self, FRA_DF, RequestDate=None, instrument=None):
        '''
        Flow controller to update FRAs table
        '''
        if instrument is not None:
            print(f'Updating PricesDB {instrument}')
        else:
            return -1

        ''' Check if not empty '''
        if not FRA_DF.empty:
            if RequestDate is None:
                RequestDate = self.Refdate

            # Delete history
            self.Views.AP_Connection.execQuery(query=f"DELETE FROM FRA_Tb WHERE Id_Instrument=(SELECT Id FROM Instruments WHERE Name='{instrument}')")

            # Insert into FRA Table
            self.Views.AP_Connection.insertDataFrame(tableDB='FRA_Tb', df=FRA_DF)
        else:
            print(f"Empty FRA Table for {instrument}")

        
