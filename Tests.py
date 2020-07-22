from Views.MainView import MainView
from Controller.Controller_Main import Controller

from datetime import date, datetime
from bizdays import *

import pandas as pd
import numpy as np

Refdate = 20200722
Controller = Controller(Refdate=Refdate)
Views = Controller.Views

DF_Instruments = Views.DF_Instruments

DF_Instruments[(DF_Instruments['CalculateFRA']==True)]

# Create FRA Table
Instrument = "BMF DI1 Future (OD)"
CalcValueType = "Yield"
CalcType = "Brazil Compounding"

# Get View Name
View_Name = DF_Instruments[(DF_Instruments['Name']==Instrument)]['View_Name'][0]

# Get DataFrame
View_DF = Views.AP_Connection.getData(query=Views.Queries.getViewTable(refdate=Refdate, viewName=View_Name, fieldValueName=CalcValueType, securityType='Product'), dtparse=['Valuation'])

# Create FRA Base DataFrame
for index, row in View_DF.iterrows():
    BaseFRA_DF = View_DF[(View_DF['Valuation']>row['Valuation'])]
    
    # Create/Concat FRA Table
    if not BaseFRA_DF.empty:
        BaseFRA_DF.rename(columns={'Valuation': 'End_Valuation',
            f'{CalcValueType}': 'End_Value',
            'Id_Product': 'End_Id_Product'}, inplace=True)

        BaseFRA_DF['Base_Valuation'] = row['Valuation']
        BaseFRA_DF['Base_Value'] = row[f'{CalcValueType}']
        BaseFRA_DF['Base_Id_Product'] = row['Id_Product']

        # Check if FRA_DF exists
        if 'FRA_DF' in locals():
            FRA_DF = pd.concat([FRA_DF, BaseFRA_DF], ignore_index=True)
        else:
            FRA_DF = BaseFRA_DF

# Order Columns
FRA_DF = FRA_DF[['Base_Valuation',
    'Base_Value',
    'Base_Id_Product',
    'End_Valuation',
    'End_Value',
    'End_Id_Product']]
