from Views.MainView import MainView
from Controller.Controller_Main import Controller

from datetime import date, datetime
from bizdays import *

import pandas as pd
import numpy as np

Refdate = 20200727
Controller = Controller(Refdate=Refdate)
Views = Controller.Views

DF_Instruments = Views.DF_Instruments

DF_Instruments[(DF_Instruments['CalculateFRA']==True)]

# Create FRA Table
Instrument = "USSW - USD SWAP (LIB)"
CalcValueType = "Yield"

# Get ViewName/CalcType
Instrument_Row = DF_Instruments[(DF_Instruments['Name']==Instrument)]

View_Name = Instrument_Row['View_Name'].values[0]
Id_CalcType = Instrument_Row['Id_CalculationType'].values[0]

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

# Additional Columns
FRA_DF['Refdate'] = pd.to_datetime(str(Refdate), format="%Y%m%d")
FRA_DF['Id_Instrument'] = DF_Instruments[(DF_Instruments['Name']==Instrument)]['Id'].values[0]

# Order Columns
FRA_DF = FRA_DF[['Refdate',
    'Id_Instrument',
    'Base_Valuation',
    'Base_Value',
    'Base_Id_Product',
    'End_Valuation',
    'End_Value',
    'End_Id_Product']]

########## Calculate FRAs According to CalculationType ##########
# Brazil Compounding => Workdays Base 252
if Id_CalcType == 1:
    FRA_DF['Base_Days'] = FRA_DF.apply(lambda x: Views.cal.bizdays(x.Refdate.strftime("%Y-%m-%d"), x.Base_Valuation.strftime("%Y-%m-%d")), axis=1)
    FRA_DF['End_Days'] = FRA_DF.apply(lambda x: Views.cal.bizdays(x.Refdate.strftime("%Y-%m-%d"), x.End_Valuation.strftime("%Y-%m-%d")), axis=1)
    FRA_DF['Between_Days'] = FRA_DF.apply(lambda x: Views.cal.bizdays(x.Base_Valuation.strftime("%Y-%m-%d"), x.End_Valuation.strftime("%Y-%m-%d")), axis=1)

    FRA_DF['FRA'] = (((((1+ FRA_DF['End_Value']/100) ** (FRA_DF['End_Days']/252)) / ((1+ FRA_DF['Base_Value']/100) ** (FRA_DF['Base_Days']/252))) ** (252/FRA_DF['Between_Days'])) - 1) * 100

# Base Days 360
elif Id_CalcType == 2:
    FRA_DF['Base_Days'] = FRA_DF.apply(lambda x: Views.days360(x.Refdate, x.Base_Valuation), axis=1)
    FRA_DF['End_Days'] = FRA_DF.apply(lambda x: Views.days360(x.Refdate, x.End_Valuation), axis=1)
    FRA_DF['Between_Days'] = FRA_DF.apply(lambda x: Views.days360(x.Base_Valuation, x.End_Valuation), axis=1)

    FRA_DF['FRA'] = (((FRA_DF['End_Value']/100*FRA_DF['End_Days']/360) - (FRA_DF['Base_Value']/100*FRA_DF['Base_Days']/360)) * 360/FRA_DF['Between_Days']) * 100


FRA_DF["Last_Update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

FRA_DF[Views.AP_Connection.getData(query=Views.Queries.columnNamesFromTable(tableName='FRA_Tb'))['COLUMN_NAME'].to_list()]
