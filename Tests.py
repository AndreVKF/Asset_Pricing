from Views.MainView import MainView
from Controller.Controller_Main import Controller

from datetime import date, datetime
from bizdays import *

import pandas as pd
import numpy as np

Refdate = 20200728
Controller = Controller(Refdate=Refdate)
Views = Controller.Views

Views.calculateFRAs(requestDate=20200728, Instrument='BMF US Dollar Fut (UC)')

DF_Instruments = Views.DF_Instruments

DF_Instruments[(DF_Instruments['CalculateFRA']==True)]

# Create FRA Table
Instrument = "BMF DI1 Future (OD)"
CalcValueType = "Yield"

# Get ViewName/CalcType
Instrument_Row = DF_Instruments[(DF_Instruments['Name']==Instrument)]

View_Name = Instrument_Row['View_Name'].values[0]
Id_CalcType = Instrument_Row['Id_CalculationType'].values[0]

