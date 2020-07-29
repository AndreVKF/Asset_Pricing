from Controller.Controller_Main import Controller
from datetime import datetime
import sys

if __name__=='__main__':
    Refdate = input("Refdate: ")
    # Timer
    iniTime = datetime.now()

    controller = Controller(Refdate=int(Refdate))

    ########### Update Indexes ###########
    controller.IndexesValueUpdate()

    ########### Update Instruments ###########
    controller.PricesUpdateByInstrument()

    ########### Update FRAs ###########
    controller.FRAsUpdateByInstrument()

    print(f"Total Runtime: {datetime.now() - iniTime}")