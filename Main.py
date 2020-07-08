from Controller.Controller_Main import Controller
import sys

if __name__=='__main__':
    controller = Controller(Refdate=int(sys.argv[1]))

    ########### Update Indexes ###########
    controller.IndexesValueUpdate()

    ########### Update Instruments ###########
    controller.PricesUpdateByInstrument()


