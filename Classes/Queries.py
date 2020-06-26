'''
    Class to store SQL Server queries
'''

class Queries():

    def selectProductsByInstruments(self, instrument):
        query = f"""
        SELECT
             * 
        FROM
            Products 
        WHERE 
            Id_Instrument = (SELECT Id FROM Instruments WHERE Name='{instrument}') 
        ORDER BY 
            Valuation
        """

        return query

    def instrumentsDF(self):
        query = f"""
        SELECT * FROM Instruments
        """

        return query

    def currenciesDF(self):
        query = f"""
        SELECT 
            Id AS Id_Currency
            ,Name AS Currency 
        FROM
             Currencies
        """

        return query

    def getInstruments(self):
        query = f"""
        SELECT * FROM Instruments
        """

        return query

    def getProducts(self):
        query = f"""
        SELECT * FROM Products
        """

        return query
