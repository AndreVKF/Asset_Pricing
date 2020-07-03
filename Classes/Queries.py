'''
    Class to store SQL Server queries
'''

class Queries():
    def columnNamesFromTable(self, tableName):
        query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = N'{tableName}'
        """

        return query

    def QFeriados_BRA(self):
        query = f"""
            SELECT
                *
            FROM
                [PM].[dbo].[Feriados_BRA]
            ORDER BY
                Data
            """

        return query

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
