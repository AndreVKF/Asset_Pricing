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

    def getViewTable(self, refdate, viewName, fieldValueName, securityType='all'):
        # Ref_Id Adjustment and DataField Name
        if securityType=='all':
            refIdQ = "Ref_Id AS Ref_Id"
            addWhereQ = ""
        elif securityType=='Product':
            refIdQ = "Ref_Id AS Id_Product"
            addWhereQ = " AND Security_Type='Product'"
        elif securityType=='Index':
            refIdQ = "Ref_Id AS Id_Index"
            addWhereQ = " AND Security_Type='Index'"
        
        query = f"""
        SELECT
            Valuation
            ,{fieldValueName}
            ,{refIdQ}
        FROM
            {viewName}
        WHERE
            Refdate='{refdate}'
        """

        orderByQ = " ORDER BY Valuation"

        return (query + addWhereQ + orderByQ)

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

    def indexesDF(self):
        query = f"""
        SELECT 
            Id AS Id_Index
            ,Name
            ,BBG_Ticker
            ,Id_Instrument
            ,Id_Source
            ,Generic_Maturity
            ,Id_Generic_Maturity_Types
        FROM 
            Indexes
        """

        return query

    def QDeletePricesByProduct(self, Id_Products):
        query = f"""
        DELETE FROM Products WHERE Id_Product IN ({','.join(Id_Products)})
        """

        return query
