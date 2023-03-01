from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from sqlalchemy import *
import logging


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    
class CreateDWHTable:
    def __init__(self,ExtractionConfig,MySQLConnectionString):
        self.ExtractionConfig = ExtractionConfig
        self.MySQLConnectionString = MySQLConnectionString
       
     
    def GetClientMetadata(self):
        '''
        --------------------------------------------
        Description:
                    (Each client has is own collection in the "MetadataDB" DB which contains the full metadata of it's datasouce.
                    The method connects to the client metadata collection in the "MetadataDB" (MongoDB)
                    and return the "metadata collection" according the entities on the "ExtractionConfig['entities']".
        
        Parameters:
                    - ExtractionConfig (python dict)
                
        Return:
                    - <class 'pymongo.cursor.Cursor'>
                    In order to see the variable value use list(<class 'pymongo.cursor.Cursor'>
        --------------------------------------------
        '''

        '''
        entities = self.ExtractionConfig["entities"]
        ScoopedEntities = [entity["EntityID"] for entity in entities]
        return ScoopedEntities
    
        '''
        
        #Generate "ScoopedEntities" a list which includes all the entities on the "ExtractionConfig" file
        
        ScoopedEntities = [dic['EntityID'] for dic in self.ExtractionConfig['entities']]
        logging.debug(f' The entities list on the ExtractionConfig: {ScoopedEntities}')
        MongoDBconnectionstring = 'mongodb+srv://talcohen0507:Taltool87!@erpdataplatform.t8ot6ep.mongodb.net/?retryWrites=true&w=majority'
        client = MongoClient(MongoDBconnectionstring)
        MetadataDB = client['MetadataDB']

        # Get the ClientMetadata collection
        ClientMetadata = MetadataDB[self.ExtractionConfig['AccountID']]

        # Return the filter metadata collection for the entities which the user ask on the "ExtractionConfig".
        query = {"_id": {"$in": ScoopedEntities}}
        return (ClientMetadata.find(query))
        
    
    
    def ChangingMetadataFormat(self):
    
        '''
        --------------------------------------------
        Description: 
                    Transforms client metadata obtained from the MongoDB database into a list of entities in a specific format.

        Parameters:
        - ExtractionConfig (dict) : The extraction configuration containing the entities and the AccountID of the client.

        Returns:
                - EntitiesList (list) : A list of entities in the format
                [{'TableName': '<table_name>', 'TableSpec': [(col1_name, col1_type), (col2_name, col2_type), ...]}, ...]
        --------------------------------------------
        '''
        # Get the client metadata from MongoDB
        ScoopedMetadata = self.GetClientMetadata()
        
        # Process the metadata to create the list of entities
        EntitiesList=[]
        for i in list(ScoopedMetadata):
            TableName = i['_id']
                        
            TableSpec = [
                            (d['fieldName']
                            ,eval(d['targetDataType'])
                            ,d['KeyFlag']
                            )
                        
                        for d in (i['Fields'])
                        ]
            
            Entity = {'TableName' : TableName, 'TableSpec' : TableSpec}
            
            EntitiesList.append(Entity)
        
        logging.debug(EntitiesList)
        return(EntitiesList)
    

    def CreateTablesObjects(self):
        '''
        --------------------------------------------
        Description:
                    The function takes as an input a python list which describe a DWH table (table name, colunms name, data type, primary key flag)
                    and create the table in the DWH according to the python list
                    
        Parameters:
                    - EntitiesList (python list)
                      EntitiesList Schema:
                                EntitiesList = [
                                                {'TableName': '<Table Name>',
                                                'TableSpec': [(<Field Name as a string>, Datatype)]}
                                                ]
                      Example:
                                EntitiesList = [
                                                {'TableName': 'ABILITYVALUES',
                                                'TableSpec': [('CODE', String(length=256)), ('DES', String(length=256)), ('ABILITYVALUE', <class 'sqlalchemy.sql.sqltypes.BigInteger'>)]}
                                                ]
                
        Return:
                    N/A The method create table and files in the DWH
        --------------------------------------------
        '''
        EntitiesList = self.ChangingMetadataFormat()
        #print(EntitiesList)
        # create a connection to the database
        DB = self.MySQLConnectionString + self.ExtractionConfig['AccountID']
        engine = create_engine(DB)
        # create a session
        Session = sessionmaker(bind=engine)
        session = Session()
        # create a MetaData object
        metadata = MetaData()

        
        TableGenerated_ls = []
        for i in EntitiesList:
            TableSpec = i['TableSpec']
            TableName = i['TableName']
            TableGenerated={}
            
            columns = [Column(n, t, primary_key=pk) for n, t, pk in TableSpec]
            columns += [Column("StagingDate", DateTime), Column("InsertDate", DateTime), Column("LastModifiedDate", DateTime)]

            logging.debug(columns)

            
            table = Table(TableName, metadata, *columns)
            
            if table in metadata.tables:
                logging.debug(f'Table {table.name} exists in the DB')
                
            
            else:
                logging.debug(f'Table {table.name} does not exists in the DB ---> Creating table {table.name}')
                
                try:
                    # create the table if it does not exist
                    #table.create(bind=engine,checkfirst=True)
                    table.create(bind=engine)
                    #logging.debug("Table %s created successfully with columns %s", TableName, *columns)
                    TableGenerated = {
                                        'TableName' : table.name,
                                        'NumColumns' : len(columns)}
                    
                    logging.debug(TableGenerated)
                    TableGenerated_ls.append(TableGenerated)
                    
                except Exception as e:
                    logging.error("An error occurred while creating table %s: %s", TableName, e)

            try:
                # commit the changes to the database
                session.commit()
                logging.debug("Changes committed successfully")
                
                
            except Exception as e:
                session.rollback()
                logging.error("An error occurred while committing changes: %s", e)
                raise

        
        # close the session
        session.close()
        return(TableGenerated_ls)


'''
--------------------------------------------
In this example, ExtractionConfig is a dictionary containing the configuration for extracting the data, and MySQLConnectionString is a string containing the connection string for connecting to the MySQL database.
You first create an instance of the CreateDWHTable class, passing in the ExtractionConfig and MySQLConnectionString as arguments to the constructor.
Then, you use this instance to call the ChangingMetadataFormet function to get the EntitiesList which is a list of entities in the specific format.
Finally, you use the instance again to call the CreateTablesObjects method and passing the EntitiesList as an argument to the function.
--------------------------------------------
'''