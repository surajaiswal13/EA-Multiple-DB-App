import pymongo
import pandas as pd
import json
from flask import send_file
import os

class MongoDBManagement:

    def __init__(self, url, password):
        """
        This function sets the required url
        """
        try:
            self.url = url
            self.password = password
            # self.url = 'localhost:27017'
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def getMongoDBClientObject(self):
        """
        This function creates the client object for connection purpose
        """
        try:
            mongo_client = pymongo.MongoClient(self.url)
            return mongo_client
        except Exception as e:
            raise Exception("(getMongoDBClientObject): Something went wrong on creation of client object\n" + str(e))

    def closeMongoDBconnection(self, mongo_client):
        """
        This function closes the connection of client
        :return:
        """
        try:
            mongo_client.close()
        except Exception as e:
            raise Exception(f"Something went wrong on closing connection\n" + str(e))

    def isDatabasePresent(self, db_name):
        """
        This function checks if the database is present or not.
        :param db_name:
        :return:
        """
        try:
            mongo_client = self.getMongoDBClientObject()
            if db_name in mongo_client.list_database_names():
                mongo_client.close()
                return True
            else:
                mongo_client.close()
                return False
        except Exception as e:
            raise Exception("(isDatabasePresent): Failed on checking if the database is present or not \n" + str(e))

    def createDatabase(self, db_name):
        """
        This function creates database.
        :param db_name:
        :return:
        """
        try:
            database_check_status = self.isDatabasePresent(db_name=db_name)
            if not database_check_status:
                mongo_client = self.getMongoDBClientObject()
                database = mongo_client[db_name]
                mongo_client.close()
                return database
            else:
                mongo_client = self.getMongoDBClientObject()
                database = mongo_client[db_name]
                mongo_client.close()
                return database
        except Exception as e:
            raise Exception(f"(createDatabase): Failed on creating database\n" + str(e))

    def dropDatabase(self, db_name):
        """
        This function deletes the database from MongoDB
        :param db_name:
        :return:
        """
        try:
            mongo_client = self.getMongoDBClientObject()
            if db_name in mongo_client.list_database_names():
                mongo_client.drop_database(db_name)
                mongo_client.close()
                return True
        except Exception as e:
            raise Exception(f"(dropDatabase): Failed to delete database {db_name}\n" + str(e))

    def getDatabase(self, db_name):
        """
        This returns databases.
        """
        try:
            mongo_client = self.getMongoDBClientObject()
            mongo_client.close()
            return mongo_client[db_name]
        except Exception as e:
            raise Exception(f"(getDatabase): Failed to get the database list")

    def getCollection(self, collection_name, db_name):
        """
        This returns collection.
        :return:
        """
        try:
            database = self.getDatabase(db_name)
            return database[collection_name]
        except Exception as e:
            raise Exception(f"(getCollection): Failed to get the database list.")

    def isCollectionPresent(self, collection_name, db_name):
        """
        This checks if collection is present or not.
        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            database_status = self.isDatabasePresent(db_name=db_name)
            if database_status:
                database = self.getDatabase(db_name=db_name)
                if collection_name in database.list_collection_names():
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            raise Exception(f"(isCollectionPresent): Failed to check collection\n" + str(e))

    def createCollection(self, collection_name, db_name):
        """
        This function creates the collection in the database given.
        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if not collection_check_status:
                database = self.getDatabase(db_name=db_name)
                collection = database[collection_name]
                return collection
        except Exception as e:
            raise Exception(f"(createCollection): Failed to create collection {collection_name}\n" + str(e))

    def dropCollection(self, collection_name, db_name):
        """
        This function drops the collection
        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.drop()
                return True
            else:
                return False
        except Exception as e:
            raise Exception(f"(dropCollection): Failed to drop collection {collection_name}")

    def insertRecord(self, db_name, collection_name, record):
        """
        This inserts a record.
        :param db_name:
        :param collection_name:
        :param record:
        :return:
        """
        try:
            # collection_check_status = self.isCollectionPresent(collection_name=collection_name,db_name=db_name)
            # print(collection_check_status)
            # if collection_check_status:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)
            record = record.replace("'",'"')
            record = json.loads(record)
            collection.insert_one(record)
            sum = 0
            return "rows inserted"
        except Exception as e:
            print(e)
            raise Exception(f"(insertRecord): Something went wrong on inserting record\n" + str(e))

    def insertRecords(self, db_name, collection_name, records):
        """
        This inserts a record.
        :param db_name:
        :param collection_name:
        :param record:
        :return:
        """
        try:
            # collection_check_status = self.isCollectionPresent(collection_name=collection_name,db_name=db_name)
            # print(collection_check_status)
            # if collection_check_status:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)
            # print(records)
            record_Temp = str(records)
            # print(record_Temp)
            # Temp_1 = record_Temp.replace('"',"'").replace("'",'"')
            Temp_1 = record_Temp.replace("'",'"')
            # print(Temp_1)
            record = json.loads(Temp_1)
            collection.insert_many(record)
            sum = 0
            return f"rows inserted "
        except Exception as e:
            raise Exception(f"(insertRecords): Something went wrong on inserting record\n" + str(e))

    def findfirstRecord(self, db_name, collection_name,query=None):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            print(collection_check_status)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                print(collection)
                firstRecord = collection.find_one(query)
                return firstRecord
        except Exception as e:
            raise Exception(f"(findRecord): Failed to find record for the given collection and database\n" + str(e))

    def findAllRecords(self, db_name, collection_name):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                findAllRecords = collection.find()
                return findAllRecords
        except Exception as e:
            raise Exception(f"(findAllRecords): Failed to find record for the given collection and database\n" + str(e))

    def findRecordOnQuery(self, db_name, collection_name, query):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                findRecords = collection.find(query)
                return findRecords
        except Exception as e:
            raise Exception(
                f"(findRecordOnQuery): Failed to find record for given query,collection or database\n" + str(e))

    def updateOneRecord(self, db_name, collection_name, previous_record, new_record):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                # previous_records = self.findAllRecords(db_name=db_name, collection_name=collection_name)
                # new_records = query
                previous_record = previous_record.replace("'",'"')
                previous_record = json.loads(previous_record)
                previous_records = previous_record
                # print(type(previous_records))

                new_record = new_record.replace("'",'"')
                new_record = json.loads(new_record)
                new_records = new_record
                # print(new_records)
                updated_record = collection.update_one(previous_records, {"$set":new_records})
                return updated_record
        except Exception as e:
            raise Exception(
                f"(updateRecord): Failed to update the records with given collection query or database name.\n" + str(
                    e))

    def updateMultipleRecord(self, db_name, collection_name, previous_record, new_record):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                previous_record = previous_record.replace("'",'"')
                previous_record = json.loads(previous_record)
                previous_records = previous_record
                print(previous_records)

                new_record = new_record.replace("'",'"')
                new_record = json.loads(new_record)
                new_records = new_record
                print(new_records)

                updated_record = collection.update_many(previous_records, {"$set":new_records})
                return updated_record
        except Exception as e:
            raise Exception(
                f"(updateMultipleRecord): Failed to update the records with given collection query or database name.\n" + str(
                    e))

    def deleteRecord(self, db_name, collection_name, query):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.delete_one(query)
                return "1 row deleted"
        except Exception as e:
            raise Exception(
                f"(deleteRecord): Failed to update the records with given collection query or database name.\n" + str(
                    e))

    def deleteRecords(self, db_name, collection_name, query):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.delete_many(query)
                return "Multiple rows deleted"
        except Exception as e:
            raise Exception(
                f"(deleteRecords): Failed to update the records with given collection query or database name.\n" + str(e))

    def getDatabaseList(self):
        try:
            mongo_client = self.getMongoDBClientObject()
            database_list = mongo_client.list_database_names()
            return database_list
        except Exception as e:
            raise Exception(
                f"(getDatabaseList): Failed to list the records.\n" + str(e))

    def getCollectionList(self,db_name):
        try:
            mongo_client = self.getMongoDBClientObject()
            database = self.getDatabase(db_name=db_name)
            collection_list = database.list_collection_names()
            return collection_list
        except Exception as e:
            raise Exception(
                f"(getDatabaseList): Failed to list the records.\n" + str(e))

    def getDataFrameOfCollection(self, db_name, collection_name):
        """
        """
        try:
            all_Records = self.findAllRecords(collection_name=collection_name, db_name=db_name)
            dataframe = pd.DataFrame(all_Records)
            return dataframe
        except Exception as e:
            raise Exception(
                f"(getDataFrameOfCollection): Failed to get DatFrame from provided collection and database.\n" + str(e))

    def saveDataFrameIntoCollection(self, collection_name, db_name, dataframe):
        """
        """
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            dataframe_dict = json.loads(dataframe.T.to_json())
            if collection_check_status:
                self.insertRecords(collection_name=collection_name, db_name=db_name, records=dataframe_dict)
                return "Inserted"
            else:
                self.createDatabase(db_name=db_name)
                self.createCollection(collection_name=collection_name, db_name=db_name)
                self.insertRecords(db_name=db_name, collection_name=collection_name, records=dataframe_dict)
                return "Inserted"
        except Exception as e:
            raise Exception(
                f"(saveDataFrameIntoCollection): Failed to save dataframe value into collection.\n" + str(e))

    def getResultToDisplayOnBrowser(self, db_name, collection_name):
        """
        This function returns the final result to display on browser.
        """
        try:
            response = self.findAllRecords(db_name=db_name, collection_name=collection_name)
            
            # result = [str(i)+"\n" for i in response]
            
            result = []
            for i in response:
                result.append(i)

            # proper_result = str(result)+"\n"

            return result  ## It returns a list please unpack it by using for loop on html and usr br tag to seperate on every line.
        except Exception as e:
            raise Exception(
                f"(getResultToDisplayOnBrowser) - Something went wrong on getting result from database.\n" + str(e))

    # def downloadDataFromCollection(self, db_name, collection_name, file_name):
    #     """
    #     This function returns the final result to display on browser.
    #     """
    #     try:
    #         collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
    #         if collection_check_status:
    #             collection = self.getCollection(collection_name=collection_name, db_name=db_name)

    #             Temp_list = []
    #             for i in collection.find():
    #                 Temp_list.append(i)
                
    #             count = 0
    #             path = 'static\\files\\'+ file_name +'.csv'
    #             print(path)
    #             with open(path,"w") as f:
                    
    #                 for j in Temp_list:
    #                     a = str(j)
    #                     f.write(a+'\n')
    #                     count += 1
                
    #             f.close()
    #             return 'Number of Records in Collection :'+ str(len(Temp_list)) +' Wher NUmber of Records downloaded in file = '+ str(count)
    #     except Exception as e:
    #         print(e)

    def downloadDataFromCollection(self, db_name, collection_name, file_name):
        """
        This function returns the final result to display on browser.
        """
        # try:
        collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
        if collection_check_status:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)

            Temp_list = []
            for i in collection.find():
                Temp_list.append(i)
            
            count = 0
            path = os.path.join(os.getcwd()+'\\static\\files\\'+ file_name +'.csv')
            print(path)
            with open(path,"w") as f:
                
                for j in Temp_list:
                    a = str(j)
                    f.write(a+'\n')
                    count += 1
            
            f.close()
            return path
        # except Exception as e:
        #     print(e)
