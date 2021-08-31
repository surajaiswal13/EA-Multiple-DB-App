import mysql.connector as connection
import csv
import os

class SqlOps():
    def __init__(self,host,user,password):
        self.host = host
        # session['host'] = host
        self.user = user
        # session['user'] = user
        self.password = password
        # session['password'] = password
        # self.db = db
        conn = connection.connect(host = self.host , user = self.user , password = self.password , use_pure = True)
        self.conn = conn
        # session['db'] = db

        self.cur = self.conn.cursor()
            
    def sql_show_db(self):
        self.cur.execute('SHOW DATABASES')
        return self.cur.fetchall()
    
    def sql_show_tables(self,db):
        self.cur.execute('USE '+ db)
        self.cur.execute('SHOW TABLES')
        return self.cur.fetchall()
    
    def sql_create_db(self,db_name):
        return self.cur.execute('CREATE DATABASE ' + db_name)
    
    def sql_delete_db(self,db_name):
        return self.cur.execute('DROP DATABASE '+ db_name)
    
    def sql_create_table(self , db_name , table_name , columns):
        self.cur.execute('USE '+db_name)
        return self.cur.execute('CREATE TABLE '+table_name+' '+'('+columns+')')
    
    def sql_delete_table(self , db_name , table_name):
        self.cur.execute('USE '+db_name)
        return self.cur.execute('DROP TABLE '+table_name )
    
    def sql_insert_table(self ,db_name , table_name , values):
#         if db_name != None:
#             self.cur.execute('use '+db_name)
#         else:
#             pass
        self.cur.execute('use '+db_name)
        self.cur.execute('INSERT INTO '+table_name+' VALUES '+"("+ values +")")
        return self.db.commit()
    
    def sql_display_all_data(self, db_name, table_name):
        self.cur.execute('USE '+ db_name)
        self.cur.execute('SELECT * FROM '+table_name)
        return self.cur.fetchall()
    
    def sql_update_data(self , db_name, table_name ,set_value , where_value):
        self.cur.execute('USE '+ db_name)
        self.cur.execute('UPDATE '+table_name+ ' SET '+set_value+' WHERE '+where_value)
        return self.db.commit()
    
    def sql_bulk_insert(self , db_name , table_name , file_name):
        self.cur.execute('USE '+db_name)
        with open(file_name,'r') as f:
            next(f) 
            data = csv.reader(f , delimiter = '\n')

            for i in data:

                x = i[0].split(',')
                print(x)
                self.cur.execute('INSERT INTO '+table_name+' VALUES ("{p}","{q}","{r}")'.format(p=x[0],q=x[1],r=x[2]))
                self.db.commit()
                
    # def sql_download_data(self ,db_name , table_name , new_filename="sql_data"):
    #     try:
    #         self.cur.execute('USE '+db_name)
    #         self.cur.execute('SELECT * FROM '+table_name)
    #         data = self.cur.fetchall()
    #         f = open(new_filename+'.csv' , "x")
    #         for i in data:
    #             j = str(i).replace('(','').replace(')','')

    #             f.write(j+'\n')
    #             # print(i)
    #             # print(j)
    #         return 'Data Downloaded in '+new_filename
    #     except Exception as e:
    #         print(e)
    #     finally:
    #         f.close()

    def sql_download_data(self ,db_name , table_name , new_filename="sql_data"):
        # try:
        self.cur.execute('USE '+db_name)
        self.cur.execute('SELECT * FROM '+table_name)
        data = self.cur.fetchall()
        path = os.path.join(os.getcwd()+'/static/files/'+ new_filename +'.csv')
        f = open(path, "x")
        for i in data:
            j = str(i).replace('(','').replace(')','')

            f.write(j+'\n')
            # print(i)
            # print(j)
        # return 'Data Downloaded in '+new_filename
        f.close()
        return path
        # except Exception as e:
        #     print(e)
        # finally:
        #     f.close()