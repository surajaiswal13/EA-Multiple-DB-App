from flask import Flask , render_template , redirect , request , flash , session , url_for
from sqlops import SqlOps
import pandas as pd
import json
from mongoDBOperations import MongoDBManagement
from flask import send_file
import os
import shutil
import time

app = Flask(__name__)

app.secret_key = "Multiple_Database_Api"

# MySQL
# from flask_mysqldb import MySQL


# app.config['MYSQL_USER'] = 'sql6433006'  # name and user are same
# app.config['MYSQL_PASSWORD'] = '9Jk2EEgEw5'
# app.config['MYSQL_HOST'] = 'sql6.freemysqlhosting.net'
# app.config['MYSQL_DB'] = 'sql6433006'
# app.config['MYSQL_CURSORCLASS'] = 'DictCursor'  # It is used for setting how the data will be returned like now it will return as dict

# cur = mysql.connection.cursor()
# cur.execute()

# mysql = MySQL(app)

@app.route('/',methods=['GET','POST'])
def home():
    try:
        if request.method == 'POST':
            selected_db = request.form['db']

            if selected_db == 'sql':
                # return redirect('/sql/select_ops')
                return render_template('sql/establish_conn.html',selected_db=selected_db) 
            elif selected_db == 'mongo':
                return render_template('mongodb/establish_mongo_conn.html',selected_db=selected_db)
            elif selected_db == 'cassandra':
                return render_template('Maintainence.html',selected_db=selected_db)  ## Templates

            return redirect('/')
            
        return render_template('index.html')
    except Exception as e:
        return e

# @app.before_request
# def before():
#     print("This is executed BEFORE each request.")

# connection class

# operation class for each db

@app.route('/hello/')
def hello():
    return "Hello World!"

# @app.route('/test')
# def new():
#     return render_template('test.html')

@app.route('/<selected_db>/establish_sql_connection',methods=['GET','POST'])
def create_conn(selected_db):
    # try:
    if request.method == 'POST':
        host = request.form['host_name']
        user = request.form['user_name']
        password = request.form['conn_password']
        # db = request.form['db_name']

        sql_obj = SqlOps(host,user,password)
        # session["sql_obj1"]=sql_obj.__dict__

        session['host'] = host
        session['user'] = user
        session['password'] = password
        # session['db'] = db
        
        return redirect('/sql/select_ops')
    # except Exception as e:
    #     return e


@app.route('/sql/select_ops',methods=['GET','POST'])
def select_ops():
    try:
        if request.method == 'POST':
            selected_op = request.form['operation']
            # return selected_op
            if selected_op == 'create_db':
                return redirect('/sql/create_database')
            elif selected_op == 'create_table':
                return redirect('/sql/create_table')
            elif selected_op == 'delete_db':
                return redirect('/sql/delete_database')
            elif selected_op == 'delete_table':
                return redirect('/sql/delete_table')
            elif selected_op == 'insert_table':
                return redirect('/sql/insert_table')
            elif selected_op == 'bulk_insert_table':
                return redirect('/sql/bulk_insert_table')
            elif selected_op == 'update_table':
                return redirect('/sql/update_table')
            elif selected_op == 'download_data':
                return redirect('/sql/download_data')
            elif selected_op == 'display_all_data':
                return redirect('/sql/display_all_data')
            elif selected_op == 'show_all_db':
                return redirect('/sql/show_databases')
            elif selected_op == 'show_all_tables':
                return redirect('/sql/show_all_tables')

            else:
                return render_template('Maintainence.html',selected_db=selected_db)

        return render_template('sql/selectOps.html')
    except Exception as e:
        return e

@app.route('/sql/create_database',methods=['GET','POST'])
def create_db():
    try:
        if request.method == 'POST':
            database_name = request.form['db_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_create_db(database_name)
            flash('"'+database_name+'"' + ' DataBase Created Successfully !!')
            return redirect('/sql/create_database')

        return render_template('sql/create_db.html')
    except Exception as e:
        return e

@app.route('/sql/create_table',methods=['GET','POST'])
def create_table():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            columns = request.form['columns']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_create_table(database_name ,table_name , columns)
            flash('"'+table_name+'"' + ' Table Created Successfully !!')
            return redirect('/sql/create_table')

        return render_template('sql/create_table.html')
    except Exception as e:
        return e

@app.route('/sql/delete_database',methods=['GET','POST'])
def delete_database():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_delete_db(database_name)
            flash('"'+database_name+'"' + ' Database Deleted Successfully !!')
            return redirect('/sql/delete_database')

        return render_template('sql/delete_database.html')
    except Exception as e:
        return e

@app.route('/sql/delete_table',methods=['GET','POST'])
def delete_table():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_delete_table(database_name,table_name)
            flash('"'+database_name+'"' + ' Database Deleted Successfully !!')
            return redirect('/sql/delete_table')

        return render_template('sql/delete_table.html')
    except Exception as e:
        return e

@app.route('/sql/insert_table',methods=['GET','POST'])
def insert_table():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            values = request.form['values']
            formatted_values = values.replace("'","")
            # print('INSERT INTO '+table_name+' VALUES '+"("+ formatted_values +")")
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_insert_table(database_name ,table_name , values)
            flash('"'+values+'"' + ' Values Inserted Successfully !!')
            return redirect('/sql/insert_table')
            # return 'INSERT INTO '+table_name+' VALUES '+"("+ formatted_values +")"


        return render_template('sql/insert_table.html')
    except Exception as e:
        return e

## MODIFY BULK INSERTION AUTOMATE IT

@app.route('/sql/bulk_insert_table',methods=['GET','POST'])
def bulk_insertion():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            file_name = request.form['file_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_bulk_insert(database_name,table_name,file_name)
            flash('Bulk Insertion Successful using'+file_name+' file')
            return redirect('/sql/bulk_insert_table')

        return render_template('sql/bulk_insertion.html')
    except Exception as e:
        return e

@app.route('/sql/update_table',methods=['GET','POST'])
def update_table():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            set_values = request.form['set_values']
            where_values = request.form['where_values']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_update_data(database_name,table_name,set_values , where_values)
            flash('Table Updated Successfully at '+where_values)
            return redirect('/sql/update_table')

        return render_template('sql/update_table.html')
    except Exception as e:
        return e

@app.route('/sql/download_data',methods=['GET','POST'])
def download_data():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            new_filename = request.form['new_filename']

            os.chmod(os.path.join(os.getcwd()+'\\static\\files'), 0o777)
            shutil.rmtree(os.path.join(os.getcwd()+'\\static\\files'), ignore_errors=True)
            os.mkdir(os.path.join(os.getcwd()+'\\static',"files"))

            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            file = sql_obj.sql_download_data(database_name,table_name,new_filename)
            print(file)
            flash('Data Downloaded Successfully at '+new_filename)
            # return redirect('/sql/download_data')
            return send_file(file, as_attachment=True, mimetype="text/csv")

        return render_template('sql/download_data.html')
    except Exception as e:
        flash('File with name "'+new_filename+'" already exists.')
        return e

@app.route('/sql/display_all_data',methods=['GET','POST'])
def display_all_data():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            display = sql_obj.sql_display_all_data(database_name,table_name)
            format_display = pd.DataFrame(display)
            html_format = format_display.to_html()
            flash('Data Displayed Successfully')
            return render_template('sql/display_all_data.html',html_format=html_format)

        return render_template('sql/display_all_data.html')
    except Exception as e:
        return e

# @app.route('/sql/display_all_data',methods=['GET','POST'])
# def display_all_data():
#     if request.method == 'POST':
#         database_name = request.form['database_name']
#         table_name = request.form['table_name']
#         sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
#         display = sql_obj.sql_display_all_data(database_name,table_name)
#         format_display = pd.DataFrame(display)
#         # html_format = format_display.to_html()
#         flash('Data Displayed Successfully')
#         return render_template('display_all_data.html',format_display=format_display)

#     return render_template('display_all_data.html')


@app.route('/sql/show_databases',methods=['GET','POST'])
def show_db():
    try:
        sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
        display_db = sql_obj.sql_show_db()
        format_display_db = pd.DataFrame(display_db)
        html_format_display_db = format_display_db.to_html()
        # flash('DataBase Listed Successfully !!')
        return render_template('sql/show_db.html',html_format_display_db=html_format_display_db)
    except Exception as e:
        return e

@app.route('/sql/show_all_tables',methods=['GET','POST'])
def show_tables():
    try:
        if request.method == 'POST':
            database_name = request.form['db_name']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            display_tables = sql_obj.sql_show_tables(database_name)
            format_display_tables = pd.DataFrame(display_tables)
            html_format_display_tables = format_display_tables.to_html()
            flash(database_name+"'s Tables Listed Successfully !!")
            return render_template('sql/show_tables.html',html_format_display_tables=html_format_display_tables)

        return render_template('sql/show_tables.html')
    except Exception as e:
        return e

        # --------- MONGODB OPS ----------

@app.route('/<selected_db>/establish_mongo_connection',methods=['GET','POST'])
def create_mongo_conn(selected_db):
    try:
        if selected_db == 'mongo':
            if request.method == 'POST':
                url = request.form['url']
                mongo_password = request.form['conn_password']
                url = url.replace('<password>',mongo_password)
                # print(url)
                session['url'] = url
                session['mongo_password'] = mongo_password
                return redirect('/mongo/select_ops')


        return redirect('/mongo/select_ops')
    except Exception as e:
        return e

@app.route('/mongo/select_ops',methods=['GET','POST'])
def mongo_select_ops():
    try:
        if request.method == 'POST':
            mongo_selected_op = request.form['mongo_operation']
            # return selected_op
            if mongo_selected_op == 'mongo_create_db':
                return redirect('/mongo/create_database')
            elif mongo_selected_op == 'create_collection':
                return redirect('/mongo/create_collection')
            elif mongo_selected_op == 'database_name':
                return redirect('/mongo/delete_database')
            elif mongo_selected_op == 'delete_collection':
                return redirect('/mongo/delete_collection')
            elif mongo_selected_op == 'insert_single_record':
                return redirect('/mongo/insert_single_record')
            elif mongo_selected_op == 'bulk_insert_records':
                return redirect('/mongo/bulk_insert_records')
            elif mongo_selected_op == 'update_single_record':
                return redirect('/mongo/update_single_record')
            elif mongo_selected_op == 'update_multiple_records':
                return redirect('/mongo/update_multiple_records')
            elif mongo_selected_op == 'download_data':
                return redirect('/mongo/download_data')
            elif mongo_selected_op == 'display_all_data':
                return redirect('/mongo/display_all_data')
            elif mongo_selected_op == 'show_all_db':
                return redirect('/mongo/show_databases')
            elif mongo_selected_op == 'show_all_collections':
                return redirect('/mongo/show_all_collections')

            else:
                return render_template('Maintainence.html',selected_db=selected_db)

        return render_template('mongodb/selectOps.html')
    except Exception as e:
        return e


@app.route('/mongo/create_database',methods=['GET','POST'])
def mongo_create_db():
    try:
        if request.method == 'POST':
            database_name = request.form['db_name']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.createDatabase(db_name=database_name)
            # print(session['url'])
            flash('"'+database_name+'"' + ' DataBase Created Successfully !!')
            return redirect('/mongo/create_database')

        return render_template('mongodb/create_db.html')
    except Exception as e:
        return e

@app.route('/mongo/create_collection',methods=['GET','POST'])
def mongo_create_table():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.createCollection(collection_name=collection_name,db_name=database_name)

            flash('"'+collection_name+'"' + ' Collection Created Successfully !!')
            return redirect('/mongo/create_collection')

        return render_template('mongodb/create_collection.html')
    except Exception as e:
        return e

@app.route('/mongo/delete_database',methods=['GET','POST'])
def mongo_delete_database():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']

            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.dropDatabase(db_name=database_name)

            flash('"'+database_name+'"' + ' Database Deleted Successfully !!')
            return redirect('/mongo/delete_database')

        return render_template('mongodb/delete_database.html')
    except Exception as e:
        return e

@app.route('/mongo/delete_collection',methods=['GET','POST'])
def mongo_delete_collection():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.dropCollection(collection_name=collection_name, db_name=database_name)

            flash('"'+collection_name+'"' + ' Collection Deleted Successfully !!')
            return redirect('/mongo/delete_collection')

        return render_template('mongodb/delete_collection.html')
    except Exception as e:
        return e

@app.route('/mongo/insert_single_record',methods=['GET','POST'])
def mongo_insert_collection():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            rec = request.form['record']
            # reco = json.loads(rec)
            print(rec)
            # print(reco)

            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.insertRecord(db_name=database_name, collection_name=collection_name, record=rec)

            flash('"'+rec+'"' + ' record Inserted Successfully !!')
            return redirect('/mongo/insert_single_record')


        return render_template('mongodb/insert_single_record.html')
    except Exception as e:
        print(e)
        # return e

@app.route('/mongo/bulk_insert_records',methods=['GET','POST'])
def mongo_bulk_insertion():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            records = request.form['records']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.insertRecords(db_name=database_name, collection_name=collection_name, records=records)

    # w = """[{"b":"1","b":"1","b":"1","b":"1"},{"b":"1","b":"1","b":"1","b":"1"}]"""
    # w = json.loads(w)
    # a.insertRecords('Ronaldo','Neymar',w)


            flash('Bulk Insertion of '+records+' Successful')
            return redirect('/mongo/bulk_insert_records')

        return render_template('mongodb/bulk_insert_records.html')
    except Exception as e:
        return e

@app.route('/mongo/update_single_record',methods=['GET','POST'])
def mongo_update_single_record():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            previous_record = request.form['previous_record']
            new_record = request.form['new_record']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            MongoClient.updateOneRecord(db_name=database_name, collection_name=collection_name,previous_record=previous_record,new_record=new_record)
            flash('Collection Updated Successfully at '+previous_record)
            return redirect('/mongo/update_single_record')

        return render_template('mongodb/update_single_record.html')
    except Exception as e:
        return e

@app.route('/mongo/update_multiple_records',methods=['GET','POST'])
def mongo_update_multiple_record():
    # try:
    if request.method == 'POST':
        database_name = request.form['database_name']
        collection_name = request.form['collection_name']
        previous_record = request.form['previous_record']
        new_record = request.form['new_record']
        MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
        MongoClient.updateMultipleRecord(db_name=database_name, collection_name=collection_name,previous_record=previous_record,new_record=new_record)
        flash('Collection Updated Successfully with multiple records of '+previous_record)
        return redirect('/mongo/update_multiple_records')

    return render_template('mongodb/update_multiple_records.html')
    # except Exception as e:
    #     return e

@app.route('/mongo/download_data',methods=['GET','POST'])
def mongo_download_data():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            collection_name = request.form['collection_name']
            new_filename = request.form['new_filename']
            # session['new_filename'] = new_filename

            os.chmod(os.path.join(os.getcwd()+'\\static\\files'), 0o777)
            shutil.rmtree(os.path.join(os.getcwd()+'\\static\\files'), ignore_errors=True)
            os.mkdir(os.path.join(os.getcwd()+'\\static',"files"))
            # time.sleep(8)
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            file = MongoClient.downloadDataFromCollection(db_name=database_name, collection_name=collection_name, file_name=new_filename)
            print(file)            


            # flash('Data Downloaded Successfully as '+new_filename+".csv")
            # return redirect('/mongo/download_data')
            return send_file(file, as_attachment=True, mimetype="text/csv")

            # return render_template('mongodb/download_data.html')

        return render_template('mongodb/download_data.html')
    except Exception as e:
        return e
    # else:
    #     # return redirect('/mongo/download_data')
    #     render_template('mongodb/download_data.html')

# @app.route('/mongo/display_all_data',methods=['GET','POST'])
# def mongo_display_all_data():
#     # try:
#     if request.method == 'POST':
#         database_name = request.form['database_name']
#         collection_name = request.form['collection_name']
#         MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
#         display = MongoClient.getResultToDisplayOnBrowser( db_name=database_name, collection_name=collection_name)
#         format_display = pd.DataFrame(display)
#         html_format = format_display.to_html()
#         flash('Data Displayed Successfully')
#         return render_template('mongodb/display_all_data.html',html_format=html_format)

@app.route('/mongo/display_all_data',methods=['GET','POST'])
def mongo_display_all_data():
    # try:
    if request.method == 'POST':
        database_name = request.form['database_name']
        collection_name = request.form['collection_name']
        MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
        display = MongoClient.getResultToDisplayOnBrowser( db_name=database_name, collection_name=collection_name)
        # format_display = pd.DataFrame(display)
        # html_format = format_display.to_html()
        # newTemp = []
        # for i in display:
        #     g = str(i)+"\n"
        #     newTemp.append(g)

        flash('Data Displayed Successfully')
        return render_template('mongodb/display_all_data.html',display=display)

    return render_template('mongodb/display_all_data.html')
    # except Exception as e:
    #     return e


@app.route('/mongo/show_databases',methods=['GET','POST'])
def mongo_show_db():
    try:
        MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
        display_db = MongoClient.getDatabaseList()
        format_display_db = pd.DataFrame(display_db)
        html_format_display_db = format_display_db.to_html()
        # flash('DataBase Listed Successfully !!')
        return render_template('mongodb/show_db.html',html_format_display_db=html_format_display_db)
    except Exception as e:
        return e

@app.route('/mongo/show_all_collections',methods=['GET','POST'])
def mongo_show_tables():
    try:
        if request.method == 'POST':
            database_name = request.form['db_name']
            MongoClient = MongoDBManagement(url=session['url'],password=session['mongo_password'])
            display_tables = MongoClient.getCollectionList(db_name=database_name)
            format_display_collections = pd.DataFrame(display_tables)
            html_format_display_collections = format_display_collections.to_html()
            flash(database_name+"'s Collections Listed Successfully !!")
            return render_template('mongodb/show_collections.html',html_format_display_collections=html_format_display_collections)

        return render_template('mongodb/show_collections.html')
    except Exception as e:
        return e

@app.route('/about_us')
def about_us():
    return render_template('about_us.html')

@app.route('/contact_us')
def contact_us():
    return render_template('contact_us.html')


if __name__ == "__main__":
    app.run(debug=True)