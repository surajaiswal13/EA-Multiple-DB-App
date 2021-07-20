from flask import Flask , render_template , redirect , request , flash , session , url_for
from sqlops import SqlOps
import pandas as pd

app = Flask(__name__)

app.secret_key = "Multiple_Database_Api"

@app.route('/',methods=['GET','POST'])
def home():
    try:
        if request.method == 'POST':
            selected_db = request.form['db']

            if selected_db == 'sql':
                # return redirect('/sql/select_ops')
                return render_template('establish_conn.html',selected_db=selected_db) 
            elif selected_db == 'mongo':
                return '<h1> Look Forward for Future Updates </h1>'
            elif selected_db == 'cassandra':
                return '<h1> Look Forward for Future Updates </h1>'  ## Templates

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

@app.route('/<selected_db>/establish_connection',methods=['GET','POST'])
def create_conn(selected_db):
    try:
        if request.method == 'POST':
            host = request.form['host_name']
            user = request.form['user_name']
            password = request.form['conn_password']
            sql_obj = SqlOps(host,user,password)
            # session["sql_obj1"]=sql_obj.__dict__
            session['host'] = host
            session['user'] = user
            session['password'] = password
        return redirect('/sql/select_ops')
    except Exception as e:
        return e


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
                return redirect('/sql/update_table')/sql/download_data
            elif selected_op == 'download_data':
                return redirect('/sql/download_data')
            elif selected_op == 'display_all_data':
                return redirect('/sql/display_all_data')
            elif selected_op == 'show_all_db':
                return redirect('/sql/show_databases')
            elif selected_op == 'show_all_tables':
                return redirect('/sql/show_all_tables')

            else:
                return '<h1> Not Implemented yet </h1>'

        return render_template('selectOps.html')
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

        return render_template('create_db.html')
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

        return render_template('create_table.html')
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

        return render_template('delete_database.html')
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

        return render_template('delete_table.html')
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


        return render_template('insert_table.html')
    except Exception as e:
        return e

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

        return render_template('bulk_insertion.html')
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

        return render_template('update_table.html')
    except Exception as e:
        return e

@app.route('/sql/download_data',methods=['GET','POST'])
def download_data():
    try:
        if request.method == 'POST':
            database_name = request.form['database_name']
            table_name = request.form['table_name']
            new_filename = request.form['new_filename']
            sql_obj = SqlOps(host =session['host'],user=session['user'],password=session['password'])
            sql_obj.sql_download_data(database_name,table_name,new_filename)
            flash('Data Downloaded Successfully at '+new_filename)
            return redirect('/sql/download_data')

        return render_template('download_data.html')
    except Exception as e:
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
            return render_template('display_all_data.html',html_format=html_format)

        return render_template('display_all_data.html')
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
        return render_template('show_db.html',html_format_display_db=html_format_display_db)
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
            return render_template('show_tables.html',html_format_display_tables=html_format_display_tables)

        return render_template('show_tables.html')
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