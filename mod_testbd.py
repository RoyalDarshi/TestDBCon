from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import mysql.connector
# import ibm_db
# import pyodbc
import pymongo

app = Flask(__name__)
CORS(app)

# Define the database connection functions
def connect_postgresql(connection_params):
    conn = None
    try:
        connection_params['host'] = connection_params.pop('hostname')
        connection_params['user'] = connection_params.pop('username')
        connection_params['dbname'] = connection_params.pop('database')
        connection_params['connect_timeout'] = 5
        conn = psycopg2.connect(**connection_params)
        return "PostgreSQL connection successful!", 200
    except psycopg2.OperationalError as e:
        return f"PostgreSQL connection failed: {str(e)}", 400
    except psycopg2.Error as e:
        return f"PostgreSQL general error: {str(e)}", 500
    except Exception as e:
        return f"PostgreSQL unexpected error: {str(e)}", 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def connect_mysql(connection_params):
    conn = None
    try:
        conn = mysql.connector.connect(**connection_params)
        return "MySQL connection successful!", 200
    except mysql.connector.Error as e:
        return f"MySQL connection failed: {str(e)}", 400
    except Exception as e:
        return f"MySQL unexpected error: {str(e)}", 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

# def connect_db2(connection_params):
#     conn = None
#     try:
#         conn_str = (
#             f"DATABASE={connection_params['database']};"
#             f"HOSTNAME={connection_params['hostname']};"
#             f"PORT={connection_params['port']};"
#             f"PROTOCOL=TCPIP;"
#             f"UID={connection_params['username']};"
#             f"PWD={connection_params['password']};"
#         )
#         conn = ibm_db.connect(conn_str, "", "")
#         return "DB2 connection successful!", 200
#     except Exception as e:
#         return f"DB2 connection failed: {str(e)}", 400
#     finally:
#         if conn is not None:
#             try:
#                 ibm_db.close(conn)
#             except Exception:
#                 pass

# def connect_oracle(connection_params):
#     conn = None
#     try:
#         dsn = cx_Oracle.makedsn(connection_params['hostname'], connection_params['port'], service_name=connection_params['database'])
#         conn = cx_Oracle.connect(user=connection_params['username'], password=connection_params['password'], dsn=dsn)
#         return "Oracle connection successful!", 200
#     except cx_Oracle.Error as e:
#         return f"Oracle connection failed: {str(e)}", 400
#     except Exception as e:
#         return f"Oracle unexpected error: {str(e)}", 500
#     finally:
#         if conn is not None:
#             try:
#                 conn.close()
#             except Exception:
#                 pass

def connect_mssql(connection_params):
    conn = None
    try:
        # Note: connection_params should include 'driver', 'server', etc., or adjust as needed
        conn = pyodbc.connect(**connection_params)
        return "MS SQL connection successful!", 200
    except pyodbc.Error as e:
        return f"MS SQL connection failed: {str(e)}", 400
    except Exception as e:
        return f"MS SQL unexpected error: {str(e)}", 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def connect_mongodb(connection_params):
    client = None
    try:
        username = connection_params.get('username', 'root')  # default to root
        password = connection_params['password']
        hostname = connection_params['hostname']
        port = connection_params.get('port', 27017)
        database = connection_params.get('database', 'admin')

        # Build the URI manually
        uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"

        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Forces connection
        return "MongoDB connection successful!", 200
    except KeyError as e:
        return f"Missing required connection parameter: {str(e)}", 400
    except pymongo.errors.ServerSelectionTimeoutError as e:
        return f"MongoDB connection timeout: {str(e)}", 408
    except pymongo.errors.ConnectionFailure as e:
        return f"MongoDB connection failed: {str(e)}", 400
    except pymongo.errors.PyMongoError as e:
        return f"MongoDB general error: {str(e)}", 500
    except Exception as e:
        return f"MongoDB unexpected error: {str(e)}", 500
    finally:
        if client is not None:
            client.close()


# Map of database types to functions
db_functions = {
    'postgresql': connect_postgresql,
    'mysql': connect_mysql,
    # 'db2': connect_db2,
    # 'oracle': connect_oracle,
    'mssql': connect_mssql,
    'mongodb': connect_mongodb
}

@app.route('/testdbcon', methods=['POST'])
def connect_db():
    data = request.get_json().get('connectionDetails')
    db_type = data.get('selectedDB')
    if db_type not in db_functions:
        return jsonify({'error': 'Unsupported or missing database type'}), 400

    connection_params = {
        'hostname': data.get('hostname'),
        'port': data.get('port'),
        'username': data.get('username'),
        'password': data.get('password'),
        'database': data.get('database')
    }
    
    # Check if all required connection parameters are provided
    if not all(connection_params.values()):
        return jsonify({'error': 'All connection parameters are required'}), 400
    
    try:
        message, status_code = db_functions[db_type](connection_params)
        return jsonify({'message': message}), status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)