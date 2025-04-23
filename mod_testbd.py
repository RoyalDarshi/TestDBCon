from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import mysql.connector
import ibm_db
import cx_Oracle
import pyodbc
import pymongo  # Added for MongoDB support

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
        # Perform operations
        return "PostgreSQL connection successful!"
    except psycopg2.OperationalError as e:
        return jsonify({'postgresql connection failed:': str(e)}), 400
    except psycopg2.Error as e:
        return jsonify({'postgresql general error:': str(e)}), 500
    except Exception as e:
        return jsonify({'postgresql unexpected error:': str(e)}), 500
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
        # Perform operations
        return "MySQL connection successful!"
    except psycopg2.OperationalError as e:
        return jsonify({'MySQL connection failed:': str(e)}), 400
    except psycopg2.Error as e:
        return jsonify({'MySQL general error:': str(e)}), 500
    except Exception as e:
        return jsonify({'MySQL unexpected error:': str(e)}), 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def connect_db2(connection_params):
    conn = None
    try:
        conn_str = (
            f"DATABASE={connection_params['database']};"
            f"HOSTNAME={connection_params['hostname']};"
            f"PORT={connection_params['port']};"
            f"PROTOCOL=TCPIP;"
            f"UID={connection_params['username']};"
            f"PWD={connection_params['password']};"
        )
        conn = ibm_db.connect(conn_str, "", "")
        # Perform operations
        return "DB2 connection successful!"
    except psycopg2.OperationalError as e:
        return jsonify({'DB2 connection failed:': str(e)}), 400
    except psycopg2.Error as e:
        return jsonify({'DB2 general error:': str(e)}), 500
    except Exception as e:
        return jsonify({'DB2 unexpected error:': str(e)}), 500
    finally:
        if conn is not None:
            try:
                ibm_db.close(conn)
            except Exception:
                pass

def connect_oracle(connection_params):
    conn = None
    try:
        dsn = cx_Oracle.makedsn(connection_params['hostname'], connection_params['port'], service_name=connection_params['database'])
        conn = cx_Oracle.connect(user=connection_params['username'], password=connection_params['password'], dsn=dsn)
        # Perform operations
        return "Oracle connection successful!"
    except psycopg2.OperationalError as e:
        return jsonify({'oracle connection failed:': str(e)}), 400
    except psycopg2.Error as e:
        return jsonify({'oracle general error:': str(e)}), 500
    except Exception as e:
        return jsonify({'oracle unexpected error:': str(e)}), 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def connect_mssql(connection_params):
    conn = None
    try:
        conn = pyodbc.connect(**connection_params)
        # Perform operations
        return "MS SQL connection successful!"
    except psycopg2.OperationalError as e:
        return jsonify({'mssql connection failed:': str(e)}), 400
    except psycopg2.Error as e:
        return jsonify({'mssql general error:': str(e)}), 500
    except Exception as e:
        return jsonify({'mssql unexpected error:': str(e)}), 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def connect_mongodb(connection_params):
    client = None
    try:
        client = pymongo.MongoClient(
            host=connection_params['hostname'],
            port=int(connection_params['port']),
            username=connection_params['username'],
            password=connection_params['password'],
            authSource=connection_params['database'],
            serverSelectionTimeoutMS=5000  # 5-second timeout to avoid hanging
        )
        client.server_info()  # Forces the connection to be established
        return "MongoDB connection successful!"
    except pymongo.errors.ConnectionFailure as e:
        return jsonify({'MongoDB connection failed:': str(e)}), 400
    except pymongo.errors.PyMongoError as e:
        return jsonify({'MongoDB general error:': str(e)}), 500
    except ValueError as e:
        return jsonify({'Invalid port number:': str(e)}), 400
    except Exception as e:
        return jsonify({'MongoDB unexpected error:': str(e)}), 500
    finally:
        if client is not None:
            client.close()

# Map of database types to functions
db_functions = {
    'postgresql': connect_postgresql,
    'mysql': connect_mysql,
    'db2': connect_db2,
    'oracle': connect_oracle,
    'mssql': connect_mssql,
    'mongodb': connect_mongodb  # Added MongoDB support
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
        result = db_functions[db_type](connection_params)
        return jsonify({'message': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)