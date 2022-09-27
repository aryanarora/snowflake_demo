import snowflake.connector
import os

PASSWORD = os.getenv('SNOWSQL_PWD')
ACCOUNT = os.getenv('SNOWSQL_ACCOUNT')

conn = snowflake.connector.connect(
    user='aryan',
    password=PASSWORD,
    account=ACCOUNT,
    session_parameters={
        'QUERY_TAG': 'sonwflake_python_connector_demo'
        }
    )

crsr = conn.cursor()
try:
    crsr.execute("USE WAREHOUSE DEMO_WH")
    crsr.execute("USE DATABASE DEMO_DB")
    crsr.execute("USE SCHEMA DEMO")
    crsr.execute("ALTER TABLE HELLO_WORLD ADD id int")
    crsr.execute("INSERT INTO HELLO_WORLD VALUES('aryan','arora',99,1)")
    crsr.execute("INSERT INTO HELLO_WORLD VALUES('amit','shah',98,2)")
    crsr.execute("SELECT * FROM HELLO_WORLD")
    for row in crsr:
        print(row)
finally:
    crsr.close()
conn.close()