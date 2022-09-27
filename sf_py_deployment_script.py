import snowflake.connector
import os
import argparse

PASSWORD = os.getenv('SNOWSQL_PWD')
ACCOUNT = os.getenv('SNOWSQL_ACCOUNT')
commands =[]

def create_connection():
    #Create Snowflake connection
    conn = snowflake.connector.connect(
        user='aryan',
        password=PASSWORD,
        account=ACCOUNT,
        warehouse='DEMO_WH',
        database='DEMO_DB',
        session_parameters={
            'QUERY_TAG': 'snowflake_python_connector_cicd_demo'
            }
        )
    return conn

def close_connection(sf_connection):
    sf_connection.close()


def read_script_file(filename):
    # Reading script files for implementation
    with open('Script/' + filename) as f:
        text = f.read().replace('\n', ' ')
        print(text)
        # Split one line text into multiple list elements with ; as delimeter
        commands = text.split(';')
        # Remove last empty element from list
        commands = list(filter(None, commands))
        print(commands)
        return commands

def run_scripts(sf_connection,commands):
    crsr = sf_connection.cursor()
    try:
        for command in commands:
            crsr.execute(command)
        crsr.execute("SELECT * FROM HELLO_WORLD")
        for row in crsr:
            print(row)
    finally:
        crsr.close()

# def initiate_logging(sf_connection):
#     log_crsr = sf_connection.cursor()
#     try:
#         log_crsr.execute("CREATE SCHEMA IF NOT EXISTS logging_schema")
#         log_crsr.execute("CREATE TABLE IF NOT EXISTS deployment_history(version string, description string, script_name string, checksum string, status string, user string, implement_time timestamp)")
#
#         log_crsr.execute("INSERT INTO TABLEdeployment_history('v1','test_deployment')")
#         for row in log_crsr:
#             print(row)
#     finally:
#         log_crsr.close()



if __name__ == '__main__':

    # create parser to adapt the arguments provided to script
    parser = argparse.ArgumentParser(description="script file with all the changes")

    # Parse argument script name under Script folder to run for this release
    parser.add_argument("script_name",
                        help="Name of the script containing commands"
                        )
    # Parse the complete list of argument
    args = parser.parse_args()
    print(args)

    # Create snowflake connections
    sf_con = create_connection()

    # initiate_logging(sf_con)

    # Read argument file and return command list to execute
    command_lst = read_script_file(filename=args.script_name)

    # Run commands in snowflake
    run_scripts(sf_con,command_lst)

    # Close snowflake Connection
    close_connection(sf_con)