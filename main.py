import shutil
from sql_utilities import *
from datetime import datetime as dt
import os

mt = MaintainTime()
setup()


def postman_main():
    """
    Main driver of the program
    :return:
    """
    sql = SQL(
        host_name     = "localhost",
        database_name = "master",
        username      = "sa",
        password      = "Igobacca1@",
    )
    # # Start the timer
    mt.start()
    print(f"Connection string using to connect to pyodbc : {sql.connection_string}")

    # # Check if able to connect to the SQl server
    sql.check_connection(skip_table_creation=True)

    # # Get the path of the data folder, where we are expecting CSV files to be present.
    data_path = os.path.join(ROOT_DIR, 'data')
    for (root, dirs, files) in os.walk(data_path, topdown=True):
        for file in files:
            # Read data from the CSV
            file_path = os.path.join(root, file)
            data = process_data(pd.read_csv(file_path))

            # # Get data already present in the table
            table_data = sql.get_from_sql(table_name="data", create_table_if_not_present=True)
            new_data = pd.concat([table_data, data]).drop_duplicates(['sku'], keep='last')

            # # Store the Data to SQL server
            sql.save_to_sql(new_data, table_name="data")

            # # Stop the timer
            mt.stop(operation=f"Loading data into SQL, after retrieving it from CSV : {file}")

            # # move the processed file to the archival folder
            shutil.move(file_path, os.path.join(ROOT_DIR, 'processed_data',
                                                dt.utcnow().strftime("%Y_%m_%d_%H_%M_") + file))


if __name__ == '__main__':
    postman_main()

