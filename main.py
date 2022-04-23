
import dotenv
import os

from db import DB


if __name__ == '__main__':
    dotenv.load_dotenv()
    operational_db = DB("OPERATIONAL_DB")
    analytical_db = DB("ANALYTICAL_DB")

    # 1. connect to the analytical DB and read the configuration table
    configuration = analytical_db.read_configuration()

    # 2. for each configuration row, connect to the source DB and read the data table
    for etl_table in configuration:
        print(etl_table)
        source_db = DB(etl_table["source_db"])
        source_table = source_db.read_table(etl_table["source_table_name"], etl_table["source_columns"],
                                            etl_table["last_unique_id"],)

        # 3. apply the transformation if needed
        ...
        # 4. store the results in the anlytical database (based on the target table attribute)
        for row in source_table:
            last_id = row["id"]
            analytical_db.store_results(etl_table["target_table"], etl_table["source_columns"], row)

        # 5. update the last_unique_id in the configuration table
        try:
            if last_id > 1:
                analytical_db.update_configuration(etl_table["source_db"], etl_table["source_table_name"], last_id)
        except Exception as e:
            print(f"Error updating the configuration table. {e}")

