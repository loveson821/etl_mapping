#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#from curses import set_tabsize
#from pdb import set_trace

import sentry_sdk
import dotenv
import os
from datetime import datetime
from db import DB


if __name__ == '__main__':
    fetch_time = str(datetime.now())  # current sync time

    dotenv.load_dotenv()
    sentry_sdk.init(os.getenv("SENTRY_URL"), traces_sample_rate=1.0)

    operational_db = DB("OPERATIONAL_DB")
    analytical_db = DB("ANALYTICAL_DB")
    
    # 1. connect to the analytical DB and read the configuration table
    configuration = analytical_db.read_configuration()

    # 2. for each configuration row, connect to the source DB and read the data table
    for etl_table in configuration:
        source_db = DB(etl_table["source_db"])

        source_table = source_db.read_table(etl_table["source_table_name"], etl_table["source_columns"],
                                                etl_table["last_id"], etl_table["last_fetch"])

        # 3. apply the transformation if needed
        # ...
        # 4. store the results in the anlytical database (based on the target table attribute)

        # create table if not exists
        analytical_db.create_table(etl_table["source_table_name"])

        last_id = 0

        for row in source_table:
            last_id = row["id"]
            analytical_db.store_results(
                etl_table["target_table"], etl_table["source_columns"], row)

        # set_trace()
        analytical_db.session.commit()
        print("%s migrate commit, last id %s, fetch time %s" %
              (etl_table["source_table_name"], last_id, fetch_time))

        # # 5. update the last_id in the configuration table
        analytical_db.update_configuration(etl_table["source_db"], etl_table["source_table_name"], last_id, fetch_time)

