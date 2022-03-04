"""Demonstrates how to connect to Cloud Bigtable and run some basic operations.

Prerequisites:

- Create a Cloud Bigtable cluster.
  https://cloud.google.com/bigtable/docs/creating-cluster
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

import argparse

import datetime
from random import randint
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
import time 
from data import data 

def main(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    print("Creating the {} table.".format(table_id))
    table = instance.table(table_id)

    print("Creating column family cf1 with Max Version GC rule...")
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = "cf1"
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))

    print("Writing data to the 'Account' table.")
    greetings = data 
    rows = []
    column = "balance".encode()
    for i, value in enumerate(greetings):
        row_key = f"balance{i}{randint(0, 10000)}".encode()
        row = table.direct_row(row_key)
        row.set_cell(
            column_family_id, column, value, timestamp=datetime.datetime.utcnow()
        )
        rows.append(row)
    table.mutate_rows(rows)

    row_filter = row_filters.CellsColumnLimitFilter(1)

    print("Getting a an account by row key.")
    key = "balance0".encode()

    row = table.read_row(key, row_filter)
    cell = row.cells[column_family_id][column][0]

    print("Scanning for all accounts:")
    partial_rows = table.read_rows(filter_=row_filter)

    # for row in partial_rows:
    #     cell = row.cells[column_family_id][column][0]
    #     print(cell.value.decode("utf-8"))

    # print("Deleting the {} table.".format(table_id))
    # table.delete()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("project_id", help="Your Cloud Platform project ID.")
    parser.add_argument(
        "instance_id", help="ID of the Cloud Bigtable instance to connect to."
    )
    parser.add_argument(
        "--table", help="Table to create and destroy.", default="Account"
    )

    args = parser.parse_args()
    for i in range(100):
        main(args.project_id, args.instance_id, args.table)
        time.sleep(10)