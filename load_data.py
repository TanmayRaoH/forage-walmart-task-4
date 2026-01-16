import sqlite3
import csv
from collections import defaultdict

DB_PATH = "shipment_database.db"



def insert_row(cursor, row):
    cursor.execute(
        """
        INSERT INTO shipment
        (shipping_identifier, origin, destination, product, quantity)
        VALUES (?, ?, ?, ?, ?)
        """,
        row
    )



def load_spreadsheet_0(cursor):
    """
    shipping_data_0.csv is self-contained.
    Columns:
    origin_warehouse, destination_store, product,
    on_time, product_quantity, driver_identifier
    """
    with open("data/shipping_data_0.csv", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            insert_row(cursor, (
                r["driver_identifier"],      # shipping_identifier
                r["origin_warehouse"],       # origin
                r["destination_store"],      # destination
                r["product"],                # product
                int(r["product_quantity"])   # quantity
            ))


def load_spreadsheets_1_and_2(cursor):
    """
    shipping_data_1.csv: product per row, grouped by shipment_identifier
    shipping_data_2.csv: origin & destination per shipment_identifier
    """

    # Load shipment locations
    shipment_locations = {}
    with open("data/shipping_data_2.csv", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            shipment_locations[r["shipment_identifier"]] = (
                r["origin"],
                r["destination"]
            )

    # Count products per shipment
    product_counts = defaultdict(lambda: defaultdict(int))
    with open("data/shipping_data_1.csv", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sid = r["shipment_identifier"]
            product = r["product"]
            product_counts[sid][product] += 1

    # Insert combined data
    for sid, products in product_counts.items():
        origin, destination = shipment_locations[sid]
        for product, quantity in products.items():
            insert_row(cursor, (
                sid,
                origin,
                destination,
                product,
                quantity
            ))


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    load_spreadsheet_0(cursor)
    load_spreadsheets_1_and_2(cursor)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
