import csv
import sys
import psycopg2 as pg

# SETUP
if len(sys.argv) < 2:
    raise ValueError("must pass a csv file to process")

# DATA
transactions_file = sys.argv[1]

categories = {
    "1": "fixed",
    "2": "want",
    "3": "other"
}

table_name = "expenses"

# FUNCTIONS
def table_exists(cursor, tablename):
    cursor.execute("""select exists(select * from information_schema.tables where table_name = %s);""", [tablename]);
    return cursor.fetchone()[0] 


def get_category():
    bucket = input('1 = fixed | 2 = want | 3 = other\n')

    if bucket in categories:
        return categories[bucket]
    else:
        get_category()

# PROGRAM
try:
    db_conn = pg.connect("dbname=branweb user=branweb")

    db_cursor = db_conn.cursor()

    if not table_exists(db_cursor, table_name):
        print("creating table")
        db_cursor.execute(
            f"create table {table_name} (id serial, date date, description text, category text, amount numeric, PRIMARY KEY(date,description,amount));"
        )
    else:
        print("table exists")


    with open(transactions_file) as oldfile:
        reader = csv.reader(oldfile, delimiter=',')
        next(reader, None) # skip header
        for row in reader:
            print('\t'.join(row))
            category = get_category()
            try:
                db_cursor.execute(
                    f"insert into {table_name} (date, description, category, amount) values (%s, %s, %s, %s);",
                    [row[0], row[2], category, row[5]]
                )
            except pg.errors.UniqueViolation as error:
                db_conn.rollback()
                print("\n===")
                print(f"{error}".strip())
                print("Ignoring transaction and moving on to next one\n===\n")
                pass

    db_conn.commit()
finally:
    db_cursor.close()
    db_conn.close()

