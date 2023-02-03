import sqlite3
import os


def merge():
    # find the database files
    files = [f for f in os.listdir('.') if f.endswith('.db')]

    # connect to the new database
    conn = sqlite3.connect('merged.db')
    c = conn.cursor()

    # iterate over the remaining databases
    for f in files:
        # connect to the database
        conn2 = sqlite3.connect(f)
        c2 = conn2.cursor()

        # get the tables
        tables = c2.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

        # iterate over the tables
        for table in tables:

            # get the table name
            table = table[0]

            # get the column names
            columns = c2.execute("PRAGMA table_info(%s)" % table).fetchall()

            # create the table in the new database
            c.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (table, ', '.join(['%s %s' % (col[1], col[2]) for col in columns])))
            conn.commit()
            
            # create the insert string
            insert = "INSERT INTO %s VALUES (%s)" % (table, ','.join(['?'] * len(columns)))

            # get the data
            data = c2.execute("SELECT * FROM %s" % table).fetchall()

            # insert the data into the first database
            c.executemany(insert, data)

        # close the database
        conn2.close()

    # commit the changes
    conn.commit()
    
    # close the database
    conn.close()

if __name__ == '__main__':
    merge()
