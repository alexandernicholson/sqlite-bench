# The goal of this script is to benchmark SQLite performance by getting the schema for each table in benchdb.db and then performing the following tests:
# - WRITE: Inserting 1,000,000 (1 million) rows into each table
# - READ: Selecting all rows from each table
# - READ: Selecting a random row from each table (using the random() function)
# We want to output time taken for each test, and the total time taken for all tests.

import sqlite3
import time
import random
import threading
import sys

# Get the schema for each table in benchdb.db
conn = sqlite3.connect('benchdb.db')
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()
schemas = {}
global_row_setting = 100000

for table in tables:
    c.execute("PRAGMA table_info({})".format(table[0]))
    schemas[table[0]] = c.fetchall()
    print("Schema for table {}: {}".format(table[0], schemas[table[0]]))

# Clear the database
print("Clearing database...")
for table in schemas:
    if table != 'schema_migrations':
        if 'ids' not in table:
            c.execute("DELETE FROM {}".format(table))
            conn.commit()
print("Done clearing database!")

# Vacuum the database
print("Vacuuming database...")
c.execute("VACUUM")
conn.commit()
print("Done vacuuming database!")

print("Checkpointing WAL to zero bytes...")
c.execute("PRAGMA wal_checkpoint(truncate);")
print("Done checkpointing WAL to zero bytes!")

# I want to run this benchmark with performance optimizations and also without performance optimizations, lets add a command line argument to toggle this
# If the user passes in the command line argument "performance", then we will run the benchmark with performance optimizations

if len(sys.argv) > 1 and sys.argv[1] == "performance":
        print("Running benchmark with performance optimizations...")
        c.execute("PRAGMA journal_mode = WAL")
        c.execute("PRAGMA synchronous = normal")
        c.execute("PRAGMA temp_store = memory")
        c.execute("PRAGMA mmap_size = 30000000000")
else:
    print("Running benchmark without performance optimizations...")

# Write 1,000,000 rows to each table
def write():
    # Track how many tables have been written to out of the total number of tables
    tables_written = 0
    for table in schemas:
        # Exclude inserting into schema_migrations table or containing 'ids' in the table name
        if table != 'schema_migrations':
            if 'ids' not in table:
                # Time how long it takes to insert 1,000,000 rows into each table
                table_write_time = time.time()
                for j in range(1):
                    # Insert [global_row_setting] rows at a time
                    for i in range(global_row_setting):
                        try:
                            # Insert into the table, respecting the schema and constraints
                            try:
                                c.execute("INSERT INTO {} VALUES ({})".format(table, ",".join(["?" for i in range(len(schemas[table]))])), [random.randint(0, 100000000000) for i in range(len(schemas[table]))])
                            except sqlite3.IntegrityError as e:
                                print("Unique constraint violation:", e)
                        except Exception as e:
                            print("Unexpected error:", e)
                    conn.commit()
                finished_table_write_time = time.time() - table_write_time
                tables_written += 1
                print("Wrote to {} tables out of {}".format(tables_written, len(schemas) - 1))
                print("Wrote {} rows to table {} in {} seconds".format(global_row_setting, table, finished_table_write_time))
                if len(sys.argv) > 1 and sys.argv[1] == "performance":
                    print("Vacuuming database...")
                    vacuum_start = time.time()
                    c.execute("PRAGMA vacuum;")
                    vacuum_time = time.time() - vacuum_start
                    print("Done vacuuming database, took {} seconds".format(vacuum_time))
                    print("Optimizing database...")
                    optimization_start = time.time()
                    c.execute("PRAGMA optimize;")
                    optimization_time = time.time() - optimization_start
                    print("Done optimizing database, took {} seconds".format(optimization_time))
                    print("Checkpointing WAL to zero bytes...")
                    wal_checkpoint_start = time.time()
                    c.execute("PRAGMA wal_checkpoint(truncate);")
                    wal_checkpoint_time = time.time() - wal_checkpoint_start
                    print("Done checkpointing WAL to zero bytes, took {} seconds".format(wal_checkpoint_time))
                else:
                    print("No performance optimisations!")

# Read all rows from each table
def read():
    for table in schemas:
        c.execute("SELECT * FROM {}".format(table))
        c.fetchall()

# Read a random row from each table, [global_row_setting] times
def read_random():
    for table in schemas:
        for i in range(global_row_setting):
            c.execute("SELECT * FROM {} WHERE rowid = {}".format(table, random.randint(0, global_row_setting)))
            c.fetchall()


# Run the tests
print("Running tests...")
print("Writing to database...")
total_time_start = time.time()
start = time.time()
write()
write_time = time.time() - start
print("Done writing to database!")
print("Reading from database...")
start = time.time()
read()
read_time = time.time() - start
print("Done reading from database!")
print("Reading random rows from database...")
start = time.time()
read_random()
read_random_time = time.time() - start
print("Done reading random rows from database!")
total_time = time.time() - total_time_start

print("Write time: {} seconds".format(write_time))
print("Read time: {} seconds".format(read_time))
print("Read random time: {} seconds".format(read_random_time))
print("Total time: {} seconds".format(total_time))
