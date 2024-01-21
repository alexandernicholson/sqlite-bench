# SQLite Bench

The goal of this script is to benchmark SQLite performance by getting the schema for each table in benchdb.db and then performing the following tests:

- **WRITE**: Inserting 1,000,000 (1 million) rows into each table
- **READ**: Selecting all rows from each table
- **READ**: Selecting a random row from each table (using the random() function)

We want to output time taken for each test, and the total time taken for all tests.
