# python
import sqlite3

conn = sqlite3.connect('trades.db')
cursor = conn.cursor()

# Drop the table if it exists, then create it fresh with a `position` column
cursor.execute('DROP TABLE IF EXISTS trades')

cursor.execute('''
    CREATE TABLE trades (
        ticker TEXT,
        shares REAL,
        actual_price REAL,
        valued_price REAL,
        currency TEXT,
        buy_datetime INTEGER,
        sell_datetime INTEGER
    )
''')

conn.commit()
conn.close()

print("Database 'trades.db' created and table 'trades' recreated with 'position'.")

