import happybase

def printHBaseTable(connection, tablename):
    table = connection.table(tablename)
    for key, data in table.scan():
        print(f"Row key: {key}")
        for column, value in data.items():
            print(f"    Column: {column} => Value: {value}")
def delete_hbase_table(connection, tablename):
    connection.delete_table(tablename, disable=True)
    print(f"Left tables: {connection.tables()}")

# Connect to HBase
connection = happybase.Connection(host='10.4.41.52', port=9090)
connection.open()

# delete_hbase_table(connection, 'price')
print(connection.tables())

printHBaseTable(connection, 'price')

connection.close()