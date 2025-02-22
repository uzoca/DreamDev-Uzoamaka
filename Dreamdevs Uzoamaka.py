import mysql.connector  
import os  
import csv  

# mysql db conection
try:
    connection = mysql.connector.connect(
        user='uzoamaka',      
        password='1234',      
        host='127.0.0.1',    
        database='dreamdev' 
    )

    cursor = connection.cursor()
    print(" Connected")
except mysql.connector.Error as error:
    print(f" not connected to database: {error}")
    exit()

# transactions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        salesStaffId INT,
        transactionTime DATETIME,
        productsSold TEXT,
        saleAmount FLOAT
    )
""")
print(" Table ready")

# read test files from directory
directory = r"C:\Users\HP\Desktop\Dreamdevs\Dreamdevs Uzoamaka\Sample data"

# error checking
if not os.path.exists(directory):
    print(f" ERROR: Directory not found - {directory}")
    exit()

files = os.listdir(directory)
if not files:
    print(" ERROR: No transaction files found")
    exit()

print(f" Found {len(files)} transaction files")

# processing only text files
rows_inserted = 0

for file in files:
    file_path = os.path.join(directory, file)

    
    if not file_path.endswith('.txt'):
        continue

    print(f" Processing file: {file}")

    # Open the file and read each line
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            columns = line.strip().split(',')

            # Each line should have exactly 4 columns for each variable within the transaction
            if len(columns) != 4:
                print(f" Skipping invalid row in {file}: {line.strip()}")
                continue

            # Extract data from the line
            sales_staff_id = int(columns[0])   
            transaction_time = columns[1]      
            products_sold = columns[2]         
            sale_amount = float(columns[3])    

            # Insert into MySQL
            cursor.execute("""
                INSERT INTO transactions (salesStaffId, transactionTime, productsSold, saleAmount)
                VALUES (%s, %s, %s, %s)
            """, (sales_staff_id, transaction_time, products_sold, sale_amount))

            rows_inserted += 1

# Commit the changes to the database
connection.commit()
print(f" Inserted {rows_inserted} rows into the database")

# Data analytics
print("\nData Analysis")

#The highest number of sales volume in a day
cursor.execute("""
    SELECT DATE(transactionTime), COUNT(*) AS total_sales
    FROM transactions
    GROUP BY DATE(transactionTime)
    ORDER BY total_sales DESC
    LIMIT 1
""")
highest_sales_volume = cursor.fetchone()
print(f"Highest Sales Volume in a Day: {highest_sales_volume[0].strftime('%Y-%m-%d')}, {highest_sales_volume[1]}")
# The highest sales value in a day
cursor.execute("""
    SELECT DATE(transactionTime), SUM(saleAmount) AS total_value
    FROM transactions
    GROUP BY DATE(transactionTime)
    ORDER BY total_value DESC
    LIMIT 1
""")
highest_sales_value = cursor.fetchone()
print(f"Highest Sales Value in a Day: {highest_sales_value[0].strftime('%Y-%m-%d')}, {highest_sales_value[1]}")
# The most sold product ID by volume
cursor.execute("SELECT productsSold FROM transactions")
product_counts = {}

for (products_sold,) in cursor.fetchall():
    items = products_sold.strip("[]").split("|") 
    for item in items:
        if ":" in item:
            product_id, quantity = item.split(":")
            product_counts[product_id] = product_counts.get(product_id, 0) + int(quantity)

most_sold_product = max(product_counts, key=product_counts.get) if product_counts else None
print(f" Most Sold Product ID: {most_sold_product}")

# Highest sales staff ID for each month
cursor.execute("""
    SELECT month, salesStaffId, total_sales FROM (
        SELECT DATE_FORMAT(transactionTime, '%Y-%m') AS month, salesStaffId, SUM(saleAmount) AS total_sales,
               ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(transactionTime, '%Y-%m') ORDER BY SUM(saleAmount) DESC) AS rn
        FROM transactions
        GROUP BY month, salesStaffId
    ) AS ranked_sales
    WHERE rn = 1
    ORDER BY month ASC
""")
highest_selling_staff = cursor.fetchall()
print("Highest Sales Staff ID per Month:")
for row in highest_selling_staff:
    print(row)

# Highest hour of the day by average transaction volume
cursor.execute("""
    SELECT HOUR(transactionTime) AS hour, COUNT(*) AS transactions
    FROM transactions
    GROUP BY hour
    ORDER BY transactions DESC
    LIMIT 1
""")
highest_sales_hour = cursor.fetchone()
print(f" Highest Hour of the Day by Avg Transaction Volume: {highest_sales_hour}")

# end database connection
connection.close()

