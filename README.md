<h1>Technical Assesment </h1>
<h2>Task 1</h2> 
1. Choosing database
<br>PostgreSQL chosen for the database because of its advanced features, scalability, reliability, open-source nature, and SQL compliance. PostgreSQL has a reputation for being highly scalable and able to handle large data sets. This is important for this project, as we are dealing with a data set of over 20 million credit card transactions</br>
</br>
2. ELT approach
<br>Decided to go with ELT approach instead of the traditional ETL approach in Task 1 because of the size of the data set. The credit card transaction data set contains over 20 million transactions, which makes the traditional ETL approach more difficult and time-consuming. The ELT approach allows us to load the raw data into the database first, and then transform the data as needed using SQL statements, which can be faster and more efficient.</br>
<br>Additionally, using the ELT approach allows for more flexibility in the transformation process, as we can use the full power of SQL to perform complex transformations on the data. This approach also allows for easier maintenance of the pipeline, as we can make changes to the transformation logic without having to modify the ETL code.</br>

<br> Based on the requirements, we need to store the credit card transaction data with the merchant name, transaction date, and transaction amount. Therefore, we will create a table named "credit_card_transactions" with the following columns:

```sql

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    merchant_id INTEGER,
    transaction_date DATE,
    amount NUMERIC(10, 2)
);

```
3.	Python script to execute the database initialization:

```python
# This program requires the python module psycopg2 to be installed.
# Install 'python3 -m pip install psycopg2' beforehand

import psycopg2

# create function to create database and establish connection with PostgreSQL
def create_database_and_tables():
    connection = psycopg2.connect(user='postgres', password='MzMxMi1hZmlmc3dh', host='localhost', port="5432", database='transactions')
    cursor = conn.cursor()
    
     SQL = """
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        merchant_id INTEGER,
        transaction_date DATE,
        amount NUMERIC(10, 2)
    );
    """
    cursor.execute(SQL)
    print("Table created")

    connection.commit()

    # close connection
    cursor.close()
    connection.close()

create_database_and_tables()
```

4.	Schema design
<br>
+-------------------------+</br>
|------transactions-------|</br>
+-------------------------+</br>
| id SERIAL PRIMARY KEY   |</br>
| merchant_name TEXT      |</br>
| transaction_date DATE   |</br>
| transaction_amount NUM  |</br>
+-------------------------+
</br>
<h2>Task 2</h2> 
1. Raw Data Ingestion Script

```python
import pandas as pd
from datetime import datetime

# Load the credit card transactions data
transactions_df = pd.read_csv('credit_card_transactions-ibm_v2.csv')

# Filter the transactions for data points from 2010 onwards
transactions_df = transactions_df[transactions_df['Year'] >= 2010]

# Define a function to group the transactions by merchant_id and frequency
def group_transactions(merchant_id, frequency):
    # Filter the transactions for the given merchant_id
    merchant_transactions_df = transactions_df[transactions_df['Merchant Name'] == merchant_id]

    # Convert the 'Year', 'Month', and 'Day' columns to a single datetime column
    merchant_transactions_df['Date'] = pd.to_datetime(merchant_transactions_df[['Year', 'Month', 'Day']])

    # Group the transactions by the specified frequency
    if frequency == 'yearly':
        group_by = pd.Grouper(key='Date', freq='Y')
        date_format = '%Y'
    elif frequency == 'monthly':
        group_by = pd.Grouper(key='Date', freq='M')
        date_format = '%Y-%m'
    else:
        raise ValueError('Invalid frequency. Only "monthly" and "yearly" are allowed.')

    grouped_transactions = merchant_transactions_df.groupby(group_by)

    # Aggregate the transaction count and total amount for each group
    total_transactions = {}
    total_amount = {}
    for name, group in grouped_transactions:
        date_str = name.strftime(date_format)
        total_transactions[date_str] = len(group)
        total_amount[date_str] = group['Amount'].sum()

    # Return the result as a dictionary
    result = {
        'merchant_id': merchant_id,
        'data': {
            'total_transactions': total_transactions,
            'total_amount': total_amount
        }
    }
    
    return result
    
    # Check for the last ingestion timestamp
    cur = conn.cursor()
    cur.execute("SELECT MAX(ingestion_timestamp) FROM transactions")
    last_ingestion_timestamp = cur.fetchone()[0]
    
    # Filter for new records
    if last_ingestion_timestamp is None:
        new_df = df
    else:
        last_timestamp = datetime.strptime(last_ingestion_timestamp, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
        new_df = df[df.timestamp > last_timestamp]
        
    # Ingest new records into the database
    if not new_df.empty:
        for i, row in new_df.iterrows():
            cur.execute("""
                INSERT INTO transactions
                (user_id, transaction_timestamp, amount, merchant_name, merchant_city, merchant_state, zip, mcc, errors, is_fraud, ingestion_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["User"],
                datetime.strptime(f"{row['Year']}-{row['Month']}-{row['Day']} {row['Time']}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
                row["Amount"],
                row["Merchant Name"],
                row["Merchant City"],
                row["Merchant State"],
                row["Zip"],
                row["MCC"],
                row["Errors?"],
                row["is Fraud?"],
                datetime.now(timezone.utc)
            ))

        conn.commit()
        print(f"{len(new_df)} new records ingested.")
    else:
        print("No new records to ingest.")
```

<h2>Task 3</h2> 
1. SQL Query Script

```sql
SELECT merchant_id,
       CASE 
           WHEN frequency = 'monthly' THEN to_char(transaction_date, 'YYYY-MM') 
           WHEN frequency = 'yearly' THEN to_char(transaction_date, 'YYYY') 
       END AS date_group,
       COUNT(*) AS total_transactions,
       SUM(amount) AS total_amount
FROM credit_card_transactions
WHERE merchant_name = 'merchant_name' AND transaction_date >= '2010-01-01'
GROUP BY merchant_id, date_group
ORDER BY date_group

