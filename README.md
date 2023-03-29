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
import psycopg2

def create_database_and_tables():
    conn = psycopg2.connect(database="your_database", user="your_user", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        merchant_id INTEGER,
        transaction_date DATE,
        amount NUMERIC(10, 2)
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

create_database_and_tables()
```
