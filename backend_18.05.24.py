import mysql.connector

# Database connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="prasad@1",
    database="safertek_backend"
)
cursor = conn.cursor()

# Create tables (only if they do not exist)
cursor.execute('''
CREATE TABLE IF NOT EXISTS Customers (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Email VARCHAR(255),
    DateOfBirth DATE
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    Price DECIMAL(10, 2)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS OrderItems (
    OrderItemID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
)
''')

# Insert data
try:
    cursor.executemany('''
    INSERT INTO Customers (CustomerID, FirstName, LastName, Email, DateOfBirth)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE FirstName=VALUES(FirstName), LastName=VALUES(LastName), Email=VALUES(Email), DateOfBirth=VALUES(DateOfBirth)
    ''', [
        (1, 'John', 'Doe', 'john.doe@example.com', '1985-01-15'),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', '1990-06-20')
    ])

    cursor.executemany('''
    INSERT INTO Products (ProductID, ProductName, Price)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE ProductName=VALUES(ProductName), Price=VALUES(Price)
    ''', [
        (1, 'Laptop', 1000),
        (2, 'Smartphone', 600),
        (3, 'Headphones', 100)
    ])

    cursor.executemany('''
    INSERT INTO Orders (OrderID, CustomerID, OrderDate)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE CustomerID=VALUES(CustomerID), OrderDate=VALUES(OrderDate)
    ''', [
        (1, 1, '2023-01-10'),
        (2, 2, '2023-01-12')
    ])

    cursor.executemany('''
    INSERT INTO OrderItems (OrderItemID, OrderID, ProductID, Quantity)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE OrderID=VALUES(OrderID), ProductID=VALUES(ProductID), Quantity=VALUES(Quantity)
    ''', [
        (1, 1, 1, 1),
        (2, 1, 3, 2),
        (3, 2, 2, 1),
        (4, 2, 3, 1)
    ])

    # Commit the transaction
    conn.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")
    conn.rollback()

# Query 1: List all customers
print("All Customers:")
cursor.execute('SELECT * FROM Customers')
for row in cursor.fetchall():
    print(row)

# Query 2: Find all orders placed in January 2023
print("\nOrders placed in January 2023:")
cursor.execute('SELECT * FROM Orders WHERE OrderDate BETWEEN "2023-01-01" AND "2023-01-31"')
for row in cursor.fetchall():
    print(row)

# Query 3: Get the details of each order, including the customer name and email
print("\nOrder details with customer name and email:")
cursor.execute('''
SELECT Orders.OrderID, Customers.FirstName, Customers.LastName, Customers.Email, Orders.OrderDate
FROM Orders
JOIN Customers ON Orders.CustomerID = Customers.CustomerID
''')
for row in cursor.fetchall():
    print(row)

# Query 4: List the products purchased in a specific order (e.g., OrderID = 1)
print("\nProducts in OrderID 1:")
cursor.execute('''
SELECT Products.ProductName, OrderItems.Quantity
FROM OrderItems
JOIN Products ON OrderItems.ProductID = Products.ProductID
WHERE OrderItems.OrderID = 1
''')
for row in cursor.fetchall():
    print(row)

# Query 5: Calculate the total amount spent by each customer
print("\nTotal amount spent by each customer:")
cursor.execute('''
SELECT Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
FROM Orders
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
JOIN Customers ON Orders.CustomerID = Customers.CustomerID
GROUP BY Customers.CustomerID
''')
for row in cursor.fetchall():
    print(row)

# Query 6: Find the most popular product (the one that has been ordered the most)
print("\nMost popular product:")
cursor.execute('''
SELECT Products.ProductName, SUM(OrderItems.Quantity) AS TotalOrdered
FROM OrderItems
JOIN Products ON OrderItems.ProductID = Products.ProductID
GROUP BY Products.ProductID
ORDER BY TotalOrdered DESC
LIMIT 1
''')
for row in cursor.fetchall():
    print(row)

# Query 7: Get the total number of orders and the total sales amount for each month in 2023
print("\nTotal number of orders and sales amount for each month in 2023:")
cursor.execute('''
SELECT DATE_FORMAT(Orders.OrderDate, "%Y-%m") AS Month, COUNT(Orders.OrderID) AS TotalOrders, SUM(Products.Price * OrderItems.Quantity) AS TotalSales
FROM Orders
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
WHERE YEAR(Orders.OrderDate) = 2023
GROUP BY Month
''')
for row in cursor.fetchall():
    print(row)

# Query 8: Find customers who have spent more than $1000
print("\nCustomers who have spent more than $1000:")
cursor.execute('''
SELECT Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
FROM Orders
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
JOIN Customers ON Orders.CustomerID = Customers.CustomerID
GROUP BY Customers.CustomerID
HAVING TotalSpent > 1000
''')
for row in cursor.fetchall():
    print(row)

# Close the connection
conn.close()
