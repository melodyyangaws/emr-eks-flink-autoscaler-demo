# MySQL Database Setup Guide for Flink CDC

## Overview

This guide walks you through creating and configuring a MySQL database as the source for Flink CDC to Paimon pipeline.

## Architecture Context

```
┌────────────────────────────────────┐
│  Step 1: Create RDS MySQL Instance │  ← You are here
│  - Enable binlog                   │
│  - Configure security groups       │
│  - Set up networking               │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  Step 2: Configure MySQL for CDC   │
│  - Create CDC user                 │
│  - Grant permissions               │
│  - Verify binlog settings          │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  Step 3: Create Database Schema    │
│  - Create database                 │
│  - Create tables                   │
│  - Add indexes                     │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  Step 4: Load Initial Data         │
│  - Insert sample data              │
│  - Verify data                     │
└────────────────────────────────────┘
```

---

## Step 1: Create RDS MySQL Instance

### Option A: Using AWS Console (Easiest)

1. **Navigate to RDS Console**
   - Go to: https://console.aws.amazon.com/rds/
   - Click "Create database"

2. **Choose Database Creation Method**
   - Select: **Standard create**

3. **Engine Options**
   - Engine type: **MySQL**
   - Version: **MySQL 8.0.35** (or latest 8.0.x)
   - Edition: **MySQL Community**

4. **Templates**
   - Select: **Dev/Test** (for POC) or **Production** (for prod)

5. **Settings**
   ```
   DB instance identifier: flink-cdc-mysql
   Master username: admin
   Master password: YourSecurePassword123!
   Confirm password: YourSecurePassword123!
   ```

6. **DB Instance Class**
   - **Dev/POC**: db.t3.medium (2 vCPU, 4 GB RAM)
   - **Production**: db.r6g.xlarge (4 vCPU, 32 GB RAM)

7. **Storage**
   ```
   Storage type: General Purpose SSD (gp3)
   Allocated storage: 100 GB
   ✅ Enable storage autoscaling
   Maximum storage threshold: 1000 GB
   ```

8. **Connectivity**
   ```
   Virtual private cloud (VPC): [Select your EKS VPC]
   Subnet group: [Auto-created or select existing]
   Public access: No (for production)
   VPC security group: Create new
   Security group name: rds-mysql-cdc-sg
   Availability Zone: No preference
   ```

9. **Database Authentication**
   - Select: **Password authentication**

10. **Monitoring**
    ```
    ✅ Enable Enhanced monitoring
    Monitoring Role: Create new role
    Granularity: 60 seconds
    ```

11. **Additional Configuration**
    ```
    Initial database name: ecommerce
    DB parameter group: default.mysql8.0
    Option group: default:mysql-8-0

    ✅ Enable automated backups
    Backup retention period: 7 days
    Backup window: 03:00-04:00 UTC

    ✅ Enable CloudWatch Logs exports
    Select: Error log, General log, Slow query log

    Maintenance window: Mon:04:00-Mon:05:00 UTC
    ✅ Enable auto minor version upgrade
    ```

12. **Click "Create database"**
    - Wait 5-10 minutes for creation
    - Status will change from "Creating" → "Available"

### Option B: Using AWS CLI (Faster for Automation)

```bash
# 1. Set environment variables
export AWS_REGION=us-west-2
export VPC_ID=vpc-xxxxxxxxx  # Your EKS VPC ID
export SUBNET_1=subnet-xxxxx  # Private subnet 1
export SUBNET_2=subnet-yyyyy  # Private subnet 2
export DB_NAME=ecommerce
export DB_USERNAME=admin
export DB_PASSWORD='YourSecurePassword123!'
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 2. Create security group for RDS
SG_ID=$(aws ec2 create-security-group \
    --group-name rds-mysql-cdc-sg \
    --description "Security group for MySQL CDC from EKS" \
    --vpc-id $VPC_ID \
    --region $AWS_REGION \
    --query 'GroupId' \
    --output text)

echo "Security Group ID: $SG_ID"

# 3. Allow MySQL access from EKS pods (adjust the CIDR to your EKS Cidr range )
aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol tcp \
    --port 3306 \
    --cidr 192.168.0.0/16 \
    --region $AWS_REGION

# 4. Create DB subnet group
aws rds create-db-subnet-group \
    --db-subnet-group-name rds-mysql-cdc-subnet-group \
    --db-subnet-group-description "Subnet group for MySQL CDC" \
    --subnet-ids $SUBNET_1 $SUBNET_2 \
    --region $AWS_REGION

# 5. Create RDS MySQL instance
# if the monitoring role doesn't exist.
# cat > trust-policy.json << 'EOF'
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": "monitoring.rds.amazonaws.com"
#       },
#       "Action": "sts:AssumeRole"
#     }
#   ]
# }
# EOF
# aws iam create-role \
#     --role-name rds-monitoring-role \
#     --assume-role-policy-document file://trust-policy.json \
#     --description "Role for RDS Enhanced Monitoring"

aws rds create-db-instance \
    --db-instance-identifier flink-cdc-mysql-8-0 \
    --db-instance-class db.m5.2xlarge \
    --engine mysql \
    --engine-version 8.0.45 \
    --master-username $DB_USERNAME \
    --master-user-password "$DB_PASSWORD" \
    --allocated-storage 500 \
    --storage-type gp3 \
    --db-subnet-group-name rds-mysql-cdc-subnet-group \
    --vpc-security-group-ids $SG_ID \
    --backup-retention-period 7 \
    --preferred-backup-window "03:00-04:00" \
    --preferred-maintenance-window "Mon:04:00-Mon:05:00" \
    --enable-cloudwatch-logs-exports '["error","general","slowquery"]' \
    --no-publicly-accessible \
    --storage-encrypted \
    --monitoring-interval 60 \
    --monitoring-role-arn arn:aws:iam::$AWS_ACCOUNT_ID:role/rds-monitoring-role \
    --enable-performance-insights \
    --performance-insights-retention-period 7 \
    --db-name $DB_NAME \
    --region $AWS_REGION

# 6. Wait for RDS instance to become available (5-10 minutes)
echo "Waiting for RDS instance to be available..."
aws rds wait db-instance-available \
    --db-instance-identifier flink-cdc-mysql-8-0 \
    --region $AWS_REGION

# 7. Get the RDS endpoint
export MYSQL_HOST=$(aws rds describe-db-instances \
    --db-instance-identifier flink-cdc-mysql-8-0 \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text \
    --region $AWS_REGION)

echo " MySQL RDS Instance Created!"
echo "   Endpoint: $MYSQL_HOST"
echo "   Port: 3306"
echo "   Database: $DB_NAME"
echo "   Username: $DB_USERNAME"
```

### Verify RDS Creation

```bash
# Check instance status
aws rds describe-db-instances \
    --db-instance-identifier flink-cdc-mysql-8-0 \
    --query 'DBInstances[0].[DBInstanceIdentifier,DBInstanceStatus,Endpoint.Address]' \
    --output table

# Expected output:
# -----------------------------------------------------------
# |              DescribeDBInstances                        |
# +-----------------+-----------+---------------------------+
# |  flink-cdc-mysql|  available| xxx.rds.amazonaws.com    |
# +-----------------+-----------+---------------------------+
```

---

## Step 2: Configure MySQL for CDC

### 2.1 Connect to MySQL via Cloudshell in EKS VPC
Launch a Cloudshell from RDS console, ensure the Cloudshell is associated with the EKS's VPC and a private subnet, choose the same RDS's SG created above. 

```bash
# Install MySQL client if not already installed
sudo dnf install mariadb105 -y  # Amazon Linux 2023

# Connect to RDS
mysql -h $MYSQL_HOST -u $DB_USERNAME -p$DB_PASSWORD

# You should see:
# MySQL [(none)]>
```

### 2.2 Verify Binlog is Enabled (Critical for CDC)

```sql
-- Check if binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';
-- Expected output:
-- +---------------+-------+
-- | Variable_name | Value |
-- +---------------+-------+
-- | log_bin       | ON    |
-- +---------------+-------+

-- Check binlog format (should be ROW for CDC)
SHOW VARIABLES LIKE 'binlog_format';
-- Expected output:
-- +---------------+-------+
-- | Variable_name | Value |
-- +---------------+-------+
-- | binlog_format | ROW   |
-- +---------------+-------+

-- Check binlog retention
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
-- Should be > 86400 (24 hours) for safety
```

**✅ Good News**: RDS MySQL **automatically enables binlog** with correct settings!

<!-- If binlog is not enabled (unlikely for RDS), you need to modify the parameter group:
```bash
# Create custom parameter group
aws rds create-db-parameter-group \
    --db-parameter-group-name mysql-cdc-params \
    --db-parameter-group-family mysql8.0 \
    --description "MySQL parameters for CDC"

# Enable binlog
aws rds modify-db-parameter-group \
    --db-parameter-group-name mysql-cdc-params \
    --parameters "ParameterName=log_bin,ParameterValue=1,ApplyMethod=pending-reboot"

# Apply to RDS instance (requires reboot)
aws rds modify-db-instance \
    --db-instance-identifier flink-cdc-mysql \
    --db-parameter-group-name mysql-cdc-params \
    --apply-immediately
``` -->

### 2.3 Create CDC User with Proper Permissions

```sql
-- Create CDC user (use from MySQL prompt)
CREATE USER 'cdcuser'@'%' IDENTIFIED BY 'CDCPassword123!';

-- Grant replication permissions (required for CDC)
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'cdcuser'@'%';

-- Grant full access to the ecommerce database (for data reading)
GRANT ALL PRIVILEGES ON ecommerce.* TO 'cdcuser'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Verify user was created
SELECT User, Host FROM mysql.user WHERE User = 'cdcuser';
-- Expected output:
-- +---------+------+
-- | User    | Host |
-- +---------+------+
-- | cdcuser | %    |
-- +---------+------+

-- Verify grants
SHOW GRANTS FOR 'cdcuser'@'%';
-- Expected output should include:
-- GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cdcuser`@`%`
-- GRANT ALL PRIVILEGES ON `ecommerce`.* TO `cdcuser`@`%`
```

### 2.4 Test CDC User Connection

```bash
# Exit MySQL and reconnect as CDC user
exit;

# Test connection with CDC user
mysql -h $MYSQL_HOST -u cdcuser -p'CDCPassword123!';
# If successful, you're ready for CDC!
exit;
```

---

## Step 3: Create Database Schema

### 3.1 Create Database (if not created during RDS setup)

```sql
-- Connect as admin user
mysql -h $MYSQL_HOST -u $DB_USERNAME -p$DB_PASSWORD

-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Use the database
USE ecommerce;

-- Verify database
SHOW DATABASES LIKE 'ecommerce';
```

### 3.2 Create E-Commerce Tables

**Copy and paste this complete schema:**

```sql
USE ecommerce;

-- ============================================================================
-- Table: customers
-- Purpose: Customer dimension table
-- ============================================================================
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    zip_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_email (email),
    INDEX idx_city (city),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- Table: products
-- Purpose: Product catalog
-- ============================================================================
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_category (category),
    INDEX idx_product_name (product_name),
    INDEX idx_price (price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- Table: orders
-- Purpose: Order transactions (fact table)
-- ============================================================================
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12, 2) NOT NULL,
    order_status VARCHAR(20) DEFAULT 'PENDING',
    payment_method VARCHAR(50),
    shipping_address TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    INDEX idx_customer_id (customer_id),
    INDEX idx_order_date (order_date),
    INDEX idx_order_status (order_status),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- Table: order_items
-- Purpose: Order line items (fact table)
-- ============================================================================
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(12, 2) AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- Verify tables were created
-- ============================================================================
SHOW TABLES;

-- Expected output:
-- +--------------------+
-- | Tables_in_ecommerce|
-- +--------------------+
-- | customers          |
-- | order_items        |
-- | orders             |
-- | products           |
-- +--------------------+

-- Check table structures
DESCRIBE customers;
DESCRIBE products;
DESCRIBE orders;
DESCRIBE order_items;
```

### 3.3 Verify Schema

```sql
-- Check all tables
SELECT TABLE_NAME, ENGINE, TABLE_ROWS, CREATE_TIME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'ecommerce';

-- Check foreign keys
SELECT
    TABLE_NAME,
    COLUMN_NAME,
    CONSTRAINT_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'ecommerce'
  AND REFERENCED_TABLE_NAME IS NOT NULL;

-- Check indexes
SELECT
    TABLE_NAME,
    INDEX_NAME,
    COLUMN_NAME,
    NON_UNIQUE
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'ecommerce'
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
```

---

## Step 4: Load Initial Sample Data

### 4.1 Insert Sample Customers

```sql
USE ecommerce;

INSERT INTO customers (customer_name, email, phone, address, city, state, zip_code) VALUES
('John Smith', 'john.smith@email.com', '+1-206-555-0101', '123 Main St', 'Seattle', 'WA', '98101'),
('Jane Doe', 'jane.doe@email.com', '+1-206-555-0102', '456 Oak Ave', 'Seattle', 'WA', '98102'),
('Mike Johnson', 'mike.j@email.com', '+1-503-555-0103', '789 Pine Blvd', 'Portland', 'OR', '97201'),
('Sarah Williams', 'sarah.w@email.com', '+1-415-555-0104', '321 Elm St', 'San Francisco', 'CA', '94102'),
('David Brown', 'david.b@email.com', '+1-206-555-0105', '654 Maple Dr', 'Seattle', 'WA', '98103'),
('Emily Davis', 'emily.d@email.com', '+1-503-555-0106', '987 Cedar Ln', 'Portland', 'OR', '97202'),
('Chris Wilson', 'chris.w@email.com', '+1-415-555-0107', '147 Birch Ave', 'San Francisco', 'CA', '94103'),
('Lisa Anderson', 'lisa.a@email.com', '+1-206-555-0108', '258 Spruce St', 'Seattle', 'WA', '98104'),
('Tom Martinez', 'tom.m@email.com', '+1-503-555-0109', '369 Willow Rd', 'Portland', 'OR', '97203'),
('Amy Taylor', 'amy.t@email.com', '+1-415-555-0110', '741 Ash Ct', 'San Francisco', 'CA', '94104');

-- Verify
SELECT COUNT(*) as customer_count FROM customers;
SELECT * FROM customers LIMIT 5;
```

### 4.2 Insert Sample Products

```sql
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
-- Electronics
('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'High-performance laptop with 16GB RAM'),
('Smartphone X', 'Electronics', 899.99, 100, 'Latest smartphone with 5G capability'),
('Wireless Headphones', 'Electronics', 199.99, 150, 'Noise-canceling wireless headphones'),
('4K Monitor', 'Electronics', 449.99, 75, '27-inch 4K UHD monitor'),
('Gaming Keyboard', 'Electronics', 129.99, 200, 'Mechanical RGB gaming keyboard'),
-- Clothing
('Cotton T-Shirt', 'Clothing', 24.99, 500, 'Comfortable 100% cotton t-shirt'),
('Denim Jeans', 'Clothing', 59.99, 300, 'Classic fit denim jeans'),
('Running Shoes', 'Clothing', 89.99, 250, 'Lightweight running shoes'),
('Winter Jacket', 'Clothing', 149.99, 100, 'Waterproof winter jacket'),
('Baseball Cap', 'Clothing', 19.99, 400, 'Adjustable baseball cap'),
-- Books
('Python Programming Guide', 'Books', 39.99, 200, 'Comprehensive Python programming book'),
('Data Science Handbook', 'Books', 49.99, 150, 'Complete guide to data science'),
('Cloud Computing Essentials', 'Books', 44.99, 175, 'AWS and cloud computing fundamentals'),
('Mystery Novel', 'Books', 14.99, 300, 'Bestselling mystery thriller'),
('Cookbook Delights', 'Books', 29.99, 250, '500 delicious recipes'),
-- Home & Garden
('Coffee Maker', 'Home & Garden', 79.99, 120, '12-cup programmable coffee maker'),
('Vacuum Cleaner', 'Home & Garden', 199.99, 80, 'Bagless upright vacuum cleaner'),
('LED Desk Lamp', 'Home & Garden', 34.99, 300, 'Adjustable LED desk lamp'),
('Garden Tool Set', 'Home & Garden', 89.99, 150, '10-piece garden tool set'),
('Throw Pillow Set', 'Home & Garden', 39.99, 200, 'Set of 4 decorative throw pillows');

-- Verify
SELECT COUNT(*) as product_count FROM products;
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
-- Electronics
('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'High-performance laptop with 16GB RAM'),
('Smartphone X', 'Electronics', 899.99, 100, 'Latest smartphone with 5G capability'),
('Wireless Headphones', 'Electronics', 199.99, 150, 'Noise-canceling wireless headphones'),
('4K Monitor', 'Electronics', 449.99, 75, '27-inch 4K UHD monitor'),
('Gaming Keyboard', 'Electronics', 129.99, 200, 'Mechanical RGB gaming keyboard'),
-- Clothing
('Cotton T-Shirt', 'Clothing', 24.99, 500, 'Comfortable 100% cotton t-shirt'),
('Denim Jeans', 'Clothing', 59.99, 300, 'Classic fit denim jeans'),
('Running Shoes', 'Clothing', 89.99, 250, 'Lightweight running shoes'),
('Winter Jacket', 'Clothing', 149.99, 100, 'Waterproof winter jacket'),
('Baseball Cap', 'Clothing', 19.99, 400, 'Adjustable baseball cap'),
-- Books
('Python Programming Guide', 'Books', 39.99, 200, 'Comprehensive Python programming book'),
('Data Science Handbook', 'Books', 49.99, 150, 'Complete guide to data science'),
('Cloud Computing Essentials', 'Books', 44.99, 175, 'AWS and cloud computing fundamentals'),
('Mystery Novel', 'Books', 14.99, 300, 'Bestselling mystery thriller'),
('Cookbook Delights', 'Books', 29.99, 250, '500 delicious recipes'),
-- Home & Garden
('Coffee Maker', 'Home & Garden', 79.99, 120, '12-cup programmable coffee maker'),
('Vacuum Cleaner', 'Home & Garden', 199.99, 80, 'Bagless upright vacuum cleaner'),
('LED Desk Lamp', 'Home & Garden', 34.99, 300, 'Adjustable LED desk lamp'),
('Garden Tool Set', 'Home & Garden', 89.99, 150, '10-piece garden tool set'),
('Throw Pillow Set', 'Home & Garden', 39.99, 200, 'Set of 4 decorative throw pillows');

```

### 4.3 Insert Sample Orders and Order Items

```sql
-- Order 1: John Smith's order
INSERT INTO orders (customer_id, total_amount, order_status, payment_method, shipping_address)
VALUES (1, 1599.97, 'DELIVERED', 'CREDIT_CARD', '123 Main St, Seattle, WA 98101');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
    (LAST_INSERT_ID(), 1, 1, 1299.99),  -- Laptop
    (LAST_INSERT_ID(), 5, 2, 129.99);   -- Gaming Keyboard x2

-- Order 2: Jane Doe's order
INSERT INTO orders (customer_id, total_amount, order_status, payment_method, shipping_address)
VALUES (2, 989.98, 'SHIPPED', 'PAYPAL', '456 Oak A
'e
    (LAST_INSERT_ID(), 8, 1, 89.99);    -- Running Shoes

-- Order 3: Mike Johnson's order
INSERT INTO orders (customer_id, total_amount, order_status, payment_method, shipping_address)
VALUES (3, 219.97, 'CONFIRMED', 'CREDIT_CARD', '789 Pine Blvd, Portland, OR 97201');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
    (LAST_INSERT_ID(), 3, 1, 199.99),   -- Headphones
    (LAST_INSERT_ID(), 10, 1, 19.99);   -- Baseball Cap

-- Order 4: Sarah Williams' order
INSERT INTO orders (customer_id, total_amount, order_status, payment_method, shipping_address)
VALUES (4, 134.97, 'PENDING', 'APPLE_PAY', '321 Elm St, San Francisco, CA 94102');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
    (LAST_INSERT_ID(), 11, 1, 39.99),   -- Python Book
    (LAST_INSERT_ID(), 12, 1, 49.99),   -- Data Science Book
    (LAST_INSERT_ID(), 15, 1, 44.99);   -- Cookbook

-- Order 5: David Brown's order
INSERT INTO orders (customer_id, total_amount, order_status, payment_method, shipping_address)
VALUES (5, 369.97, 'DELIVERED', 'CREDIT_CARD', '654 Maple Dr, Seattle, WA 98103');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
    (LAST_INSERT_ID(), 16, 1, 79.99),   -- Coffee Maker
    (LAST_INSERT_ID(), 17, 1, 199.99),  -- Vacuum Cleaner
    (LAST_INSERT_ID(), 18, 2, 34.99),   -- LED Lamp x2
    (LAST_INSERT_ID(), 10, 1, 19.99);   -- Baseball Cap

-- Verify orders
SELECT COUNT(*) as order_count FROM orders;
SELECT COUNT(*) as order_item_count FROM order_items;

-- View order summary
SELECT
    o.order_id,
    c.customer_name,
    o.order_date,
    o.total_amount,
    o.order_status,
    COUNT(oi.order_item_id) as item_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, c.customer_name, o.order_date, o.total_amount, o.order_status
ORDER BY o.order_date DESC;
```

---

## Step 5: Verify CDC Readiness

### 5.1 Final Verification Checklist

```sql
-- Check data counts
SELECT
    'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items;

-- Expected output:
-- +-------------+-----------+
-- | table_name  | row_count |
-- +-------------+-----------+
-- | customers   |        10 |
-- | products    |        20 |
-- | orders      |         5 |
-- | order_items |        11 |
-- +-------------+-----------+

-- 5. Test a simple UPDATE to verify binlog captures changes
UPDATE customers SET city = 'Bellevue' WHERE customer_id = 1;

-- 6. Check binlog position (this will be used by Flink CDC, ensure mysql version < 8.4.x)
SHOW MASTER STATUS;
-- Should show current binlog file and position
```

## Step 6: Test Connection from EKS

Before running Flink CDC, test that your EKS pods can reach the MySQL database. Run the followings from your local terminal, not in cloudshell.

```bash
# Deploy a test pod in the same namespace
kubectl run mysql-test --rm -it --restart=Never \
  --image=mysql:8.0.45 \
  --namespace=emr-flink \
  --command -- bash
# after login to the pod, set the RDS host name
MYSQL_HOST=xxxxxxxxx
# Inside the pod, test connection:
mysql -h $MYSQL_HOST -u cdcuser -p'CDCPassword123!' -e "SELECT COUNT(*) FROM ecommerce.customers;"

# If successful, you'll see:
# +----------+
# | COUNT(*) |
# +----------+
# |       10 |
# +----------+

# Exit the pod
exit;
```

---


## Step 7. Export Environment Variables for Flink CDC in Local Terminal

```bash
# Export these for use with Flink CDC deployment
export AWS_REGION=us-west-2
export MYSQL_HOST=$(aws rds describe-db-instances \
    --db-instance-identifier flink-cdc-mysql-8-0 \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text \
    --region $AWS_REGION)

export MYSQL_PORT=3306
export MYSQL_DATABASE=ecommerce
export MYSQL_USER=cdcuser
export MYSQL_PASSWORD='CDCPassword123!'

# Save to a file for easy reuse
cat > mysql-cdc-env.sh <<EOF
export MYSQL_HOST=$MYSQL_HOST
export MYSQL_PORT=3306
export MYSQL_DATABASE=ecommerce
export MYSQL_USER=cdcuser
export MYSQL_PASSWORD='CDCPassword123!'
EOF

chmod +x mysql-cdc-env.sh
echo $MYSQL_HOST
```

---

## Troubleshooting

### Issue: Cannot connect to RDS from EKS

```bash
# 1. Verify security group allows traffic from EKS
aws ec2 describe-security-groups \
    --group-ids $SG_ID \
    --query 'SecurityGroups[0].IpPermissions'

# 2. Check RDS is in the same VPC as EKS
aws rds describe-db-instances \
    --db-instance-identifier flink-cdc-mysql \
    --query 'DBInstances[0].DBSubnetGroup.VpcId'

# 3. Test DNS resolution
kubectl run dns-test --rm -it --restart=Never \
  --image=busybox \
  --namespace=emr-flink \
  -- nslookup $MYSQL_HOST

# 4. Test network connectivity
kubectl run netcat-test --rm -it --restart=Never \
  --image=busybox \
  --namespace=emr-flink \
  -- nc -zv $MYSQL_HOST 3306
```

### Issue: Binlog not enabled

This is rare for RDS, but if you see `log_bin = OFF`:

```bash
# Check parameter group
aws rds describe-db-instances \
    --db-instance-identifier flink-cdc-mysql \
    --query 'DBInstances[0].DBParameterGroups'

# RDS MySQL 8.0+ has binlog enabled by default
# If not, you need to enable automated backups (this enables binlog)
aws rds modify-db-instance \
    --db-instance-identifier flink-cdc-mysql \
    --backup-retention-period 7 \
    --apply-immediately
```

### Issue: CDC user permission denied

```sql
-- Reconnect as admin and re-grant permissions
mysql -h $MYSQL_HOST -u admin -p

-- Drop and recreate user
DROP USER IF EXISTS 'cdcuser'@'%';
CREATE USER 'cdcuser'@'%' IDENTIFIED BY 'CDCPassword123!';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcuser'@'%';
GRANT ALL PRIVILEGES ON ecommerce.* TO 'cdcuser'@'%';
FLUSH PRIVILEGES;
```

---


## Summary

✅ **RDS MySQL Instance Created** with binlog enabled
✅ **Security groups configured** for EKS access
✅ **CDC user created** with proper permissions
✅ **Database schema deployed** (4 tables)
✅ **Sample data loaded** for testing
✅ **CDC readiness verified**

Your MySQL database is now ready to be the source for Flink CDC! 🎉
