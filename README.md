# Data Warehouse Course Project

A comprehensive data warehouse environment designed for teaching data science students about ETL processes, data warehousing, and business intelligence. This project includes a complete Docker-based setup with realistic dummy data and practical examples.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CSV Files    │    │     Airflow     │    │   PostgreSQL    │
│   (Raw Data)   │───▶│   (ETL Engine)  │───▶│  Data Warehouse │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │    Metabase     │
                                              │   (BI Layer)    │
                                              └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB RAM available
- Ports 3000, 5432, 6379, 8080 available
- **No Python installation required** - everything runs in containers!

### 1. Clone and Setup
```bash
git clone <your-repo-url>
cd data-engineering-sandbox
```

### 2. Start Everything with One Command
```bash
# First time (builds images and generates data)
docker compose up --build

# Subsequent runs (uses existing images and data)
docker compose up
```

### 3. Smart Data Generation
The system automatically generates realistic dummy data on first run:
- ✅ **1,000 users** with demographics and registration data
- ✅ **500 products** across multiple categories
- ✅ **10,000 transactions** with realistic purchasing patterns
- ✅ **10,000 activity logs** tracking user behavior
- ✅ **8,000 clickstream events** for web analytics
- ✅ **3 years of date dimensions** for time-based analysis

**Data is automatically regenerated only if missing** - existing data is preserved across restarts.

### 4. Access Services
- **Test Service**: http://localhost:8081 (verify setup is working)
- **Airflow**: http://localhost:8080 (admin/admin)
- **Metabase**: http://localhost:3000
- **PostgreSQL**: localhost:5432 (dw_user/dw_password)

## 🔗 Database Connection Guide

### DBeaver Setup (Recommended Database Client)

**DBeaver** is a free, universal database tool perfect for exploring your data warehouse. Here's how to connect:

#### 1. Download and Install DBeaver
- Visit: https://dbeaver.io/download/
- Download the **Community Edition** (free)
- Install following the standard process for your OS

#### 2. Create PostgreSQL Connection
1. **Open DBeaver** and click **"New Database Connection"** (plug icon) or press `Ctrl+Shift+N`
2. **Select PostgreSQL** from the database list
3. **Configure connection settings**:

```
Host: localhost
Port: 5432
Database: datawarehouse
Username: dw_user
Password: dw_password
```

4. **Test the connection** by clicking **"Test Connection"**
   - If prompted to download PostgreSQL driver, click **"Download"**
   - Should show: ✅ **"Connected"**

5. **Click "Finish"** to save the connection

#### 3. Explore Your Data Warehouse
Once connected, you'll see the database structure:

```
datawarehouse/
├── Schemas/
│   ├── dimensions/          # Master data tables
│   │   ├── users           # User information
│   │   ├── products        # Product catalog  
│   │   └── dates           # Date dimension
│   ├── raw_data/           # Raw imported data
│   │   ├── user_activity_logs
│   │   ├── transactions
│   │   └── clickstream
│   ├── processed_data/     # Cleaned/processed data
│   │   ├── daily_user_metrics
│   │   ├── daily_product_performance
│   │   └── daily_overall_metrics
│   └── aggregated_data/    # Summary tables
```

#### 4. Run Your First Query
Right-click on **`datawarehouse`** → **SQL Editor** → **New SQL Script**

Try this sample query:
```sql
-- Get top 10 users by transaction count
SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    COUNT(t.transaction_id) as transaction_count,
    SUM(t.amount) as total_spent
FROM dimensions.users u
LEFT JOIN raw_data.transactions t ON u.user_id = t.user_id
GROUP BY u.user_id, u.first_name, u.last_name, u.email
ORDER BY transaction_count DESC
LIMIT 10;
```

#### 5. Additional Database Connections (Optional)
You can also connect to the other databases in the same PostgreSQL instance:

**Airflow Metadata Database:**
```
Host: localhost
Port: 5432
Database: airflow
Username: dw_user
Password: dw_password
```

**Metabase Configuration Database:**
```
Host: localhost
Port: 5432
Database: metabase
Username: dw_user
Password: dw_password
```

### Alternative Database Clients
If you prefer other tools:
- **pgAdmin**: Web-based PostgreSQL administration
- **TablePlus**: Clean, native database client (paid)
- **DataGrip**: JetBrains database IDE (paid)
- **Command Line**: `docker exec -it data-engineering-sandbox-postgres-1 psql -U dw_user -d datawarehouse`

## 📊 Data Warehouse Schema

### Dimension Tables (`dimensions` schema)
- **`users`**: User information (demographics, registration)
- **`products`**: Product catalog (categories, pricing, suppliers)
- **`dates`**: Date dimension for time-based analysis

### Raw Data Tables (`raw_data` schema)
- **`user_activity_logs`**: Web activity and page views
- **`transactions`**: Purchase transactions
- **`clickstream`**: User click behavior

### Processed Tables (`processed_data` schema)
- **`daily_user_metrics`**: Aggregated user behavior metrics

### Aggregated Tables (`aggregated_data` schema)
- **`daily_product_performance`**: Product performance metrics
- **`daily_overall_metrics`**: System-wide KPIs

## 🔄 ETL Pipeline

### Data Ingestion DAG (`01_data_ingestion_dag.py`)
- Loads CSV files into PostgreSQL tables
- Demonstrates data loading best practices
- Includes data validation steps

### Data Transformation DAG (`02_data_transformation_dag.py`)
- Creates daily aggregated metrics
- Shows complex SQL transformations
- Implements incremental processing

## 📚 Learning Objectives

### For Students
1. **Data Modeling**: Understand star schema and dimensional modeling
2. **ETL Processes**: Learn how to orchestrate data pipelines with Airflow
3. **SQL Mastery**: Practice complex queries on realistic data
4. **Business Intelligence**: Create dashboards and visualizations
5. **Data Quality**: Learn data validation and testing techniques

### Key Concepts Covered
- **Data Warehousing**: Star schema, fact/dimension tables
- **ETL/ELT**: Extract, Transform, Load processes
- **Data Pipeline Orchestration**: Airflow DAGs and scheduling
- **SQL Analytics**: Window functions, CTEs, complex joins
- **Business Metrics**: KPIs, conversion rates, customer analytics

## 🎯 Sample SQL Queries

The `sql-practice/sample_queries.sql` file contains 50+ example queries covering:

- Basic data exploration
- JOIN operations
- Aggregation and grouping
- Window functions
- Subqueries and CTEs
- Time-based analysis
- Data quality checks
- Business metrics calculation
- Advanced analytics (cohort analysis)

## 🛠️ Hands-On Exercises

### Beginner Level
1. Count total users and products
2. Find users by country
3. Basic JOIN operations

### Intermediate Level
1. Calculate daily revenue trends
2. Product performance analysis
3. User engagement metrics

### Advanced Level
1. Cohort analysis
2. Customer lifetime value
3. Complex business intelligence queries

## 🔧 Configuration

### Smart Data Generation
The setup automatically detects if data exists and only generates it when needed:

- **First run**: Automatically generates dummy data
- **Subsequent runs**: Skips data generation (data already exists)
- **Data regeneration**: Delete `./data/` folder to regenerate

### Environment Variables
- `POSTGRES_DB`: Database name (default: datawarehouse)
- `POSTGRES_USER`: Database user (default: dw_user)
- `POSTGRES_PASSWORD`: Database password (default: dw_password)

### Ports
- **PostgreSQL**: 5432
- **Airflow**: 8080
- **Metabase**: 3000
- **Redis**: 6379

## 📈 Sample Business Questions

Students can practice answering these real-world questions:

1. **Customer Analytics**
   - Who are our top customers by revenue?
   - What's the customer retention rate?
   - Which countries have the highest engagement?

2. **Product Analytics**
   - Which products have the best conversion rates?
   - What's the profitability by category?
   - Which products are viewed but never purchased?

3. **Operational Analytics**
   - What are the peak usage hours?
   - How does performance vary by day of week?
   - What's the average session duration?

## 🐛 Troubleshooting

### Common Issues
1. **Port conflicts**: Ensure required ports are available
2. **Memory issues**: Increase Docker memory allocation
3. **Data not loading**: Check CSV file paths and permissions
4. **Services not accessible**: Wait for all services to fully start

### Debug Commands
```bash
# Check service status
docker compose ps

# Check if ports are bound
docker compose ps --format "table {{.Name}}\t{{.Ports}}"

# View logs
docker compose logs airflow-webserver

# Restart services
docker compose restart

# Reset environment
docker compose down -v
docker compose up -d

# Regenerate data (delete data folder first)
rm -rf ./data
docker compose up --build

# View data generation logs
docker compose logs data-generator
```

## 📚 Additional Resources

### Documentation
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Metabase Documentation](https://www.metabase.com/docs/)

### Learning Paths
1. **SQL Fundamentals**: Basic queries and joins
2. **Data Modeling**: Understanding schemas and relationships
3. **ETL Processes**: Pipeline design and orchestration
4. **Business Intelligence**: Dashboard creation and analysis

## 📁 Git Workflow & Data Management

### What's Tracked in Git
- ✅ Docker configuration files
- ✅ Airflow DAGs and Python scripts
- ✅ Database schema and initialization scripts
- ✅ Sample SQL queries and documentation
- ✅ Project configuration files

### What's Ignored (Auto-Generated)
- ❌ `data/` folder and all CSV files (regenerated automatically)
- ❌ Docker volumes and container data
- ❌ Airflow logs and temporary files
- ❌ Python cache files and virtual environments

### Sharing the Project
```bash
# Clone repository
git clone <repo-url>
cd data-engineering-sandbox

# Start with fresh data generation
docker compose up --build

# Data will be automatically generated - no manual setup needed!
```

**Students get a clean environment every time** without needing to manage large data files in Git.

## 🤝 Contributing

This project is designed for educational purposes. Feel free to:
- Add more sample queries
- Create additional DAGs
- Improve documentation
- Suggest new features

## 📄 License

This project is for educational use. Please ensure compliance with your institution's policies.

---

**Happy Learning! 🎉**

Your students will have a hands-on experience with real-world data engineering concepts, from basic SQL queries to complex ETL pipelines and business intelligence dashboards.
