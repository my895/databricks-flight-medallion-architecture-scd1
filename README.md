# 🛫 Databricks Flight Data Medallion Architecture with SCD 

![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion-BrightGreen) 
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-0073e6) 
![SCD Type1](https://img.shields.io/badge/SCD-Type%201-orange)
![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621)
![dbt](https://img.shields.io/badge/ETL-dbt-FF694D)
![PySpark](https://img.shields.io/badge/Processing-PySpark-orange)

A production-grade implementation of **Databricks Medallion Architecture** with **SCD Type 1** logic for airline bookings data. This pipeline ensures high data reliability, modularity, and maintainability using Delta Live Tables and Auto Loader.
## 🧱 Architecture Overview

![Screenshot 2025-07-07 004053](https://github.com/user-attachments/assets/3deb1e46-208c-41ba-b8d3-e119b5cb616e)


## ⚙️ Key Features

- **Auto Loader** for efficient streaming ingestion
- **SCD Type 1** implementation using `dlt.create_auto_cdc_flow`
- **Data Quality Enforcement** with `dlt.expect_all_or_drop`
- **Dimensional Modeling** in Gold layer
- **Delta Lake** storage format
- **GitHub Integration** for version control

## 🚀 How to Run

1. **Clone Repository** in Databricks Workspace:
   ```bash
   Repos > Add Repo > https://github.com/my895/databricks-flight-medallion-architecture-scd1
   ```

2. **Upload Raw Data** to Volumes:
  

3. **Execute Pipeline** in Order:
   - Bronze Layer Notebooks
   - Silver Layer Notebooks
   - Gold Layer Notebook 


## 📚 Resources

- [Medallion Architecture Documentation](https://www.databricks.com/blog/2020/06/30/what-is-a-medallion-architecture.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [SCD Type 1 with DLT](https://docs.databricks.com/delta-live-tables/cdc.html)
  
