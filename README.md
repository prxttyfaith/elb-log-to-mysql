This Python script performs an ETL (Extract, Transform, Load) process that pulls AWS Application Load Balancer (ALB/ELB) access logs from an S3 bucket, transforms the logs, and loads the processed data into a MySQL database table.

### Tools Used
- **Python** – for scripting the ETL logic  
- **Pandas** – for data transformation and manipulation  
- **boto3** – to connect and interact with AWS S3  
- **python-dotenv** – to manage AWS and DB credentials from a `.env` file  
- **SQLAlchemy** – to handle database interaction with MySQL  
- **AWS S3** – as the source of ELB log files  
- **MySQL** – as the final destination of transformed records  
