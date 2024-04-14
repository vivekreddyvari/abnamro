
## Credit Lending Product

The product is designed to facilitate the lending process by collecting and transforming relevant data from various sources. The transformed data is then used to create basic and advanced analytics reports that can be used to improve the credit lending process.

## Architecture
![architecture] (docs/Credit Lending Product.png)
### The product's architecture consists of the following main components:

- **Data Ingestion Layer**: This component is responsible for ingesting data from various sources, such as CSV files, JSON files, and databases.

- **Data Transformation**: This component is responsible for transforming the ingested data into a consistent format that can be used by the rest of the system.

- **Data Storage**: This component is responsible for storing the transformed data in a scalable and reliable manner.

- **Data Analytics**: This component is responsible for performing analytics on the stored data to generate insights that can be used to improve the credit lending process.

- **User Interface**: This component is responsible for providing a user-friendly interface for users to interact with the system.



###  Apache Spark for distributed data processing
Python for data manipulation and transformation
- Data Transformation: **PySpark** SQL for data transformation and aggregation
- Python for custom business logic

**Data Loading**:
- resources.datagenerators are used to generate the dummy data.
- Apache Parquet for efficient data storage and retrieval

**Data Sources**:

- Structured: CSV files (Clients.csv, Collaterals.csv)
- Semi-structured: JSON file (stocks.json)

**Scalability**:
- Apache Spark for distributed computing
- Cloud-based infrastructure (e.g., AWS, Azure) for elastic scaling (if you want to implement in cloud)

###  Running the code
You could use following instructions as guidelines to run the application:
```bash
# Install pre-requisites needed by requirements.txt
pip install -r requirements.txt
```

### Running the spark program jobs.
``` 
chmod +x ./ingest.sh
```

### Running the spark program tests.
``` 
chmod +x ./test_ingest.sh
```

### Technical Structure: 

```
credit-lending-product-env
data_transformations
    |_ ingest.py
docs
    |_ Credit Lending Product.png (has architectural diagram)
    |_ data_engineers_assessment.pptx (What the assignment is ?)
jobs
    |_ ingest.sh
    |_ test_ingest.sh
metastore_db
output
    |_ collateral_status.parquet
        |_ ._SUCCESS.crc
        |_ part-00000-c5dae749-02c9-49ea-a22e-d50ef8251fab-c000.snappy.parquet
    |_ part-00000-c5dae749-02c9-49ea-a22e-d50ef8251fab-c000.snappy.csv
    
resources
    |_ clients
    |   |_clients.csv
    |    
    |_ collateral
    |   |_ collateral.csv
    |_ datagenerators {This programs are used to generate dummy data}
        |_ client_data_generator.py
        |_ collateral_data_generator.py
        |_ stock_data_generator.py
    |_ stocks
        |_ stocks.json
schemas
    |_ schema_enforcement.py
    
tests
    |_ tests.py
 
README.MD
requirements.txt    
        
```
