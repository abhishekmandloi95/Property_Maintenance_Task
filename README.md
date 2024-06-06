# Property Maintenance Data Pipeline

## Star Schema

### Fact Table
- **Repairs Fact Table**
  - `property_id` (FK)
  - `repair_date_id` (FK)
  - `repair_count`
  - `total_repair_cost`

### Dimension Tables
- **Property Dimension Table**
  - `property_id` (PK)
  - `region_name`
  - `construction_year`
  - `occupants`

- **Date Dimension Table**
  - `repair_date_id` (PK)
  - `repair_year`

### Design Decisions

- **Star Schema**: A star schema was chosen to facilitate quick and efficient querying of repair records by separating facts (quantifiable information) from dimensions (contextual information).

- **Fact Table**:
  
  - **Repairs Fact Table**: This table was chosen as the fact table because it contains the measurable events (repairs) with metrics like repair_count and total_repair_cost. It uses composite keys property_id and repair_date_id to uniquely identify each record.

- **Dimension Tables**:
  
  - **Property Dimension Table**: This table contains contextual information about the properties such as region name, construction year and occupants. The property_id is the primary key because it uniquely identifies each property.
  
  - **Date Dimension Table**: This table contains information about the repair years. The repair_date_id is the primary key because it uniquely identifies each year and a unique identifier is generated for each year.

- **Foreign Keys**: The property_id and repair_date_id are used as foreign keys in the fact table to link to the property and date dimensions facilitating efficient joins for querying.

## Setup and run instructions

1. **Build and run docker container**
   
   docker build -t property-db .
   docker run -d -p 5432:5432 --name property-db-container property-db

2.  **Run the data pipeline**
   
   spark-submit --jars ./postgresql-42.7.3.jar data_pipeline.py

3. **Run the tests**

   spark-submit --jars ./postgresql-42.7.3.jar test_data_pipeline.py