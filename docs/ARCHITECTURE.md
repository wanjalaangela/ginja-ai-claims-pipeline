# System Architecture

## Overview

The healthcare claims pipeline is built as a modular data processing system with clear separation between data ingestion, transformation, storage, and presentation layers. This document describes the overall system design and how components interact.

## Data Flow

The system follows a traditional ETL (Extract, Transform, Load) pattern:

1. Raw data is extracted from CSV files
2. Data is validated and cleaned through a series of transformation steps
3. Clean data is loaded into a PostgreSQL database
4. Analytics queries and dashboards provide access to the processed data
5. Machine learning features are engineered for downstream modeling

## Architecture Components

### Data Generation Layer

The system creates 5,000 synthetic healthcare claims records that realistically represent actual data with common quality issues:

- Duplicate claim records (20 instances)
- Missing values in key fields (50 instances)
- Date inconsistencies where claims are submitted before services (15 instances)
- Outlier claim amounts that are unusually high (30 instances)

This synthetic data approach allows the pipeline to demonstrate robust handling of real-world data quality problems without needing actual patient records.

### Extract Phase

The extract phase reads data from three CSV files into memory using pandas DataFrames:

- claims.csv: Contains 5,000+ claim records with details about amounts, status, and dates
- providers.csv: Contains 10 healthcare provider records with establishment dates and locations
- insurers.csv: Contains 5 insurance company records

The extraction is straightforward - it reads files and loads them into DataFrames for processing. This phase could be extended to read from databases, APIs, or other sources.

### Transform Phase

The transformation phase handles data cleaning and validation:

**Duplicate Removal**
Exact duplicate rows are identified and removed. This handles data entry errors or system glitches that created duplicate records.

**Missing Value Imputation**
Missing claim amounts are filled using the median value from the dataset. The median approach is preferred over mean because it is resistant to outliers - a few extremely large claims won't skew the imputed value for missing records.

**Date Validation**
Claims must logically be submitted after services are provided. If a claim date is earlier than the service date, the two dates are swapped to correct the obvious error.

**Text Standardization**
Status fields (Approved, Pending, Rejected) are standardized to title case for consistency.

**Amount Validation**
Claims with zero or negative amounts are removed as they don't represent valid claims.

### Load Phase

The load phase creates the database schema and inserts cleaned data:

**Database Schema**

The system uses three tables with proper relationships:

Claims table stores the main claim records with the following structure:
- claim_id: Unique identifier (primary key)
- member_id: Patient identifier
- provider_id: References the providers table (foreign key)
- claim_amount: Amount claimed in Kenyan Shillings
- approved_amount: Amount approved by insurance
- status: Current claim status
- Additional fields: diagnosis code, insurance plan, dates

Providers table stores healthcare facility information:
- provider_id: Unique identifier (primary key)
- provider_name: Facility name
- location: Geographic location
- established_year: Year the facility was established

Insurers table stores insurance company information:
- insurer_id: Unique identifier (primary key)
- insurer_name: Company name
- headquarters: Location

Foreign key constraints ensure data integrity - claims can only reference existing providers and insurers.

**Connection Management**

The system uses SQLAlchemy with connection pooling to manage database connections efficiently:

- A pool of 10 connections is maintained ready for queries
- Up to 20 additional connections can be created if needed
- Connections are recycled after 1 hour to prevent timeout issues
- Each connection is verified before use to ensure it is still alive

This connection pooling approach is essential for Streamlit dashboards where many queries may be executed rapidly.

### Analytics Layer

Thirty SQL queries provide insights organized by stakeholder needs:

Executive queries return high-level metrics like approval rates, claim volumes, and trends over time. These support leadership decision-making.

Operations queries examine processing efficiency metrics like average processing time by status and data quality scores. These support process improvement.

Product team queries look at data distributions and patterns like claim amount distributions by diagnosis code. These inform product and policy decisions.

### Presentation Layer

Three Streamlit dashboards present data through interactive visualizations:

The Executive Summary dashboard shows key performance indicators including total claims processed, approval rates, and trends. Charts are color-coded to show approval, pending, and rejection statuses.

The Operations Metrics dashboard focuses on process health including data quality scores, processing time analysis, and identification of claims requiring manual review. These metrics help the operations team identify bottlenecks and quality issues.

The ML Insights dashboard shows patterns relevant to machine learning including claim amount distributions, provider performance ratings, and fraud risk indicators. These help the data science team understand the data they will be modeling.

### Feature Engineering

The feature engineering layer creates 25+ derived features from the raw claim data:

Temporal features capture timing information like the number of days between service and claim submission, the month when service occurred, and whether the service was on a weekend.

Amount features normalize and categorize claim amounts, including z-score normalization, high-value flags for claims in the top 5%, and approval ratios showing what percentage of the claimed amount was approved.

Provider features aggregate information about healthcare providers, including years of experience and historical approval rates. These capture provider quality and reliability.

Member features aggregate information about individual patients including their claim frequency, historical approval rates, and whether they are repeat customers.

Diagnosis features track patterns by medical diagnosis code including approval rates specific to each diagnosis type.

Derived features combine multiple signals into composite metrics like risk scores and fraud indicators.

## Scalability Considerations

### Current System

The current system is designed to handle 5,000 to 10,000 claims:

- All data fits in memory during processing
- Single PostgreSQL instance on localhost
- Batch processing run manually or on a schedule
- Three concurrent users on the dashboard

### Scaling to Larger Volumes

If the system needs to handle 100,000+ claims:

**Vertical Scaling** (more powerful hardware):
- Increase available RAM to 64GB to load larger datasets
- Use SSD storage for faster database operations
- Limited by single machine resources

**Horizontal Scaling** (multiple machines):
- Split data processing across multiple workers
- Use read replicas for the database
- Load balance dashboard traffic across multiple servers
- Message queues for asynchronous processing

**Real-time Processing**:
- Replace batch processing with stream processing using Kafka
- Use Spark for distributed processing
- Update dashboards in real-time as claims arrive

## Data Quality Approach

The system implements multiple quality gates:

Before processing, completeness is checked - all required columns must be present.

During processing, values are validated against business rules - claims must be positive amounts, dates must be logical, status values must be from allowed set.

References are validated - claims must reference valid providers and insurers.

Duplicates are detected and removed.

The system logs what was fixed at each stage so stakeholders understand what data issues were encountered and how they were handled.

## Security and Operations

The database credentials are stored in a .env file that is not committed to version control. This prevents accidental exposure of passwords.

The codebase is version controlled with git, allowing recovery from mistakes and tracking of changes over time.

All components have error handling to prevent cascading failures - if one claim has an issue, processing continues for other claims.

Database operations use transactions so that either all data is loaded or none is, preventing partial states.

For production deployment, additional considerations would include:
- Automated backups of the database
- Access control limiting who can view which data
- Audit logging of who accessed what data and when
- Monitoring and alerting if processing fails or data quality drops
- Disaster recovery procedures

## Design Decisions

The system is built with modular components because this makes it easier to test, maintain, and modify individual pieces. If the data source changes, only the extract module needs modification.

Python was chosen because it has excellent libraries for data processing and the code is readable - important for a case study project.

PostgreSQL was chosen because it is open-source, widely used, and has good support for complex queries. It is suitable for analytical workloads.

Streamlit was chosen for dashboards because it reduces development time - Python developers can build dashboards without learning HTML/CSS/JavaScript.

Features are engineered conservatively - we created 25+ features rather than hundreds because more features make models harder to interpret and debug. We focused on features with clear business meaning.