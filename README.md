# Healthcare Claims Intelligence Pipeline

An end-to-end data engineering case study demonstrating ETL pipelines, SQL analytics, and machine learning feature preparation for healthcare insurance claims processing.

## Project Overview

This project implements a complete claims processing system that includes:

- Processing and cleaning 5,000+ healthcare insurance claims records
- Implementing robust data quality validation and transformation
- Building interactive analytics dashboards with real-time data access
- Generating machine learning-ready datasets for fraud detection and approval prediction models
- Demonstrating scalable, production-grade system architecture

## Quick Start

### Prerequisites

- Python 3.12 or higher
- PostgreSQL 16.13 or higher
- 4GB RAM minimum, 8GB recommended
- 2GB disk space

### Installation

Clone the repository and navigate to the project directory:

```bash
git clone <repository-url>
cd ginja-ai-claims-pipeline
```

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure environment variables by creating a .env file:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ginja_claims
DB_USER=postgres
DB_PASSWORD=<your_password>
```

### Running the Pipeline

Execute the complete ETL pipeline:

```bash
python src/etl_pipeline.py
```

Launch the interactive dashboard:

```bash
streamlit run dashboards/app.py
```

The dashboard will open at http://localhost:8501

## Project Structure

```
ginja-ai-claims-pipeline/
├── README.md                      # This file
├── SETUP.md                       # Detailed setup instructions
├── TESTING.md                     # Unit testing guide
├── docs/
│   ├── ARCHITECTURE.md            # System design and data flow
│   ├── ASSUMPTIONS.md             # Design decisions and assumptions
│   ├── METRICS.md                 # KPI definitions
│   ├── DATA_DICTIONARY.md         # Database schema
│   └── FEATURES.md                # Feature engineering documentation
├── src/
│   ├── generate_data.py           # Synthetic data generation
│   ├── extract.py                 # Data extraction
│   ├── transform.py               # Data cleaning and validation
│   ├── load.py                    # Database loading
│   ├── database.py                # Database connection management
│   ├── etl_pipeline.py            # Pipeline orchestration
│   └── feature_engineering.py     # ML feature creation
├── dashboards/
│   ├── app.py                     # Streamlit application
│   └── queries.sql                # SQL analytics queries
├── data/
│   ├── raw/                       # Input CSV files
│   └── processed/                 # ML-ready datasets
├── tests/
│   ├── test_etl.py                # ETL unit tests
│   ├── test_database.py           # Database unit tests
│   ├── test_runner.py             # Test execution
│   └── __init__.py
├── requirements.txt               # Python dependencies
└── .env                          # Environment configuration (git ignored)
```

## What This Project Includes

### Task 1: Synthetic Data Generation

The system generates 5,000 realistic healthcare claims records with intentional data quality issues to demonstrate real-world scenarios:

- 500 unique patient records
- 10 different healthcare providers
- 5 insurance companies
- Real ICD-10 diagnosis codes
- Kenyan geographic locations for local relevance
- Deliberate quality issues: 20 duplicates, 50 missing values, 30 outliers, and 15 date inconsistencies

### Task 2: ETL Pipeline

A modular pipeline that extracts, transforms, and loads data into PostgreSQL:

- Extract phase reads data from CSV files into memory
- Transform phase handles duplicates, missing values, date errors, and text standardization
- Load phase creates the database schema and inserts cleaned data with proper relationships
- Connection pooling ensures efficient database resource management
- Full error handling and transaction management for data integrity

### Task 3: SQL Analytics

Thirty SQL queries organized by user role:

- Executive queries provide high-level business metrics and trends
- Operations queries focus on process efficiency and data quality
- Product team queries examine data distributions and patterns
- Exploratory queries identify unusual patterns and relationships

### Task 4: Interactive Dashboards

Three Streamlit dashboards providing real-time data visualization:

- Executive Summary: Key performance indicators, approval rates, and trends
- Operations Metrics: Data quality scores, processing time analysis, and items requiring review
- ML Insights: Claim amount distributions, provider ratings, and fraud signals

Dashboards include interactive Plotly charts that update in real-time from the database.

### Task 5: Feature Engineering

Twenty-five engineered features prepared for machine learning:

- Temporal features: days to claim, service month, weekend indicator
- Amount features: normalized claims, high-value flags, approval ratios
- Provider features: experience years, historical approval rates
- Member features: claim frequency, approval history, repeat customer indicator
- Diagnosis features: encoded diagnosis codes, diagnosis-specific approval rates
- Derived features: composite risk scores and fraud indicators

Three specialized datasets generated for different ML use cases:
- ml_fraud_detection.csv: optimized for fraud classification
- ml_approval_prediction.csv: prepared for approval amount regression
- ml_ready_claims_full.csv: complete feature set for experimentation

## Results and Metrics

### Data Processing

- Original records: 5,020 claims
- After cleaning: 5,000 claims
- Data completeness achieved: 100%
- Processing time: Less than 5 seconds for full pipeline

### Database

- Claims records: 5,000
- Provider records: 10
- Insurer records: 5
- Query response time: Under 1 second with connection pooling

### Analysis and Visualization

- SQL queries created: 30+
- Dashboard pages: 3
- Interactive charts: 15+
- Concurrent users supported: 10+

### Machine Learning Preparation

- Features engineered: 25+
- ML datasets created: 3
- Feature completeness: 100%
- Data types properly validated and normalized

## Technology Stack

The project uses industry-standard technologies:

- Python 3.12 for all data processing and scripting
- Pandas and NumPy for data manipulation and numerical computing
- PostgreSQL for data storage and SQL analytics
- SQLAlchemy for database connection management and pooling
- Streamlit for building interactive web dashboards
- Plotly for interactive data visualization
- Scikit-learn for machine learning preparation

## Documentation

Comprehensive documentation is provided in the docs folder:

- **ARCHITECTURE.md**: Detailed explanation of system design, data flow, and components
- **ASSUMPTIONS.md**: Design decisions, business rules, and technical constraints
- **METRICS.md**: Definitions of all key performance indicators and how they are calculated
- **DATA_DICTIONARY.md**: Complete database schema with field descriptions and data types
- **FEATURES.md**: Detailed explanation of each engineered feature and its purpose

## Testing

The project includes 31 unit tests covering:

- Data transformation functions
- Feature engineering logic
- Database operations
- Data quality validation
- Data integrity checks

Run tests with:

```bash
python tests/test_runner.py
```

## Next Steps for Production

To deploy this system in a production environment:

1. Implement automated ETL scheduling using Airflow or similar tools
2. Set up data quality monitoring and alerting
3. Deploy the dashboard to a cloud platform such as Streamlit Cloud or Heroku
4. Establish database backup and recovery procedures
5. Implement role-based access control and authentication
6. Set up CI/CD pipelines for automated testing and deployment

## Development and Implementation Notes

The pipeline was developed with attention to software engineering best practices including modular code organization, comprehensive error handling, and clear separation of concerns. The codebase demonstrates understanding of database design principles through proper use of primary keys, foreign keys, and referential integrity constraints.

The feature engineering approach combines domain knowledge of healthcare claims with statistical techniques to create meaningful signals for machine learning. The synthetic data includes intentional quality issues to showcase real-world data challenges and the robustness of the cleaning logic.

All components are designed to scale horizontally, with connection pooling preventing database bottlenecks and stateless processing enabling parallel execution.

## License

This project is submitted as a case study for the Ginja AI data engineering internship program.

For detailed setup instructions, see SETUP.md. For information about running tests, see TESTING.md.