# Order Value Calculator

A Python-based ETL procedure for calculating monthly order values from Snowflake data sources. This automated process handles complex calculations including outlier detection, data aggregation, and various business logic implementations for accurate order value determination.

## Overview

This project implements a Snowflake stored procedure that:
1. Processes monthly job data to calculate order values
2. Handles unit of measure conversions and normalizations
3. Implements business logic for charge calculations
4. Performs outlier detection and removal
5. Aggregates data at various levels
6. Produces standardized reports

## Architecture

### Data Flow
```
Raw Data Sources → Data Formatting → Aggregation → Calculations → Final Output
    ↓                    ↓               ↓              ↓            ↓
Jobs Data        UOM Conversion    Site-level Stats   Order Value   Monthly
Scenario Data    Quantity Norm.    Project Stats      Adjustments   Reports
Pricing Data     Charge Calcs      Job Metrics        Materials %
```

### Key Components

- **Data Extraction**: Pulls data from multiple Snowflake tables including:
  - Jobs
  - Work Orders
  - Sites
  - Accounts
  - Scenario Charges
  - UOM Ratios

- **Processing Pipeline**:
  1. `format_ov_input_data`: Formats and merges input data
  2. `job_aggregator`: Aggregates job-level metrics
  3. `ov_variable_creator`: Creates variables for OV calculation
  4. `ov_calculator`: Applies business logic and formulas
  5. `final_ov_formatter`: Produces final output format

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/order-value-calculator.git
cd order-value-calculator
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Dependencies

- snowflake-snowpark-python
- pandas
- numpy

## Usage

The procedure runs automatically in Snowflake and processes data month by month. It can be executed using:

```sql
CALL REVOPS.PRODUCTION.ORDER_VALUE_MONTHLY();
```

### Input Requirements

- Active sites with valid account information
- Job data with produced quantities
- Valid scenario charges and UOM configurations
- Date range for processing (automatically determined)

### Output

The procedure writes results to `REVOPS.PRODUCTION.ORDER_VALUE` table with the following key metrics:
- Order Value with and without outliers
- Adjusted Order Value
- Fill rates and percentages
- Project and job counts
- Material charge percentages

## Business Logic

### Order Value Calculation

The core calculation follows the formula:
```
Adjusted Order Value = A × (100% - B) + A × 10% + A × (B - 10%) × 5%
Where:
A = Order Value
B = Materials Percentage
```

### Outlier Detection

Uses Median Absolute Deviation (MAD) with:
- MAD factor: 5
- Lower quantile: 0.05
- Custom bounds for specific scenarios

## Development

### Project Structure
```
src/
├── core/
│   ├── calculations.py    # Core calculation logic
│   ├── aggregations.py    # Data aggregation functions
│   └── formatting.py      # Data formatting utilities
├── utils/
│   ├── outliers.py       # Outlier detection
│   └── conversions.py    # UOM conversion logic
└── config/
    └── settings.py       # Configuration parameters
```

### Testing

Execute tests using:
```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Run tests and ensure they pass
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Create a Pull Request
