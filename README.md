# Healthcare Claim Resubmission Pipeline

A robust data engineering pipeline for processing healthcare insurance claims from multiple Electronic Medical Records (EMR) systems, normalizing the data, and identifying claims eligible for resubmission based on business rules and denial reasons.

## Features

- **Multi-source Data Ingestion**: Handles CSV and JSON data from different EMR systems
- **Data Normalization**: Unifies schema from various sources into a common format
- **Intelligent Eligibility Analysis**: Combines deterministic and inferable logic for claim resubmission eligibility
- **Comprehensive Logging**: Detailed logging with metrics and error handling
- **REST API**: FastAPI endpoints for programmatic access
- **Business Rule Engine**: Configurable rules for eligibility determination

## Project Structure

```
humaein-case-study-1/
├── claim_pipeline.py      # Main pipeline implementation
├── api_server.py          # FastAPI server with REST endpoints
├── emr_alpha.csv          # Sample CSV data (EMR Alpha)
├── emr_beta.json          # Sample JSON data (EMR Beta)
├── requirements.txt       # Python dependencies
├── README.md             # This file
└── resubmission_candidates.json  # Output file (generated)
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd humaein-case-study-1
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Pipeline

Run the main pipeline to process the sample data:

```bash
python claim_pipeline.py
```

This will:
- Ingest data from `emr_alpha.csv` and `emr_beta.json`
- Normalize and analyze all claims
- Generate `resubmission_candidates.json` with eligible claims
- Output metrics and logging information

### API Server

Start the FastAPI server:

```bash
python api_server.py
```

The server will be available at `http://localhost:8000`

#### API Endpoints

1. **Health Check**: `GET /health`
   - Returns server status and version

2. **Process Claims**: `POST /process-claims`
   - Upload CSV and JSON files for processing
   - Returns processed claims and metrics

3. **Analyze Single Claim**: `GET /analyze-claim`
   - Analyze individual claim for eligibility
   - Parameters: `claim_id`, `procedure_code`, `submitted_at`, etc.

4. **Get Metrics**: `GET /metrics`
   - Returns current pipeline metrics and statistics

5. **Get Business Rules**: `GET /business-rules`
   - Returns current business rules configuration

#### API Documentation

Once the server is running, visit:
- **Interactive API Docs**: `http://localhost:8000/docs`
- **ReDoc Documentation**: `http://localhost:8000/redoc`

## Data Sources

### EMR Alpha (CSV Format)
```csv
claim_id,patient_id,procedure_code,denial_reason,submitted_at,status
A123,P001,99213,Missing modifier,2025-07-01,denied
A124,P002,99214,Incorrect NPI,2025-07-10,denied
```

### EMR Beta (JSON Format)
```json
[
  {
    "id": "B987",
    "member": "P010",
    "code": "99213",
    "error_msg": "Incorrect provider type",
    "date": "2025-07-03T00:00:00",
    "status": "denied"
  }
]
```

## Business Rules

### Resubmission Eligibility Criteria
A claim is eligible for resubmission if **ALL** of the following are true:

1. **Status is denied**
2. **Patient ID is not null**
3. **Claim was submitted more than 7 days ago** (reference date: 2025-07-30)
4. **Denial reason is retryable** (see below)

### Retryable Denial Reasons
- Missing modifier
- Incorrect NPI
- Prior auth required

### Known Non-Retryable Reasons
- Authorization expired
- Incorrect provider type

### Ambiguous Reasons (LLM Classification)
- Incorrect procedure → **Not retryable**
- Form incomplete → **Retryable**
- Not billable → **Not retryable**
- Null/None → **Not retryable**

### Eligibility Scoring
The pipeline calculates an eligibility score (0.0-1.0) based on:
- **Base Score (60%)**: Denial reason analysis
- **Procedure Bonus (20%)**: High-success procedure codes
- **Data Completeness (10%)**: Having patient ID
- **Recency Bonus (10%)**: Claims within 30 days

## Output Format

The pipeline generates `resubmission_candidates.json` with the following structure:

```json
{
  "metadata": {
    "generated_at": "2025-01-27T10:30:00",
    "total_claims_processed": 9,
    "eligible_claims_count": 6,
    "eligibility_rate": 0.667
  },
  "resubmission_candidates": [
    {
      "claim_id": "A123",
      "patient_id": "P001",
      "procedure_code": "99213",
      "denial_reason": "Missing modifier",
      "submitted_at": "2025-07-01T00:00:00",
      "source": "emr_alpha",
      "eligibility_score": 0.9,
      "business_rule_flags": ["Deterministic rule: Missing modifier"]
    }
  ]
}
```

## Logging

The pipeline generates detailed logs in `pipeline.log` including:
- Data ingestion progress
- Processing statistics
- Error handling and validation
- Business rule application
- Output generation metrics

## Error Handling

The pipeline includes comprehensive error handling for:
- **File Not Found**: Missing input files
- **Invalid Data Formats**: Malformed CSV/JSON
- **Missing Required Fields**: Incomplete claim records
- **Date Parsing Errors**: Invalid date formats
- **API Errors**: HTTP exceptions and validation

## Example Usage

### Process Sample Data
```bash
python claim_pipeline.py
```

### Run Test Suite
```bash
python test_pipeline.py
```

### Start API Server
```bash
python api_server.py
```

### Analyze Single Claim via API
```bash
curl "http://localhost:8000/analyze-claim?claim_id=TEST123&procedure_code=99213&denial_reason=Missing%20modifier&submitted_at=2025-07-01T00:00:00"
```

### Get Pipeline Metrics
```bash
curl "http://localhost:8000/metrics"
```

## Performance Considerations

- **Memory Efficient**: Processes claims in batches
- **Error Resilient**: Continues processing on individual record errors
- **Scalable**: Modular design allows for easy extension
- **Configurable**: Business rules can be easily modified

## Future Enhancements

- **Database Integration**: Store processed claims in database
- **Real-time Processing**: Stream processing capabilities
- **Machine Learning**: Enhanced eligibility prediction
- **Dashboard**: Web-based monitoring interface
- **Batch Processing**: Support for larger datasets
- **Additional EMR Sources**: Extend to more data formats

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues, please contact the Healthcare Data Engineering Team.