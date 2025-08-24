#!/usr/bin/env python3
"""
FastAPI Server for Healthcare Claim Resubmission Pipeline
========================================================

This module provides REST API endpoints for the claim resubmission pipeline,
allowing programmatic access to claim processing and analysis functionality.

Author: Healthcare Data Engineering Team
Date: 2025
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import logging
from datetime import datetime
import tempfile
import os

from claim_pipeline import DataIngestionPipeline, ClaimRecord

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Healthcare Claim Resubmission API",
    description="API for processing healthcare claims and identifying resubmission candidates",
    version="1.0.0"
)

# Initialize pipeline
pipeline = DataIngestionPipeline()


class ClaimResponse(BaseModel):
    """Response model for claim data."""
    claim_id: str
    patient_id: Optional[str]
    procedure_code: str
    denial_reason: Optional[str]
    submitted_at: str
    source: str
    eligibility_score: float
    resubmission_eligible: bool
    business_rule_flags: List[str]


class PipelineResponse(BaseModel):
    """Response model for pipeline processing results."""
    metadata: Dict[str, Any]
    resubmission_candidates: List[ClaimResponse]
    metrics: Dict[str, Any]


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: str
    version: str


@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint with health check."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="1.0.0"
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="1.0.0"
    )


@app.post("/process-claims", response_model=PipelineResponse)
async def process_claims(
    csv_file: UploadFile = File(...),
    json_file: UploadFile = File(...)
):
    """
    Process claims from uploaded CSV and JSON files.
    
    Args:
        csv_file: CSV file containing claim data (EMR Alpha format)
        json_file: JSON file containing claim data (EMR Beta format)
    
    Returns:
        PipelineResponse with processed claims and metrics
    """
    try:
        logger.info("Processing claims via API")
        
        # Save uploaded files temporarily
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as csv_temp:
            csv_content = await csv_file.read()
            csv_temp.write(csv_content)
            csv_temp_path = csv_temp.name
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as json_temp:
            json_content = await json_file.read()
            json_temp.write(json_content)
            json_temp_path = json_temp.name
        
        try:
            # Process claims
            claims = pipeline.process_claims(csv_temp_path, json_temp_path)
            
            # Generate metrics
            metrics = pipeline.generate_metrics(claims)
            
            # Filter eligible claims
            eligible_claims = [claim for claim in claims if claim.resubmission_eligible]
            
            # Prepare response
            response_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_claims_processed': len(claims),
                    'eligible_claims_count': len(eligible_claims),
                    'eligibility_rate': len(eligible_claims) / len(claims) if claims else 0
                },
                'resubmission_candidates': [
                    ClaimResponse(
                        claim_id=claim.claim_id,
                        patient_id=claim.patient_id,
                        procedure_code=claim.procedure_code,
                        denial_reason=claim.denial_reason,
                        submitted_at=claim.submitted_at.isoformat(),
                        source=claim.source,
                        eligibility_score=round(claim.eligibility_score, 3),
                        resubmission_eligible=claim.resubmission_eligible,
                        business_rule_flags=claim.business_rule_flags
                    )
                    for claim in eligible_claims
                ],
                'metrics': metrics
            }
            
            logger.info(f"API processed {len(claims)} claims, {len(eligible_claims)} eligible")
            return PipelineResponse(**response_data)
            
        finally:
            # Clean up temporary files
            os.unlink(csv_temp_path)
            os.unlink(json_temp_path)
            
    except Exception as e:
        logger.error(f"Error processing claims via API: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analyze-claim", response_model=Dict[str, Any])
async def analyze_single_claim(
    claim_id: str,
    patient_id: Optional[str] = None,
    procedure_code: str = None,
    denial_reason: Optional[str] = None,
    submitted_at: str = None,
    status: str = "denied",
    source: str = "api"
):
    """
    Analyze a single claim for resubmission eligibility.
    
    Args:
        claim_id: Unique claim identifier
        patient_id: Patient identifier (optional)
        procedure_code: Medical procedure code
        denial_reason: Reason for denial (optional)
        submitted_at: Submission date (ISO format)
        status: Claim status (default: denied)
        source: Data source identifier
    
    Returns:
        Analysis results for the single claim
    """
    try:
        logger.info(f"Analyzing single claim: {claim_id}")
        
        # Validate required fields
        if not claim_id or not procedure_code or not submitted_at:
            raise HTTPException(
                status_code=400,
                detail="claim_id, procedure_code, and submitted_at are required"
            )
        
        # Parse date
        try:
            parsed_date = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)"
            )
        
        # Create claim record
        claim = ClaimRecord(
            claim_id=claim_id,
            patient_id=patient_id,
            procedure_code=procedure_code,
            denial_reason=denial_reason,
            submitted_at=parsed_date,
            status=status.lower(),
            source=source
        )
        
        # Analyze claim
        claim.eligibility_score = pipeline.eligibility_engine.calculate_eligibility_score(claim)
        eligibility_check = pipeline.eligibility_engine.check_resubmission_eligibility(claim)
        claim.resubmission_eligible = eligibility_check['eligible']
        
        # Add business rule flags based on eligibility check
        checks = eligibility_check['checks']
        if not checks['status_denied']:
            claim.business_rule_flags.append('Claim not denied')
        if not checks['patient_id_not_null']:
            claim.business_rule_flags.append('Missing patient ID')
        if not checks['submitted_more_than_7_days_ago']:
            claim.business_rule_flags.append(f'Claim too recent ({eligibility_check["days_since_submission"]} days old)')
        if checks['denial_reason_analysis']:
            claim.business_rule_flags.append(checks['denial_reason_analysis']['reason'])
        
        # Prepare response
        response = {
            'claim_id': claim.claim_id,
            'patient_id': claim.patient_id,
            'procedure_code': claim.procedure_code,
            'denial_reason': claim.denial_reason,
            'submitted_at': claim.submitted_at.isoformat(),
            'status': claim.status,
            'source': claim.source,
            'eligibility_score': round(claim.eligibility_score, 3),
            'resubmission_eligible': claim.resubmission_eligible,
            'business_rule_flags': claim.business_rule_flags,
            'eligibility_check': eligibility_check
        }
        
        logger.info(f"Claim {claim_id} analysis completed")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing claim {claim_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics", response_model=Dict[str, Any])
async def get_pipeline_metrics():
    """
    Get current pipeline metrics and statistics.
    
    Returns:
        Dictionary containing pipeline metrics
    """
    try:
        logger.info("Retrieving pipeline metrics")
        
        # For demo purposes, process the sample files if they exist
        if os.path.exists('emr_alpha.csv') and os.path.exists('emr_beta.json'):
            claims = pipeline.process_claims('emr_alpha.csv', 'emr_beta.json')
            metrics = pipeline.generate_metrics(claims)
        else:
            metrics = {
                'message': 'No sample data available. Upload files to generate metrics.',
                'total_claims_processed': 0,
                'denied_claims_count': 0,
                'eligible_for_resubmission': 0,
                'eligibility_rate': 0.0
            }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/business-rules", response_model=Dict[str, Any])
async def get_business_rules():
    """
    Get current business rules for claim eligibility.
    
    Returns:
        Dictionary containing business rules
    """
    try:
        logger.info("Retrieving business rules")
        
        rules = {
            'retryable_reasons': list(pipeline.eligibility_engine.RETRYABLE_REASONS),
            'non_retryable_reasons': list(pipeline.eligibility_engine.NON_RETRYABLE_REASONS),
            'ambiguous_reasons': list(pipeline.eligibility_engine.AMBIGUOUS_REASONS),
            'high_success_procedures': list(pipeline.eligibility_engine.HIGH_SUCCESS_PROCEDURES),
            'eligibility_criteria': {
                'base_score_weight': 0.6,
                'procedure_bonus': 0.2,
                'patient_id_bonus': 0.1,
                'recent_claim_bonus': 0.1,
                'recent_claim_threshold_days': 30
            }
        }
        
        return rules
        
    except Exception as e:
        logger.error(f"Error retrieving business rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting FastAPI server for Healthcare Claim Resubmission Pipeline")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
