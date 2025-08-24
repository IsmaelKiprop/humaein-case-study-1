#!/usr/bin/env python3
"""
Healthcare Claim Resubmission Pipeline
=====================================

This pipeline ingests insurance claim data from multiple EMR sources,
normalizes the data, and identifies claims eligible for resubmission
based on business rules and denial reasons.

Author: Healthcare Data Engineering Team
Date: 2025
"""

import json
import csv
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ClaimRecord:
    """Standardized claim record structure."""
    claim_id: str
    patient_id: Optional[str]
    procedure_code: str
    denial_reason: Optional[str]
    submitted_at: datetime
    status: str
    source: str
    eligibility_score: float = 0.0
    resubmission_eligible: bool = False
    business_rule_flags: List[str] = None
    
    def __post_init__(self):
        if self.business_rule_flags is None:
            self.business_rule_flags = []


class ClaimEligibilityEngine:
    """Engine for determining claim resubmission eligibility."""
    
    # Retryable denial reasons (from requirements)
    RETRYABLE_REASONS = {
        'missing modifier',
        'incorrect npi',
        'prior auth required'
    }
    
    # Known non-retryable reasons (from requirements)
    NON_RETRYABLE_REASONS = {
        'authorization expired',
        'incorrect provider type'
    }
    
    # Ambiguous reasons that need LLM classification
    AMBIGUOUS_REASONS = {
        'incorrect procedure',
        'form incomplete',
        'not billable'
    }
    
    # Procedure codes that are more likely to be resubmitted successfully
    HIGH_SUCCESS_PROCEDURES = {'99213', '99214', '99215', '99381', '99401'}
    
    @staticmethod
    def analyze_denial_reason(denial_reason: Optional[str]) -> Dict[str, Any]:
        """
        Analyze denial reason and return eligibility assessment.
        
        Args:
            denial_reason: The denial reason from the claim
            
        Returns:
            Dictionary with eligibility analysis
        """
        if not denial_reason or denial_reason.lower() == 'none':
            return {
                'eligible': False,
                'confidence': 1.0,
                'reason': 'No denial reason provided'
            }
        
        denial_lower = denial_reason.lower().strip()
        
        # Check retryable reasons
        if denial_lower in ClaimEligibilityEngine.RETRYABLE_REASONS:
            return {
                'eligible': True,
                'confidence': 0.9,
                'reason': f'Retryable reason: {denial_reason}'
            }
        
        # Check non-retryable reasons
        if denial_lower in ClaimEligibilityEngine.NON_RETRYABLE_REASONS:
            return {
                'eligible': False,
                'confidence': 0.9,
                'reason': f'Non-retryable reason: {denial_reason}'
            }
        
        # Check ambiguous reasons that need LLM classification
        if denial_lower in ClaimEligibilityEngine.AMBIGUOUS_REASONS:
            return ClaimEligibilityEngine._apply_llm_classification(denial_lower)
        
        # Apply inferable logic for ambiguous cases
        return ClaimEligibilityEngine._apply_inferable_logic(denial_lower)
    
    @staticmethod
    def _apply_llm_classification(denial_reason: str) -> Dict[str, Any]:
        """
        Mock LLM classification for ambiguous denial reasons.
        This simulates an LLM-based classifier for complex cases.
        
        Args:
            denial_reason: Lowercase denial reason
            
        Returns:
            Dictionary with eligibility analysis
        """
        # Mock LLM classification logic
        llm_classifications = {
            'incorrect procedure': {
                'eligible': False,
                'confidence': 0.8,
                'reason': 'LLM classified: Incorrect procedure - not retryable'
            },
            'form incomplete': {
                'eligible': True,
                'confidence': 0.7,
                'reason': 'LLM classified: Form incomplete - can be retried with corrections'
            },
            'not billable': {
                'eligible': False,
                'confidence': 0.9,
                'reason': 'LLM classified: Not billable - fundamental issue'
            }
        }
        
        return llm_classifications.get(denial_reason, {
            'eligible': False,
            'confidence': 0.5,
            'reason': f'LLM classified: Unknown reason "{denial_reason}" - manual review needed'
        })
    
    @staticmethod
    def _apply_inferable_logic(denial_reason: str) -> Dict[str, Any]:
        """
        Apply inferable logic for ambiguous denial reasons.
        This simulates LLM-based analysis for complex cases.
        
        Args:
            denial_reason: Lowercase denial reason
            
        Returns:
            Dictionary with eligibility analysis
        """
        # Keywords that suggest resubmission might be successful
        positive_keywords = ['missing', 'incorrect', 'expired', 'required', 'incomplete']
        negative_keywords = ['not covered', 'duplicate', 'invalid', 'fraud', 'experimental']
        
        positive_score = sum(1 for keyword in positive_keywords if keyword in denial_reason)
        negative_score = sum(1 for keyword in negative_keywords if keyword in denial_reason)
        
        if positive_score > negative_score:
            return {
                'eligible': True,
                'confidence': 0.7,
                'reason': f'Inferable logic: Positive keywords detected in "{denial_reason}"'
            }
        elif negative_score > positive_score:
            return {
                'eligible': False,
                'confidence': 0.7,
                'reason': f'Inferable logic: Negative keywords detected in "{denial_reason}"'
            }
        else:
            return {
                'eligible': False,
                'confidence': 0.5,
                'reason': f'Ambiguous case: "{denial_reason}" - manual review recommended'
            }
    
    @staticmethod
    def calculate_eligibility_score(claim: ClaimRecord) -> float:
        """
        Calculate a numerical eligibility score for the claim.
        
        Args:
            claim: The claim record to analyze
            
        Returns:
            Float score between 0.0 and 1.0
        """
        score = 0.0
        
        # Base score from denial reason analysis
        analysis = ClaimEligibilityEngine.analyze_denial_reason(claim.denial_reason)
        if analysis['eligible']:
            score += analysis['confidence'] * 0.6
        
        # Bonus for high-success procedure codes
        if claim.procedure_code in ClaimEligibilityEngine.HIGH_SUCCESS_PROCEDURES:
            score += 0.2
        
        # Bonus for having patient ID (complete data)
        if claim.patient_id:
            score += 0.1
        
        # Bonus for recent claims (within 30 days)
        days_old = (datetime.now() - claim.submitted_at).days
        if days_old <= 30:
            score += 0.1
        
        return min(score, 1.0)
    
    @staticmethod
    def check_resubmission_eligibility(claim: ClaimRecord, reference_date: datetime = None) -> Dict[str, Any]:
        """
        Check if a claim meets all resubmission eligibility criteria.
        
        Args:
            claim: The claim record to analyze
            reference_date: Reference date for 7-day calculation (default: 2025-07-30)
            
        Returns:
            Dictionary with eligibility assessment
        """
        if reference_date is None:
            reference_date = datetime(2025, 7, 30)  # Default reference date from requirements
        
        # Check all eligibility criteria
        checks = {
            'status_denied': claim.status == 'denied',
            'patient_id_not_null': claim.patient_id is not None and claim.patient_id.strip() != '',
            'submitted_more_than_7_days_ago': (reference_date - claim.submitted_at).days > 7,
            'denial_reason_eligible': False,
            'denial_reason_analysis': None
        }
        
        # Analyze denial reason
        denial_analysis = ClaimEligibilityEngine.analyze_denial_reason(claim.denial_reason)
        checks['denial_reason_analysis'] = denial_analysis
        checks['denial_reason_eligible'] = denial_analysis['eligible']
        
        # All criteria must be met
        all_criteria_met = all([
            checks['status_denied'],
            checks['patient_id_not_null'],
            checks['submitted_more_than_7_days_ago'],
            checks['denial_reason_eligible']
        ])
        
        return {
            'eligible': all_criteria_met,
            'checks': checks,
            'reference_date': reference_date.isoformat(),
            'days_since_submission': (reference_date - claim.submitted_at).days
        }


class DataIngestionPipeline:
    """Pipeline for ingesting and normalizing data from multiple EMR sources."""
    
    def __init__(self):
        self.claims: List[ClaimRecord] = []
        self.eligibility_engine = ClaimEligibilityEngine()
    
    def ingest_csv_source(self, file_path: str) -> List[ClaimRecord]:
        """
        Ingest data from CSV source (EMR Alpha).
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List of normalized ClaimRecord objects
        """
        logger.info(f"Ingesting CSV data from {file_path}")
        claims = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        # Parse and validate data
                        claim_id = row.get('claim_id', '').strip()
                        patient_id = row.get('patient_id', '').strip() or None
                        procedure_code = row.get('procedure_code', '').strip()
                        denial_reason = row.get('denial_reason', '').strip() or None
                        submitted_at_str = row.get('submitted_at', '').strip()
                        status = row.get('status', '').strip().lower()
                        
                        # Validate required fields
                        if not claim_id or not procedure_code:
                            logger.warning(f"Row {row_num}: Missing required fields (claim_id or procedure_code)")
                            continue
                        
                        # Parse date
                        try:
                            submitted_at = datetime.strptime(submitted_at_str, '%Y-%m-%d')
                        except ValueError:
                            logger.warning(f"Row {row_num}: Invalid date format: {submitted_at_str}")
                            submitted_at = datetime.now()
                        
                        # Create normalized record
                        claim = ClaimRecord(
                            claim_id=claim_id,
                            patient_id=patient_id,
                            procedure_code=procedure_code,
                            denial_reason=denial_reason,
                            submitted_at=submitted_at,
                            status=status,
                            source='emr_alpha'
                        )
                        
                        claims.append(claim)
                        
                    except Exception as e:
                        logger.error(f"Row {row_num}: Error processing row: {e}")
                        continue
                        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            raise
        
        logger.info(f"Successfully ingested {len(claims)} claims from CSV source")
        return claims
    
    def ingest_json_source(self, file_path: str) -> List[ClaimRecord]:
        """
        Ingest data from JSON source (EMR Beta).
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List of normalized ClaimRecord objects
        """
        logger.info(f"Ingesting JSON data from {file_path}")
        claims = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            if not isinstance(data, list):
                logger.error("JSON data is not a list")
                raise ValueError("Invalid JSON format: expected list")
            
            for item_num, item in enumerate(data, start=1):
                try:
                    # Extract and validate data
                    claim_id = item.get('id', '').strip()
                    patient_id = item.get('member', '').strip() or None
                    procedure_code = item.get('code', '').strip()
                    denial_reason = item.get('error_msg')
                    if denial_reason is not None:
                        denial_reason = denial_reason.strip()
                    submitted_at_str = item.get('date', '').strip()
                    status = item.get('status', '').strip().lower()
                    
                    # Validate required fields
                    if not claim_id or not procedure_code:
                        logger.warning(f"Item {item_num}: Missing required fields (id or code)")
                        continue
                    
                    # Parse date (handle ISO format)
                    try:
                        if 'T' in submitted_at_str:
                            submitted_at = datetime.fromisoformat(submitted_at_str.replace('Z', '+00:00'))
                        else:
                            submitted_at = datetime.strptime(submitted_at_str, '%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"Item {item_num}: Invalid date format: {submitted_at_str}")
                        submitted_at = datetime.now()
                    
                    # Create normalized record
                    claim = ClaimRecord(
                        claim_id=claim_id,
                        patient_id=patient_id,
                        procedure_code=procedure_code,
                        denial_reason=denial_reason,
                        submitted_at=submitted_at,
                        status=status,
                        source='emr_beta'
                    )
                    
                    claims.append(claim)
                    
                except Exception as e:
                    logger.error(f"Item {item_num}: Error processing item: {e}")
                    continue
                    
        except FileNotFoundError:
            logger.error(f"JSON file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading JSON file {file_path}: {e}")
            raise
        
        logger.info(f"Successfully ingested {len(claims)} claims from JSON source")
        return claims
    
    def process_claims(self, csv_file: str, json_file: str) -> List[ClaimRecord]:
        """
        Process all claims from both sources.
        
        Args:
            csv_file: Path to CSV file
            json_file: Path to JSON file
            
        Returns:
            List of all processed ClaimRecord objects
        """
        logger.info("Starting claim processing pipeline")
        
        # Ingest from both sources
        csv_claims = self.ingest_csv_source(csv_file)
        json_claims = self.ingest_json_source(json_file)
        
        # Combine all claims
        all_claims = csv_claims + json_claims
        
        # Apply eligibility analysis
        for claim in all_claims:
            # Calculate eligibility score
            claim.eligibility_score = self.eligibility_engine.calculate_eligibility_score(claim)
            
            # Check resubmission eligibility using all criteria
            eligibility_check = self.eligibility_engine.check_resubmission_eligibility(claim)
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
        
        logger.info(f"Processed {len(all_claims)} total claims")
        return all_claims
    
    def generate_output(self, claims: List[ClaimRecord], output_file: str = 'resubmission_candidates.json'):
        """
        Generate output file with resubmission candidates.
        
        Args:
            claims: List of processed claims
            output_file: Output file path
        """
        logger.info(f"Generating output file: {output_file}")
        
        # Filter eligible claims
        eligible_claims = [claim for claim in claims if claim.resubmission_eligible]
        
        # Convert to dictionary format for JSON serialization
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_claims_processed': len(claims),
                'eligible_claims_count': len(eligible_claims),
                'eligibility_rate': len(eligible_claims) / len(claims) if claims else 0
            },
            'resubmission_candidates': [
                {
                    'claim_id': claim.claim_id,
                    'patient_id': claim.patient_id,
                    'procedure_code': claim.procedure_code,
                    'denial_reason': claim.denial_reason,
                    'submitted_at': claim.submitted_at.isoformat(),
                    'source': claim.source,
                    'eligibility_score': round(claim.eligibility_score, 3),
                    'business_rule_flags': claim.business_rule_flags
                }
                for claim in eligible_claims
            ]
        }
        
        # Write to file
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(output_data, file, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully wrote {len(eligible_claims)} eligible claims to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing output file {output_file}: {e}")
            raise
    
    def generate_metrics(self, claims: List[ClaimRecord]) -> Dict[str, Any]:
        """
        Generate pipeline metrics and statistics.
        
        Args:
            claims: List of processed claims
            
        Returns:
            Dictionary with metrics
        """
        if not claims:
            return {}
        
        total_claims = len(claims)
        denied_claims = [c for c in claims if c.status == 'denied']
        eligible_claims = [c for c in claims if c.resubmission_eligible]
        
        # Source breakdown
        source_counts = {}
        for claim in claims:
            source_counts[claim.source] = source_counts.get(claim.source, 0) + 1
        
        # Denial reason analysis
        denial_reasons = {}
        for claim in denied_claims:
            reason = claim.denial_reason or 'Unknown'
            denial_reasons[reason] = denial_reasons.get(reason, 0) + 1
        
        # Procedure code analysis
        procedure_counts = {}
        for claim in claims:
            procedure_counts[claim.procedure_code] = procedure_counts.get(claim.procedure_code, 0) + 1
        
        metrics = {
            'total_claims_processed': total_claims,
            'denied_claims_count': len(denied_claims),
            'eligible_for_resubmission': len(eligible_claims),
            'eligibility_rate': len(eligible_claims) / len(denied_claims) if denied_claims else 0,
            'source_breakdown': source_counts,
            'top_denial_reasons': dict(sorted(denial_reasons.items(), key=lambda x: x[1], reverse=True)[:5]),
            'top_procedure_codes': dict(sorted(procedure_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            'average_eligibility_score': sum(c.eligibility_score for c in claims) / total_claims
        }
        
        logger.info("Pipeline metrics generated")
        return metrics
    
    def export_rejected_claims(self, claims: List[ClaimRecord], output_file: str = 'rejected_claims.json'):
        """
        Export rejected claims to a separate file for analysis.
        
        Args:
            claims: List of processed claims
            output_file: Output file path for rejected claims
        """
        logger.info(f"Exporting rejected claims to {output_file}")
        
        # Filter rejected claims (not eligible for resubmission)
        rejected_claims = [claim for claim in claims if not claim.resubmission_eligible]
        
        # Group rejected claims by reason
        rejection_reasons = {}
        for claim in rejected_claims:
            for flag in claim.business_rule_flags:
                if flag not in rejection_reasons:
                    rejection_reasons[flag] = []
                rejection_reasons[flag].append({
                    'claim_id': claim.claim_id,
                    'patient_id': claim.patient_id,
                    'procedure_code': claim.procedure_code,
                    'denial_reason': claim.denial_reason,
                    'submitted_at': claim.submitted_at.isoformat(),
                    'source': claim.source,
                    'status': claim.status
                })
        
        # Prepare output data
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_claims_processed': len(claims),
                'rejected_claims_count': len(rejected_claims),
                'rejection_rate': len(rejected_claims) / len(claims) if claims else 0
            },
            'rejection_summary': {
                reason: len(claims_list) for reason, claims_list in rejection_reasons.items()
            },
            'rejected_claims_by_reason': rejection_reasons
        }
        
        # Write to file
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(output_data, file, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully exported {len(rejected_claims)} rejected claims to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing rejected claims file {output_file}: {e}")
            raise


def main():
    """Main pipeline execution function."""
    logger.info("Starting Healthcare Claim Resubmission Pipeline")
    
    try:
        # Initialize pipeline
        pipeline = DataIngestionPipeline()
        
        # Process claims from both sources
        claims = pipeline.process_claims('emr_alpha.csv', 'emr_beta.json')
        
        # Generate output
        pipeline.generate_output(claims)
        
        # Export rejected claims
        pipeline.export_rejected_claims(claims)
        
        # Generate and log metrics
        metrics = pipeline.generate_metrics(claims)
        
        logger.info("Pipeline execution completed successfully")
        logger.info(f"Metrics: {json.dumps(metrics, indent=2)}")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
