#!/usr/bin/env python3
"""
Synthetic Transaction Data Generator
Generates synthetic transaction data for testing and development
"""

import os
import json
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import uuid
import sys

from faker import Faker

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from utils.logger import setup_logger
from utils.config import config

logger = setup_logger("data-generator")
fake = Faker()

def generate_bank_transaction(timestamp=None):
    """Generate a synthetic bank transaction"""
    status_options = ["completed", "pending", "failed", "declined"]
    card_types = ["visa", "mastercard", "amex", "discover"]
    
    amount = round(random.uniform(1.0, 1000.0), 2)
    
    # Higher amounts have a slightly higher chance of being declined
    status_weights = [0.85, 0.10, 0.03, 0.02]
    if amount > 500:
        status_weights = [0.75, 0.10, 0.05, 0.10]
    
    if timestamp is None:
        timestamp = int(datetime.now().timestamp() * 1000)
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "timestamp": timestamp,
        "amount": amount,
        "status": random.choices(status_options, weights=status_weights)[0],
        "source": "bank",
        "merchant": fake.company(),
        "card_type": random.choice(card_types),
        "location": fake.city(),
        "ip_address": fake.ipv4(),
        "device_id": f"device_{random.randint(1, 500)}"
    }
    
    return transaction

def generate_gateway_transaction(timestamp=None):
    """Generate a synthetic payment gateway transaction"""
    status_options = ["completed", "pending", "failed", "error"]
    card_types = ["visa", "mastercard", "amex", "discover"]
    gateway_names = ["Stripe", "PayPal", "Adyen", "Braintree", "Square"]
    
    amount = round(random.uniform(1.0, 1000.0), 2)
    
    # Gateway status probabilities
    status_weights = [0.88, 0.07, 0.03, 0.02]
    
    if timestamp is None:
        timestamp = int(datetime.now().timestamp() * 1000)
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "timestamp": timestamp,
        "amount": amount,
        "status": random.choices(status_options, weights=status_weights)[0],
        "source": random.choice(gateway_names),
        "merchant": fake.company(),
        "card_type": random.choice(card_types),
        "location": fake.city(),
        "ip_address": fake.ipv4(),
        "device_id": f"device_{random.randint(1, 500)}"
    }
    
    return transaction

def generate_processor_transaction(timestamp=None):
    """Generate a synthetic payment processor transaction"""
    status_options = ["completed", "pending", "rejected", "timeout"]
    card_types = ["visa", "mastercard", "amex", "discover"]
    processor_names = ["FirstData", "TSYS", "Worldpay", "Fiserv", "Elavon"]
    
    amount = round(random.uniform(1.0, 1000.0), 2)
    
    # Processor status probabilities
    status_weights = [0.90, 0.05, 0.03, 0.02]
    
    if timestamp is None:
        timestamp = int(datetime.now().timestamp() * 1000)
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "timestamp": timestamp,
        "amount": amount,
        "status": random.choices(status_options, weights=status_weights)[0],
        "source": random.choice(processor_names),
        "merchant": fake.company(),
        "card_type": random.choice(card_types),
        "location": fake.city(),
        "ip_address": fake.ipv4(),
        "device_id": f"device_{random.randint(1, 500)}"
    }
    
    return transaction

def generate_fraud_patterns(count=5):
    """Generate synthetic fraud patterns (bursts of transactions)"""
    
    fraud_patterns = []
    
    for _ in range(count):
        # Pick a random user
        user_id = f"user_{random.randint(1, 1000)}"
        
        # Pick a random timestamp in the last 24 hours
        now = datetime.now()
        start_time = now - timedelta(hours=24)
        random_time = start_time + timedelta(
            seconds=random.randint(0, int((now - start_time).total_seconds()))
        )
        
        # Create a burst of 5-10 transactions within 30 seconds
        burst_count = random.randint(5, 10)
        base_timestamp = int(random_time.timestamp() * 1000)
        
        for i in range(burst_count):
            # Transactions happen 1-5 seconds apart
            timestamp = base_timestamp + (i * random.randint(1000, 5000))
            
            # Generate a transaction
            if random.random() < 0.5:
                transaction = generate_bank_transaction(timestamp)
            elif random.random() < 0.75:
                transaction = generate_gateway_transaction(timestamp)
            else:
                transaction = generate_processor_transaction(timestamp)
            
            # Set the user ID to be the same for all transactions in this burst
            transaction["user_id"] = user_id
            
            fraud_patterns.append(transaction)
    
    return fraud_patterns

def generate_reconciliation_issues(count=10):
    """Generate synthetic reconciliation issues (missing or mismatched transactions)"""
    
    issues = []
    
    for _ in range(count):
        # Create a base transaction ID and amount
        transaction_id = str(uuid.uuid4())
        user_id = f"user_{random.randint(1, 1000)}"
        base_amount = round(random.uniform(10.0, 1000.0), 2)
        timestamp = int((datetime.now() - timedelta(hours=random.randint(1, 24))).timestamp() * 1000)
        
        # Issue type: missing transaction (not in all sources), amount mismatch, or status mismatch
        issue_type = random.choice(["missing", "amount", "status"])
        
        if issue_type == "missing":
            # Create transaction in bank and gateway, but not processor
            bank_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "completed",
                "source": "bank",
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            gateway_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "completed",
                "source": random.choice(["Stripe", "PayPal", "Adyen", "Braintree", "Square"]),
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            issues.append(("bank", bank_txn))
            issues.append(("gateway", gateway_txn))
            # Processor transaction is intentionally missing
            
        elif issue_type == "amount":
            # Create transactions in all sources but with different amounts
            bank_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "completed",
                "source": "bank",
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            gateway_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount + round(random.uniform(0.01, 5.0), 2),  # Slightly different amount
                "status": "completed",
                "source": random.choice(["Stripe", "PayPal", "Adyen", "Braintree", "Square"]),
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            processor_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,  # Same as bank
                "status": "completed",
                "source": random.choice(["FirstData", "TSYS", "Worldpay", "Fiserv", "Elavon"]),
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            issues.append(("bank", bank_txn))
            issues.append(("gateway", gateway_txn))
            issues.append(("processor", processor_txn))
            
        else:  # status mismatch
            # Create transactions in all sources but with different statuses
            bank_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "completed",
                "source": "bank",
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            gateway_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "pending",  # Different status
                "source": random.choice(["Stripe", "PayPal", "Adyen", "Braintree", "Square"]),
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            processor_txn = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": base_amount,
                "status": "completed",  # Same as bank
                "source": random.choice(["FirstData", "TSYS", "Worldpay", "Fiserv", "Elavon"]),
                "merchant": fake.company(),
                "card_type": random.choice(["visa", "mastercard", "amex", "discover"]),
                "location": fake.city(),
                "ip_address": fake.ipv4(),
                "device_id": f"device_{random.randint(1, 500)}"
            }
            
            issues.append(("bank", bank_txn))
            issues.append(("gateway", gateway_txn))
            issues.append(("processor", processor_txn))
    
    return issues

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--output-dir", type=str, default="data/synthetic",
                        help="Output directory for generated data (default: data/synthetic)")
    parser.add_argument("--bank-count", type=int, default=1000,
                        help="Number of bank transactions to generate (default: 1000)")
    parser.add_argument("--gateway-count", type=int, default=1000,
                        help="Number of gateway transactions to generate (default: 1000)")
    parser.add_argument("--processor-count", type=int, default=1000,
                        help="Number of processor transactions to generate (default: 1000)")
    parser.add_argument("--fraud-count", type=int, default=5,
                        help="Number of fraud patterns to generate (default: 5)")
    parser.add_argument("--reconciliation-issues", type=int, default=10,
                        help="Number of reconciliation issues to generate (default: 10)")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Generating synthetic transaction data...")
    
    # Generate bank transactions
    bank_transactions = [generate_bank_transaction() for _ in range(args.bank_count)]
    
    # Generate gateway transactions
    gateway_transactions = [generate_gateway_transaction() for _ in range(args.gateway_count)]
    
    # Generate processor transactions
    processor_transactions = [generate_processor_transaction() for _ in range(args.processor_count)]
    
    # Generate fraud patterns
    fraud_patterns = generate_fraud_patterns(args.fraud_count)
    
    # Generate reconciliation issues
    reconciliation_issues = generate_reconciliation_issues(args.reconciliation_issues)
    
    # Add fraud patterns and reconciliation issues to the appropriate lists
    for transaction in fraud_patterns:
        if transaction["source"] == "bank":
            bank_transactions.append(transaction)
        elif transaction["source"] in ["Stripe", "PayPal", "Adyen", "Braintree", "Square"]:
            gateway_transactions.append(transaction)
        else:
            processor_transactions.append(transaction)
    
    for source, transaction in reconciliation_issues:
        if source == "bank":
            bank_transactions.append(transaction)
        elif source == "gateway":
            gateway_transactions.append(transaction)
        else:
            processor_transactions.append(transaction)
    
    # Write to files
    bank_output = output_dir / "bank_transactions.json"
    gateway_output = output_dir / "gateway_transactions.json"
    processor_output = output_dir / "processor_transactions.json"
    
    with open(bank_output, "w") as f:
        for transaction in bank_transactions:
            f.write(json.dumps(transaction) + "\n")
    
    with open(gateway_output, "w") as f:
        for transaction in gateway_transactions:
            f.write(json.dumps(transaction) + "\n")
    
    with open(processor_output, "w") as f:
        for transaction in processor_transactions:
            f.write(json.dumps(transaction) + "\n")
    
    logger.info(f"Generated {len(bank_transactions)} bank transactions")
    logger.info(f"Generated {len(gateway_transactions)} gateway transactions")
    logger.info(f"Generated {len(processor_transactions)} processor transactions")
    logger.info(f"Generated {args.fraud_count} fraud patterns")
    logger.info(f"Generated {args.reconciliation_issues} reconciliation issues")
    logger.info(f"Output written to {output_dir}")

if __name__ == "__main__":
    main() 