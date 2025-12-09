#!/usr/bin/env python3
"""
CDC Pipeline Monitor and Data Tracker

This script allows you to:
1. Insert test data into PostgreSQL
2. Monitor data flow through the CDC pipeline
3. Track inserts, updates, and deletes across all stages
4. Report on data movement from PostgreSQL → Kafka → ClickHouse Staging → ClickHouse DW

Usage:
    python pipeline_monitor.py --insert-customer
    python pipeline_monitor.py --monitor-kafka
    python pipeline_monitor.py --check-staging
    python pipeline_monitor.py --check-dw
    python pipeline_monitor.py --track-all
"""

import argparse
import subprocess
import time
import json
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
POSTGRES_CONTAINER = "postgres"
POSTGRES_DB = "northwind"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_PORT = "15432"

KAFKA_CONTAINER = "kafka"
CLICKHOUSE_CONTAINER = "clickhouse1"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "123456"


def run_docker_command(container: str, command: str, capture_output: bool = True) -> str:
    """Execute a command inside a Docker container."""
    full_command = f"docker exec {container} {command}"
    try:
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=capture_output,
            text=True,
            check=False
        )
        if capture_output:
            return result.stdout.strip()
        return ""
    except Exception as e:
        return f"Error: {str(e)}"


def insert_customer(customer_id: str, company_name: str, contact_name: str = None, 
                   city: str = None, country: str = None) -> bool:
    """Insert a new customer into PostgreSQL."""
    contact_name = contact_name or f"Contact {customer_id}"
    city = city or "Unknown"
    country = country or "Unknown"
    
    sql = f"""
    INSERT INTO customers (customer_id, company_name, contact_name, city, country)
    VALUES ('{customer_id}', '{company_name}', '{contact_name}', '{city}', '{country}');
    """
    
    command = f'psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{sql}"'
    result = run_docker_command(POSTGRES_CONTAINER, command)
    
    if "INSERT" in result or "ERROR" not in result:
        print(f"✅ Inserted customer: {customer_id} - {company_name}")
        return True
    else:
        print(f"❌ Failed to insert customer: {result}")
        return False


def insert_order(order_id: int, customer_id: str, employee_id: int = 1, 
                product_id: int = 1, quantity: int = 10, unit_price: float = 25.50) -> bool:
    """Insert a new order and order detail into PostgreSQL."""
    order_date = datetime.now().strftime("%Y-%m-%d")
    required_date = datetime.now().strftime("%Y-%m-%d")
    
    # Insert order
    order_sql = f"""
    INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, ship_via, freight)
    VALUES ({order_id}, '{customer_id}', {employee_id}, '{order_date}', '{required_date}', 1, 10.50);
    """
    
    # Insert order detail
    detail_sql = f"""
    INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount)
    VALUES ({order_id}, {product_id}, {quantity}, {unit_price}, 0.0);
    """
    
    order_result = run_docker_command(POSTGRES_CONTAINER, 
        f'psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{order_sql}"')
    detail_result = run_docker_command(POSTGRES_CONTAINER,
        f'psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{detail_sql}"')
    
    if "INSERT" in order_result and "INSERT" in detail_result:
        print(f"✅ Inserted order: {order_id} for customer {customer_id}")
        return True
    else:
        print(f"❌ Failed to insert order: {order_result} / {detail_result}")
        return False


def update_customer(customer_id: str, new_company_name: str) -> bool:
    """Update a customer in PostgreSQL."""
    sql = f"""
    UPDATE customers 
    SET company_name = '{new_company_name}'
    WHERE customer_id = '{customer_id}';
    """
    
    command = f'psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{sql}"'
    result = run_docker_command(POSTGRES_CONTAINER, command)
    
    if "UPDATE" in result:
        print(f"✅ Updated customer: {customer_id} -> {new_company_name}")
        return True
    else:
        print(f"❌ Failed to update customer: {result}")
        return False


def delete_customer(customer_id: str) -> bool:
    """Delete a customer from PostgreSQL."""
    sql = f"""
    DELETE FROM customers 
    WHERE customer_id = '{customer_id}';
    """
    
    command = f'psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{sql}"'
    result = run_docker_command(POSTGRES_CONTAINER, command)
    
    if "DELETE" in result:
        print(f"✅ Deleted customer: {customer_id}")
        return True
    else:
        print(f"❌ Failed to delete customer: {result}")
        return False


def check_kafka_topics() -> List[str]:
    """List all Kafka topics."""
    command = "kafka-topics.sh --list --bootstrap-server localhost:9092"
    result = run_docker_command(KAFKA_CONTAINER, command)
    topics = [t for t in result.split('\n') if t and 'northwind' in t]
    return topics


def check_kafka_topic_messages(topic: str, limit: int = 5) -> str:
    """Get recent messages from a Kafka topic."""
    command = f"""kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic} --from-beginning --max-messages {limit} --timeout-ms 5000 2>/dev/null || echo 'No messages'"""
    result = run_docker_command(KAFKA_CONTAINER, command)
    return result


def check_clickhouse_staging(table: str, limit: int = 10) -> str:
    """Query ClickHouse staging table."""
    query = f"SELECT * FROM {table} ORDER BY updatedate DESC LIMIT {limit} FORMAT PrettyCompact"
    command = f'clickhouse-client --query "{query}"'
    result = run_docker_command(CLICKHOUSE_CONTAINER, command)
    return result


def check_clickhouse_dw(table: str, limit: int = 10) -> str:
    """Query ClickHouse data warehouse table."""
    query = f"SELECT * FROM {table} ORDER BY updatedate DESC LIMIT {limit} FORMAT PrettyCompact"
    command = f'clickhouse-client --query "{query}"'
    result = run_docker_command(CLICKHOUSE_CONTAINER, command)
    return result


def count_clickhouse_table(table: str) -> int:
    """Get row count from a ClickHouse table."""
    query = f"SELECT COUNT(*) FROM {table}"
    command = f'clickhouse-client --query "{query}"'
    result = run_docker_command(CLICKHOUSE_CONTAINER, command)
    try:
        return int(result.strip())
    except:
        return 0


def monitor_kafka():
    """Monitor Kafka topics for CDC events."""
    print("\n" + "="*80)
    print("KAFKA TOPICS MONITORING")
    print("="*80)
    
    topics = check_kafka_topics()
    print(f"\n📊 Found {len(topics)} Northwind topics:")
    for topic in topics:
        print(f"  - {topic}")
    
    print("\n📨 Recent messages (sample):")
    for topic in topics[:3]:  # Check first 3 topics
        print(f"\n  Topic: {topic}")
        messages = check_kafka_topic_messages(topic, limit=2)
        if messages and "No messages" not in messages:
            print(f"    {messages[:200]}...")
        else:
            print("    No recent messages")


def check_staging():
    """Check ClickHouse staging tables."""
    print("\n" + "="*80)
    print("CLICKHOUSE STAGING TABLES")
    print("="*80)
    
    staging_tables = [
        "northwind.northwind_customers",
        "northwind.northwind_orders",
        "northwind.northwind_order_details",
        "northwind.northwind_products",
        "northwind.northwind_employees"
    ]
    
    for table in staging_tables:
        count = count_clickhouse_table(table)
        print(f"\n📊 {table}: {count} rows")
        
        if count > 0:
            recent = check_clickhouse_staging(table, limit=3)
            print(f"  Recent records:")
            for line in recent.split('\n')[:5]:
                if line.strip():
                    print(f"    {line[:100]}")


def check_dw():
    """Check ClickHouse data warehouse tables."""
    print("\n" + "="*80)
    print("CLICKHOUSE DATA WAREHOUSE")
    print("="*80)
    
    dw_tables = [
        "DimCustomer",
        "DimProducts",
        "DimEmployees",
        "FactOrders",
        "FactEmployeeTerritories"
    ]
    
    for table in dw_tables:
        count = count_clickhouse_table(table)
        print(f"\n📊 {table}: {count} rows")
        
        if count > 0:
            recent = check_clickhouse_dw(table, limit=2)
            print(f"  Sample records:")
            for line in recent.split('\n')[:4]:
                if line.strip():
                    print(f"    {line[:100]}")


def track_data_flow(customer_id: str = "TEST1", wait_seconds: int = 30):
    """Track data flow from PostgreSQL through the entire pipeline."""
    print("\n" + "="*80)
    print("TRACKING DATA FLOW THROUGH CDC PIPELINE")
    print("="*80)
    
    # Step 1: Insert into PostgreSQL
    print(f"\n[STEP 1] Inserting test customer '{customer_id}' into PostgreSQL...")
    if not insert_customer(customer_id, f"Test Company {customer_id}", 
                          f"Test Contact {customer_id}", "Test City", "Test Country"):
        print("❌ Failed to insert. Aborting tracking.")
        return
    
    time.sleep(2)  # Wait for Debezium to capture
    
    # Step 2: Check Kafka
    print(f"\n[STEP 2] Checking Kafka topic for CDC event (waiting {wait_seconds}s)...")
    time.sleep(wait_seconds)
    
    kafka_topic = "northwind.public.customers"
    messages = check_kafka_topic_messages(kafka_topic, limit=5)
    if messages and "No messages" not in messages:
        print(f"✅ CDC event detected in Kafka topic: {kafka_topic}")
        print(f"   Message preview: {messages[:150]}...")
    else:
        print(f"⚠️  No messages found in Kafka topic: {kafka_topic}")
        print("   (This might be normal if messages were already consumed)")
    
    # Step 3: Check ClickHouse Staging
    print(f"\n[STEP 3] Checking ClickHouse staging table...")
    time.sleep(5)  # Wait for Spark to process
    
    staging_count_before = count_clickhouse_table("northwind.northwind_customers")
    print(f"   Staging table count: {staging_count_before}")
    
    # Check for our test customer
    query = f"SELECT customer_id, company_name, operation, updatedate FROM northwind.northwind_customers WHERE customer_id = '{customer_id}' ORDER BY updatedate DESC LIMIT 1"
    command = f'clickhouse-client --query "{query}"'
    staging_result = run_docker_command(CLICKHOUSE_CONTAINER, command)
    
    if customer_id in staging_result:
        print(f"✅ Customer found in staging: {staging_result}")
    else:
        print(f"⚠️  Customer not yet in staging (may need more time)")
        print(f"   Latest staging records:")
        recent = check_clickhouse_staging("northwind.northwind_customers", limit=3)
        print(f"   {recent[:200]}")
    
    # Step 4: Check Data Warehouse
    print(f"\n[STEP 4] Checking ClickHouse Data Warehouse (waiting for ETL job)...")
    time.sleep(20)  # Wait for DW ETL job to run
    
    dw_query = f"SELECT CustomerAlternateKey, CompanyName, updatedate FROM DimCustomer WHERE CustomerAlternateKey = '{customer_id}' ORDER BY updatedate DESC LIMIT 1"
    command = f'clickhouse-client --query "{dw_query}"'
    dw_result = run_docker_command(CLICKHOUSE_CONTAINER, command)
    
    if customer_id in dw_result:
        print(f"✅ Customer found in Data Warehouse: {dw_result}")
    else:
        print(f"⚠️  Customer not yet in DW (ETL job may need more time)")
        print(f"   Latest DW records:")
        recent = check_clickhouse_dw("DimCustomer", limit=3)
        print(f"   {recent[:200]}")
    
    print("\n" + "="*80)
    print("DATA FLOW TRACKING COMPLETE")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Monitor and Data Tracker")
    
    parser.add_argument("--insert-customer", action="store_true",
                       help="Insert a test customer")
    parser.add_argument("--insert-order", type=int, metavar="ORDER_ID",
                       help="Insert a test order with given ID")
    parser.add_argument("--update-customer", nargs=2, metavar=("CUSTOMER_ID", "NEW_NAME"),
                       help="Update a customer's company name")
    parser.add_argument("--delete-customer", type=str, metavar="CUSTOMER_ID",
                       help="Delete a customer")
    parser.add_argument("--monitor-kafka", action="store_true",
                       help="Monitor Kafka topics")
    parser.add_argument("--check-staging", action="store_true",
                       help="Check ClickHouse staging tables")
    parser.add_argument("--check-dw", action="store_true",
                       help="Check ClickHouse data warehouse tables")
    parser.add_argument("--track-all", action="store_true",
                       help="Track complete data flow (insert + monitor)")
    parser.add_argument("--customer-id", type=str, default="TEST1",
                       help="Customer ID for test operations (default: TEST1)")
    
    args = parser.parse_args()
    
    if args.insert_customer:
        insert_customer(args.customer_id, f"Test Company {args.customer_id}")
    elif args.insert_order:
        insert_order(args.insert_order, args.customer_id)
    elif args.update_customer:
        update_customer(args.update_customer[0], args.update_customer[1])
    elif args.delete_customer:
        delete_customer(args.delete_customer)
    elif args.monitor_kafka:
        monitor_kafka()
    elif args.check_staging:
        check_staging()
    elif args.check_dw:
        check_dw()
    elif args.track_all:
        track_data_flow(args.customer_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

