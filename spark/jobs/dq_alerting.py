#!/usr/bin/env python3
"""
Data Quality Alerting Job
Sends alerts when DQ thresholds are breached
"""

import json
import argparse
import requests
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from datetime import datetime
import os

def load_dq_reports(date_str):
    """Load all DQ reports for a given date"""
    reports = []
    base_path = "/opt/hadoop/data/quality/dq_reports/"
    
    for dataset in ["customers", "orders", "products", "sellers", "order_items", "reviews", "payments"]:
        report_path = f"{base_path}/{dataset}_report_{date_str}.json"
        if os.path.exists(report_path):
            with open(report_path, 'r') as f:
                reports.append(json.load(f))
    
    return reports

def send_slack_alert(reports, threshold):
    """Send alert to Slack webhook"""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        print("No Slack webhook URL configured")
        return
    
    failed_reports = [r for r in reports if r["dq_score"] < threshold]
    
    if not failed_reports:
        return
    
    message = {
        "text": f"*Data Quality Alert - {datetime.now().strftime('%Y-%m-%d')}*",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🚨 DATA QUALITY ALERT*\n\n{len(failed_reports)} datasets failed the quality threshold ({threshold}%)"
                }
            }
        ]
    }
    
    for report in failed_reports:
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{report['dataset'].upper()}*: DQ Score = {report['dq_score']:.2f}%\nFailed checks: {report['failed_checks']}/{report['total_checks']}"
            }
        })
    
    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        print("Slack alert sent successfully")
    except Exception as e:
        print(f"Error sending Slack alert: {str(e)}")

def send_email_alert(reports, threshold):
    """Send email alert"""
    smtp_host = os.environ.get("SMTP_HOST", "")
    if not smtp_host:
        print("No SMTP configuration found")
        return
    
    failed_reports = [r for r in reports if r["dq_score"] < threshold]
    
    if not failed_reports:
        return
    
    msg = MimeMultipart()
    msg['Subject'] = f'Data Quality Alert - {datetime.now().strftime("%Y-%m-%d")}'
    msg['From'] = os.environ.get("ALERT_FROM_EMAIL", "alerts@olist.com")
    msg['To'] = os.environ.get("ALERT_TO_EMAIL", "data-team@olist.com")
    
    body = "The following datasets failed data quality checks:\n\n"
    for report in failed_reports:
        body += f"- {report['dataset']}: {report['dq_score']:.2f}% (threshold: {threshold}%)\n"
        for result in report['results']:
            if not result['passed']:
                body += f"  - {result['check_type']}: {result.get('failed_count', 0)} failures\n"
    
    msg.attach(MimeText(body, 'plain'))
    
    try:
        with smtplib.SMTP(smtp_host, 587) as server:
            server.starttls()
            server.login(os.environ.get("SMTP_USER", ""), os.environ.get("SMTP_PASSWORD", ""))
            server.send_message(msg)
        print("Email alert sent successfully")
    except Exception as e:
        print(f"Error sending email alert: {str(e)}")

def quarantine_failed_records(date_str):
    """Quarantine failed records for manual review"""
    base_path = "/opt/hadoop/data/quality/failed_records/"
    
    for dataset in ["customers", "orders", "products", "sellers"]:
        report_path = f"/opt/hadoop/data/quality/dq_reports/{dataset}_report_{date_str}.json"
        if not os.path.exists(report_path):
            continue
        
        with open(report_path, 'r') as f:
            report = json.load(f)
        
        # Check if there are failures
        if report['failed_checks'] > 0:
            # Copy failed records to quarantine
            quarantine_dir = f"{base_path}/{dataset}/"
            os.makedirs(quarantine_dir, exist_ok=True)
            
            quarantine_file = f"{quarantine_dir}/failed_{date_str}.parquet"
            print(f"Quarantining failed records to {quarantine_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--action", required=True, choices=["report", "alert", "quarantine"])
    parser.add_argument("--threshold", default=90, type=float)
    args = parser.parse_args()
    
    reports = load_dq_reports(args.date)
    
    if args.action == "report":
        for report in reports:
            print(f"{report['dataset']}: {report['dq_score']:.2f}%")
    
    elif args.action == "alert":
        send_slack_alert(reports, args.threshold)
        send_email_alert(reports, args.threshold)
    
    elif args.action == "quarantine":
        quarantine_failed_records(args.date)

if __name__ == "__main__":
    main()
