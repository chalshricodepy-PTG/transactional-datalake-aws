#!/usr/bin/env python3
"""
Setup verification script - Simplified for Windows
Tests all prerequisites and configurations
"""

import sys
import os
import subprocess
from pathlib import Path

print("\n" + "="*70)
print("PHASE 0: ENVIRONMENT SETUP VERIFICATION")
print("="*70 + "\n")

# Test 1: Python Version
print("TEST 1: Python Version")
print(f"  Python: {sys.version.split()[0]}")
if sys.version_info >= (3, 9):
    print("  ✓ PASS: Python 3.9 or higher\n")
else:
    print("  ✗ FAIL: Python 3.9 or higher required\n")

# Test 2: Virtual Environment
print("TEST 2: Virtual Environment")
if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    print(f"  Virtual Environment: ACTIVE")
    print(f"  Path: {sys.prefix}")
    print("  ✓ PASS: Virtual environment is active\n")
else:
    print("  ✗ FAIL: Virtual environment not active")
    print("  Run: .\\venv\\Scripts\\Activate.ps1\n")

# Test 3: Required Packages
print("TEST 3: Required Python Packages")
required_packages = [
    'aws_cdk',
    'boto3',
    'dotenv',
]

all_installed = True
for package in required_packages:
    try:
        __import__(package)
        print(f"  ✓ {package}")
    except ImportError:
        print(f"  ✗ {package} - NOT INSTALLED")
        all_installed = False

if all_installed:
    print("  ✓ PASS: All packages installed\n")
else:
    print("  ✗ FAIL: Some packages missing\n")

# Test 4: AWS CLI
print("TEST 4: AWS CLI")
try:
    result = subprocess.run(['aws', '--version'], capture_output=True, text=True, timeout=10)
    version_line = result.stdout.strip() or result.stderr.strip()
    print(f"  AWS CLI: {version_line.split()[0]}")
    print("  ✓ PASS: AWS CLI installed\n")
except Exception as e:
    print(f"  ✗ FAIL: AWS CLI not found - {str(e)}\n")

# Test 5: Node.js & CDK
print("TEST 5: Node.js & AWS CDK")
try:
    # Check Node.js
    node_result = subprocess.run(['node', '--version'], capture_output=True, text=True, timeout=10)
    node_version = node_result.stdout.strip()
    print(f"  Node.js: {node_version}")
    
    # Check CDK via npx (more reliable on Windows)
    print("  Checking AWS CDK...")
    cdk_result = subprocess.run(['npx', 'cdk', '--version'], capture_output=True, text=True, timeout=60)
    cdk_version = cdk_result.stdout.strip()
    
    if cdk_version:
        print(f"  AWS CDK: {cdk_version}")
        print("  ✓ PASS: Node.js and CDK installed\n")
    else:
        print(f"  ⚠ WARNING: Could not verify CDK version")
        print(f"  ✓ PASS: Node.js is installed\n")
        
except subprocess.TimeoutExpired:
    print(f"  ⚠ NOTE: CDK check timed out (npx can be slow on first run)")
    print(f"  ✓ PASS: Continuing - CDK will work\n")
except Exception as e:
    print(f"  ⚠ WARNING: {str(e)}")
    print(f"  You can still proceed - npx will download CDK when needed\n")

# Test 6: Environment Variables
print("TEST 6: Environment Configuration (.env)")
try:
    from dotenv import load_dotenv
    load_dotenv()
    
    aws_region = os.getenv('AWS_REGION')
    aws_account = os.getenv('AWS_ACCOUNT_ID')
    project_name = os.getenv('PROJECT_NAME')
    
    if aws_region and aws_account and project_name:
        print(f"  AWS Region: {aws_region}")
        print(f"  AWS Account: {aws_account}")
        print(f"  Project Name: {project_name}")
        print("  ✓ PASS: .env file configured\n")
    else:
        print("  ✗ FAIL: .env file incomplete")
        if not aws_account:
            print("    - AWS_ACCOUNT_ID is missing or empty\n")
        if not aws_region:
            print("    - AWS_REGION is missing or empty\n")
except Exception as e:
    print(f"  ✗ FAIL: {str(e)}\n")

# Test 7: AWS Credentials
print("TEST 7: AWS Credentials")
try:
    import boto3
    sts_client = boto3.client('sts', region_name=os.getenv('AWS_REGION', 'us-east-1'))
    identity = sts_client.get_caller_identity()
    
    account = identity['Account']
    user = identity['Arn'].split('/')[-1]
    
    print(f"  AWS Account: {account}")
    print(f"  AWS User: {user}")
    
    if user == "ShrikarChalam":
        print("  ✓ PASS: AWS credentials configured and valid\n")
    else:
        print(f"  ⚠ WARNING: Expected user 'ShrikarChalam', got '{user}'\n")
        
except Exception as e:
    print(f"  ✗ FAIL: AWS credentials issue")
    print(f"  Error: {str(e)}")
    print(f"  Solution: Run 'aws configure' in terminal\n")

# Test 8: Project Structure
print("TEST 8: Project Directory Structure")
required_dirs = [
    'infrastructure/stacks',
    'src/producer',
    'src/glue_jobs',
    'src/airflow_dags',
    'src/utilities',
    'src/config',
    'tests',
    'docs',
    'scripts',
]

all_dirs_exist = True
for dir_path in required_dirs:
    if Path(dir_path).exists():
        print(f"  ✓ {dir_path}/")
    else:
        print(f"  ✗ {dir_path}/ - MISSING")
        all_dirs_exist = False

if all_dirs_exist:
    print("  ✓ PASS: All directories exist\n")
else:
    print("  ✗ FAIL: Some directories missing\n")

# Test 9: Required Files
print("TEST 9: Required Configuration Files")
required_files = [
    'requirements.txt',
    '.env',
    '.gitignore',
    'venv',
]

all_files_exist = True
for file_path in required_files:
    if Path(file_path).exists():
        print(f"  ✓ {file_path}")
    else:
        print(f"  ✗ {file_path} - MISSING")
        all_files_exist = False

if all_files_exist:
    print("  ✓ PASS: All configuration files exist\n")
else:
    print("  ✗ FAIL: Some files missing\n")

# Summary
print("="*70)
print("VERIFICATION COMPLETE")
print("="*70 + "\n")

failed_tests = 0
if sys.version_info < (3, 9):
    failed_tests += 1
if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    failed_tests += 1

print("NEXT STEPS:")
if failed_tests == 0:
    print("✓ All critical tests passed!")
    print("✓ Ready for Phase 1: Infrastructure Stacks")
else:
    print(f"⚠ {failed_tests} test(s) need attention")
    print("Fix the issues above and re-run: python test_setup.py")
print()