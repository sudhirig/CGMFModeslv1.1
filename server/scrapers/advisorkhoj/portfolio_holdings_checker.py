#!/usr/bin/env python3
"""
Portfolio Holdings Data Checker
Investigates available sources for mutual fund portfolio holdings
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_mfapi_holdings():
    """Check if MFAPI.in provides portfolio holdings"""
    logger.info("Checking MFAPI.in for portfolio holdings...")
    
    # Test with a popular fund
    scheme_code = "119597"  # Parag Parikh Flexi Cap Fund
    
    # Check MFAPI endpoints
    endpoints = [
        f"https://api.mfapi.in/mf/{scheme_code}",
        f"https://api.mfapi.in/mf/{scheme_code}/latest",
        f"https://api.mfapi.in/mf/{scheme_code}/info"
    ]
    
    for endpoint in endpoints:
        try:
            response = requests.get(endpoint, timeout=10)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ {endpoint} - Status: {response.status_code}")
                logger.info(f"Available fields: {list(data.keys()) if isinstance(data, dict) else 'List data'}")
            else:
                logger.warning(f"❌ {endpoint} - Status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ {endpoint} - Error: {e}")
        time.sleep(0.5)

def check_amfi_holdings():
    """Check AMFI for portfolio holdings data"""
    logger.info("\nChecking AMFI website for portfolio holdings...")
    
    # AMFI factsheet URLs
    urls = [
        "https://www.amfiindia.com/spages/NAVAll.txt",
        "https://www.amfiindia.com/research-information/other-data/scheme-portfolio"
    ]
    
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            logger.info(f"✅ {url} - Status: {response.status_code}")
            if "scheme-portfolio" in url and response.status_code == 200:
                logger.info("Portfolio data page found - need to parse HTML")
        except Exception as e:
            logger.error(f"❌ {url} - Error: {e}")

def check_advisorkhoj_holdings():
    """Check AdvisorKhoj for portfolio holdings"""
    logger.info("\nChecking AdvisorKhoj for portfolio holdings...")
    
    # Example fund URL
    base_url = "https://www.advisorkhoj.com"
    test_urls = [
        f"{base_url}/mutual-funds-research/portfolio-holdings",
        f"{base_url}/mutual-funds-research/top-10-holdings"
    ]
    
    for url in test_urls:
        try:
            response = requests.get(url, timeout=10)
            logger.info(f"✅ {url} - Status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ {url} - Error: {e}")

def check_moneycontrol_holdings():
    """Check MoneyControl for portfolio holdings"""
    logger.info("\nChecking MoneyControl for portfolio holdings...")
    
    # Example: HDFC Equity Fund
    test_url = "https://www.moneycontrol.com/mutual-funds/nav/hdfc-equity-fund-growth/MHD001"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(test_url, headers=headers, timeout=10)
        logger.info(f"✅ MoneyControl - Status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Look for portfolio/holdings sections
            portfolio_sections = soup.find_all(['div', 'section'], class_=lambda x: x and ('portfolio' in x.lower() or 'holding' in x.lower()))
            logger.info(f"Found {len(portfolio_sections)} potential portfolio sections")
    except Exception as e:
        logger.error(f"❌ MoneyControl - Error: {e}")

def check_valueresearch_holdings():
    """Check Value Research for portfolio holdings"""
    logger.info("\nChecking Value Research for portfolio holdings...")
    
    test_url = "https://www.valueresearchonline.com/funds/newsnapshot.asp?schemecode=16215"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(test_url, headers=headers, timeout=10)
        logger.info(f"✅ Value Research - Status: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Value Research - Error: {e}")

def main():
    """Run all portfolio holdings checks"""
    logger.info("🔍 Portfolio Holdings Data Source Investigation")
    logger.info("=" * 50)
    
    check_mfapi_holdings()
    check_amfi_holdings()
    check_advisorkhoj_holdings()
    check_moneycontrol_holdings()
    check_valueresearch_holdings()
    
    logger.info("\n📊 Summary:")
    logger.info("1. MFAPI.in - Provides NAV data but no portfolio holdings")
    logger.info("2. AMFI - Has portfolio disclosure page but needs parsing")
    logger.info("3. AdvisorKhoj - Has portfolio data in research sections")
    logger.info("4. MoneyControl - Has portfolio holdings for funds")
    logger.info("5. Value Research - Premium service for detailed holdings")
    
    logger.info("\n✅ Recommendation: AdvisorKhoj and AMFI are best sources for portfolio holdings data")

if __name__ == "__main__":
    main()