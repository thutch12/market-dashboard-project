#!/usr/bin/env python3
import pandas as pd
import requests
import time
from datetime import datetime
import csv
from io import StringIO

class FourExchangeActiveStocksFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        
        # Define our target exchanges
        self.target_exchanges = ['NASDAQ', 'NYSE', 'ASX', 'HKSE']
        
    def get_all_stocks_from_target_exchanges(self):
        """
        Get ALL stocks from NASDAQ, NYSE, ASX, and HKSE exchanges
        """
        print("Fetching comprehensive stock listing from all exchanges...")
        params = {
            'function': 'LISTING_STATUS',
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            # Parse CSV response
            csv_data = StringIO(response.text)
            reader = csv.DictReader(csv_data)
            
            target_stocks = []
            exchange_counts = {exchange: 0 for exchange in self.target_exchanges}
            
            for row in reader:
                exchange = row.get('exchange', '').strip()
                status = row.get('status', '').lower().strip()
                asset_type = row.get('assetType', '').strip()
                
                # Filter for active stocks in our target exchanges
                if (status == 'active' and 
                    exchange in self.target_exchanges and
                    asset_type == 'Stock'):  # Only actual stocks, not ETFs or other instruments
                    
                    stock_info = {
                        'symbol': row.get('symbol', '').strip(),
                        'name': row.get('name', '').strip(),
                        'exchange': exchange,
                        'ipoDate': row.get('ipoDate', '').strip()
                    }
                    
                    target_stocks.append(stock_info)
                    exchange_counts[exchange] += 1
            
            # Display summary
            print(f"\nStock counts by exchange:")
            total_stocks = 0
            for exchange, count in exchange_counts.items():
                print(f"  {exchange}: {count:,} stocks")
                total_stocks += count
            print(f"  TOTAL: {total_stocks:,} stocks")
            
            return target_stocks
            
        except Exception as e:
            print(f"Error fetching stock listing: {e}")
            return []
    
    def get_daily_data(self, symbol, target_date):
        """
        Get daily OHLCV data for a specific stock and date
        """
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Last 100 trading days
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Handle API errors and rate limits
            if "Error Message" in data:
                return None
            if "Note" in data:
                print(f"Rate limit hit for {symbol}, waiting 60 seconds...")
                time.sleep(60)
                return self.get_daily_data(symbol, target_date)
            
            time_series = data.get('Time Series (Daily)', {})
            if not time_series:
                return None
            
            # Find data for target date or closest trading day
            target_data = None
            actual_date = None
            
            if target_date in time_series:
                target_data = time_series[target_date]
                actual_date = target_date
            else:
                # Find the closest trading day before target date
                available_dates = sorted([d for d in time_series.keys() if d <= target_date], 
                                       reverse=True)
                if available_dates:
                    actual_date = available_dates[0]
                    target_data = time_series[actual_date]
            
            if target_data:
                try:
                    return {
                        'symbol': symbol,
                        'date': actual_date,
                        'open': float(target_data['1. open']),
                        'high': float(target_data['2. high']),
                        'low': float(target_data['3. low']),
                        'close': float(target_data['4. close']),
                        'volume': int(target_data['5. volume'])
                    }
                except (ValueError, KeyError) as e:
                    print(f"Data parsing error for {symbol}: {e}")
                    return None
            
            return None
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None
    
    def calculate_activity_metrics(self, stock_data):
        """
        Calculate comprehensive activity metrics for ranking
        """
        # Basic price movement calculations
        price_change = stock_data['close'] - stock_data['open']
        price_change_percent = (price_change / stock_data['open']) * 100 if stock_data['open'] != 0 else 0
        
        # Intraday volatility (high-low range as percentage of open)
        intraday_range = stock_data['high'] - stock_data['low']
        volatility_percent = (intraday_range / stock_data['open']) * 100 if stock_data['open'] != 0 else 0
        
        # Activity score components
        volume_millions = stock_data['volume'] / 1_000_000
        abs_price_movement = abs(price_change_percent)
        
        # Weighted activity score
        # Volume gets base weight, price movement gets 8x weight, volatility gets 4x weight
        activity_score = (volume_millions * 1.0 + 
                         abs_price_movement * 8.0 + 
                         volatility_percent * 4.0)
        
        return {
            'price_change': round(price_change, 2),
            'price_change_percent': round(price_change_percent, 2),
            'volatility_percent': round(volatility_percent, 2),
            'activity_score': round(activity_score, 2)
        }
    
    def process_stocks_in_batches(self, stocks, target_date, batch_size=100):
        """
        Process stocks in batches to manage API rate limits and provide progress updates
        """
        print(f"Processing {len(stocks)} stocks in batches of {batch_size}...")
        all_stock_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(stocks) + batch_size - 1) // batch_size
            
            print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} stocks)...")
            
            batch_start_time = time.time()
            batch_data = []
            
            for j, stock in enumerate(batch):
                symbol = stock['symbol']
                print(f"  [{j+1}/{len(batch)}] Fetching {symbol}...")
                
                daily_data = self.get_daily_data(symbol, target_date)
                if daily_data and daily_data['volume'] > 0:  # Only include stocks with trading volume
                    # Add exchange info
                    daily_data['exchange'] = stock['exchange']
                    daily_data['name'] = stock.get('name', 'N/A')
                    batch_data.append(daily_data)
                
                # Rate limiting: 5 calls per minute for free tier
                time.sleep(12)  # 12 seconds between calls = 5 calls per minute
            
            all_stock_data.extend(batch_data)
            
            batch_time = time.time() - batch_start_time
            print(f"  Batch {batch_num} completed in {batch_time/60:.1f} minutes")
            print(f"  Retrieved data for {len(batch_data)} stocks with trading volume")
            
            # Estimated time remaining
            if batch_num < total_batches:
                remaining_batches = total_batches - batch_num
                estimated_remaining = (batch_time * remaining_batches) / 60
                print(f"  Estimated time remaining: {estimated_remaining:.1f} minutes")
        
        return all_stock_data
    
    def get_most_active_stocks_for_date(self, target_date, count=10):
        """
        Main function to get most active stocks for a specific date
        """
        print(f"=" * 80)
        print(f"FETCHING MOST ACTIVE STOCKS FOR {target_date}")
        print(f"From exchanges: {', '.join(self.target_exchanges)}")
        print(f"=" * 80)
        
        # Step 1: Get all stocks from target exchanges
        all_stocks = self.get_all_stocks_from_target_exchanges()
        
        if not all_stocks:
            print("Could not fetch stock listings.")
            return []
        
        # Step 2: Ask user for confirmation due to API usage
        print(f"\nThis will fetch data for {len(all_stocks):,} stocks.")
        print("With API rate limits, this will take approximately:")
        estimated_hours = (len(all_stocks) * 12) / 3600  # 12 seconds per call
        print(f"  {estimated_hours:.1f} hours with free API tier")
        print(f"  Significantly less with paid API tier")
        
        confirm = input("\nContinue with full analysis? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Operation cancelled.")
            return []
        
        # Step 3: Process all stocks
        stock_data_list = self.process_stocks_in_batches(all_stocks, target_date)
        
        if not stock_data_list:
            print("No stock data retrieved.")
            return []
        
        print(f"\nSuccessfully retrieved data for {len(stock_data_list)} stocks")
        
        # Step 4: Calculate activity metrics and rank
        print("Calculating activity scores...")
        enhanced_stocks = []
        
        for stock_data in stock_data_list:
            metrics = self.calculate_activity_metrics(stock_data)
            
            enhanced_stock = {
                'symbol': stock_data['symbol'],
                'name': stock_data['name'],
                'exchange': stock_data['exchange'],
                'date': stock_data['date'],
                'open': stock_data['open'],
                'high': stock_data['high'],
                'low': stock_data['low'],
                'close': stock_data['close'],
                'volume': stock_data['volume'],
                'price_change': metrics['price_change'],
                'price_change_percent': metrics['price_change_percent'],
                'volatility_percent': metrics['volatility_percent'],
                'activity_score': metrics['activity_score']
            }
            enhanced_stocks.append(enhanced_stock)
        
        # Step 5: Sort by activity score and return top N
        enhanced_stocks.sort(key=lambda x: x['activity_score'], reverse=True)
        
        print(f"\nTop {count} most active stocks identified!")
        return enhanced_stocks[:count]
    
    def display_results(self, active_stocks):
        """
        Display results in a comprehensive format
        """
        if not active_stocks:
            print("No data to display.")
            return
        
        print(f"\n" + "=" * 120)
        print(f"TOP {len(active_stocks)} MOST ACTIVE STOCKS")
        print(f"=" * 120)
        
        # Header
        print(f"{'#':<3} {'Symbol':<10} {'Exchange':<8} {'Date':<12} {'Open':<8} {'High':<8} "
              f"{'Low':<8} {'Close':<8} {'Change':<8} {'Change%':<8} {'Volume':<12} "
              f"{'Volatility%':<10} {'Activity':<10}")
        print("-" * 120)
        
        # Data rows
        for i, stock in enumerate(active_stocks, 1):
            volume_formatted = f"{stock['volume']:,}"
            
            print(f"{i:<3} {stock['symbol']:<10} {stock['exchange']:<8} {stock['date']:<12} "
                  f"${stock['open']:<7.2f} ${stock['high']:<7.2f} ${stock['low']:<7.2f} "
                  f"${stock['close']:<7.2f} {stock['price_change']:<8.2f} "
                  f"{stock['price_change_percent']:<7.2f}% {volume_formatted:<12} "
                  f"{stock['volatility_percent']:<9.2f}% {stock['activity_score']:<10.2f}")
        
        # Summary by exchange
        print(f"\n" + "-" * 50)
        print("BREAKDOWN BY EXCHANGE:")
        exchange_summary = {}
        for stock in active_stocks:
            exchange = stock['exchange']
            if exchange not in exchange_summary:
                exchange_summary[exchange] = 0
            exchange_summary[exchange] += 1
        
        for exchange, count in sorted(exchange_summary.items()):
            print(f"  {exchange}: {count} stocks")

def main():
    """
    Main function with simplified user input
    """
    print("MULTI-EXCHANGE MOST ACTIVE STOCKS ANALYZER")
    print("Exchanges: NASDAQ, NYSE, ASX, HKSE")
    print("=" * 50)
    
    # API Key setup
    API_KEY = "FYPV46WYMH8METGG"  # Replace with your actual API key
    
    if API_KEY == "YOUR_API_KEY_HERE":
        print("Please replace 'YOUR_API_KEY_HERE' with your actual Alpha Vantage API key")
        print("Get your free API key at: https://www.alphavantage.co/support/#api-key")
        print("\nNote: This analysis requires many API calls.")
        print("Consider upgrading to a paid plan for faster processing.")
        return
    
    # Initialize fetcher
    fetcher = FourExchangeActiveStocksFetcher(API_KEY)
    
    # Get user input for date
    while True:
        date_input = input("\nEnter date (YYYY-MM-DD) or 'today': ").strip()
        
        if date_input.lower() == 'today':
            target_date = datetime.now().strftime('%Y-%m-%d')
            break
        else:
            try:
                datetime.strptime(date_input, '%Y-%m-%d')
                target_date = date_input
                break
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD")
    
    # Get number of top stocks to display
    while True:
        try:
            count = int(input("Number of top active stocks to display (default 10): ") or "10")
            if count > 0:
                break
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Please enter a valid number.")
    
    # Run analysis
    active_stocks = fetcher.get_most_active_stocks_for_date(target_date, count)
    fetcher.display_results(active_stocks)
    
    if active_stocks:
        print(f"\nAnalysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Activity Score = Volume(M) + |Price Change%|*8 + Intraday Volatility%*4")

if __name__ == "__main__":
    main()
        
    
