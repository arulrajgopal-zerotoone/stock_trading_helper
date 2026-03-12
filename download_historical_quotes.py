from yfinance import download

stock_name = "ITC"

data =download(f"{stock_name}.NS")
data.to_csv(f"Data\{stock_name}.csv")