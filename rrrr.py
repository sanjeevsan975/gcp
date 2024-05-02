import csv
import random
from datetime import datetime, timedelta

def generate_stock_prices(start_price, end_price, start_time, end_time, time_increment):
    current_time = start_time
    prices = []
    while current_time <= end_time:
        price = round(random.uniform(start_price, end_price), 2)
        prices.append((current_time.strftime("2024-4-24 %H:%M:%S"), price))
        current_time += timedelta(minutes=time_increment)
    return prices

def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Timestamp', 'Price'])
        writer.writerows(data)

start_price = 1444.90
end_price = 1438.45
start_time = datetime.strptime('09:30:00', '%H:%M:%S')
end_time = datetime.strptime('15:30:00', '%H:%M:%S')
time_increment = 1

prices = generate_stock_prices(start_price, end_price, start_time, end_time, time_increment)
save_to_csv(prices, 'prices.csv')
