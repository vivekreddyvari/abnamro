from dataclasses import dataclass
from typing import List
import json
import random
import datetime
import os


@dataclass
class StockData:
    """
    Stock Data yields data related to stocks
    1. to_dict function converts to key, value pair

    """
    symbol: str
    price: float
    date: str
    open: float
    high: float
    low: float
    volume: int

    def to_dict(self):
        return {
            'symbol': self.symbol,
            'price': self.price,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'volume': self.volume
        }


class StockGenerator():
    """
    Stock generator generates data

    """

    symbols = ["ASML", "NN", "ABN", "ING", "Philips", "Shell", "Adyen"]

    def generate(self) -> List[StockData]:
        data = []
        for symbol in self.symbols:
            for date in self.get_dates():
                open_price = random.uniform(95, 150)
                high = random.uniform(open_price, open_price + 8)
                low = random.uniform(open_price - 30, open_price)
                volume = random.randint(10000, 20000)
                data.append(StockData(
                    symbol,
                    open_price,
                    date,
                    open_price,
                    high,
                    low,
                    volume
                ))
        return data

    def get_dates(self) -> List[str]:

        start = datetime.date(2024, 2, 1)
        end = datetime.date(2024, 2, 6)

        dates = []

        while start <= end:
            dates.append(str(start))
            start += datetime.timedelta(days=1)

        return dates


class JSONWriter:
    """
    Convert to Json and write in .json format
    """

    def write(self, data: List[StockData], folder: str):
        filepath = os.path.join(folder, "stocks.json")

        # convert to dict
        json_data = [d.to_dict() for d in data]

        with open(filepath, "w") as f:
            json.dump(json_data, f, indent=2)


def main():
    """
    Serves as entry point for the program

    """
    import os

    folder = os.getcwd()
    print(folder)

    generator = StockGenerator()
    data = generator.generate()

    folder = "../stocks"
    os.makedirs(folder, exist_ok=True)

    writer = JSONWriter()
    writer.write(data, folder)


if __name__ == "__main__":
    main()



