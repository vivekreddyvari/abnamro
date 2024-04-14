from dataclasses import dataclass
from abc import ABC, abstractmethod
import csv
import random
from datetime import datetime
from typing import List


@dataclass
class RowData:
    client_id: int
    collateral_type: str
    symbol: str
    quantity: int
    price: float
    date: str


Rows = List[RowData]


class DataGenerator(ABC):
    @abstractmethod
    def generate(self) -> List[RowData]:
        pass


class CollateralDataGenerator(DataGenerator):

    def __init__(self):
        self.client_ids = range(100010, 100400)
        self.quantities = range(1000, 10000, 1000)
        self.symbols = ["ASML", "NN", "ABN", "ING", "Philips", "Shell", "Adyen"]

    def generate(self) -> Rows:
        rows: Rows = []
        for client_id in self.client_ids:
            for quantity in self.quantities:
                for day in range(1, 7):
                    date: str = datetime(2024, 2, day).strftime("%Y-%m-%d")
                    symbol: str = random.choice(self.symbols)
                    price: float = random.uniform(
                        quantity * 10000, quantity * 10000 + 10.545
                    )
                    row: RowData = RowData(client_id, "stock", symbol, quantity, price, date)
                    rows.append(row)
        return rows


class DataPrinter:
    def print_rows(self, rows: Rows):
        for row in rows:
            print(row)


class CSVWriter:
    def write_to_csv(self, rows: Rows, filename: str):
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(
                [[row.client_id, row.collateral_type, row.symbol, row.quantity, row.price, row.date] for row in rows]
            )


def main() -> None:
    # type annotations
    generator = CollateralDataGenerator()
    rows = generator.generate()

    printer = DataPrinter()
    printer.print_rows(rows)

    writer = CSVWriter()
    writer.write_to_csv(rows, '../collateral/collateral.csv')


if __name__ == '__main__':
    main()
