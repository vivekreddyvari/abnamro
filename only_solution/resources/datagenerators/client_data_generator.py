import logging
import random
import string
from dataclasses import dataclass
from abc import ABC, abstractmethod

logging.basicConfig(filename="app.log", level=logging.INFO)


@dataclass
class Client:
    id: int
    name: str
    email: str


class ClientGenerator(ABC):

    @abstractmethod
    def generate(self) -> list[Client]:
        pass


class RandomGenerator(ClientGenerator):
    names = ['John', 'Javier', 'Thomas', 'Wright', 'Electra', 'Pip', 'Shadowman']
    mail_domains = ['gmail', 'yahoo', 'hotmail', 'aol']

    def generate(self) -> list[Client]:
        clients = []

        for id in range(100010, 100400):
            name = self.generate_random_name()
            email = self.generate_random_email()

            clients.append(Client(id, name, email))

        return clients

    def generate_random_name(self):
        first = random.choice(self.names)
        last = "".join(random.choices(string.ascii_uppercase, k=random.randint(4, 7)))
        return f"{first} {last}"

    def generate_random_email(self):
        name = self.generate_random_name().replace(" ", "").lower()
        domain = random.choice(self.mail_domains)
        return f"{name}@{domain}"


class ClientWriter(ABC):

    @abstractmethod
    def write(self, clients: list[Client]):
        pass


class CSVWriter(ClientWriter):

    def write(self, clients: list[Client]):
        data = "\n".join(",".join(map(str, c.__dict__.values())) for c in clients)

        with open('../clients/clients.csv', 'w') as f:
            f.write(data)


def main():
    generator = RandomGenerator()
    clients = generator.generate()

    writer = CSVWriter()
    writer.write(clients)

    logging.info("Data generation completed")


if __name__ == '__main__':
    main()