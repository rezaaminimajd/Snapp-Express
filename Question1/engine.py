import logging

from services import DataLoader
from services import ArgumentParser


logging.basicConfig(encoding='utf-8', level=logging.INFO)

if __name__ == '__main__':
    csv_files = DataLoader(ArgumentParser().parser.parse_args().data_path)
