import logging
import argparse

from services import DataLoader

logging.basicConfig(encoding='utf-8', level=logging.INFO)

parser = argparse.ArgumentParser(description="Describe How to Build and Run")
parser.add_argument(
    '--data_path',
    type=str,
    help="set relative path of data files",
    required=False,
    default='../data'
)

if __name__ == '__main__':
    csv_files = DataLoader(parser.parse_args().data_path)
