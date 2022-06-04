import argparse


class ArgumentParser:

    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Describe How to Build and Run")
        self.parser.add_argument(
            '--data-path',
            type=str,
            help="set relative path of data directory",
            required=False,
            default='../data'
        )
        self.parser.add_argument(
            '--port',
            type=int,
            help="set postgres port",
            required=False,
            default=5432
        )
        self.parser.add_argument(
            '--ip',
            type=str,
            help="set postgres ip",
            required=False,
            default='127.0.0.1'
        )
