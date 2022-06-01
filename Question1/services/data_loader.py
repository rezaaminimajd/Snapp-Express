import os


class DataLoader:
    DIRECTORY_SPLITTER = '/'
    FILENAME_SPLITTER = '.'
    FIRST_PART = 0
    LAST_PART = -1

    def __init__(self, files_path: str):
        cwd = os.getcwd()

        self.files_path = os.path.abspath(
            os.path.join(cwd, files_path)
        )

    def get_files(self) -> list:
        return [os.path.join(self.files_path, name) for name in os.listdir(self.files_path)]

    def extract_filenames(self) -> dict:
        files_paths = self.get_files()
        file_names = dict()
        for file_path in files_paths:
            file_names[DataLoader.get_filename(file_path)] = file_path
        return file_names

    @staticmethod
    def get_filename(filename: str) -> str:
        return (filename.split(DataLoader.DIRECTORY_SPLITTER)[DataLoader.LAST_PART]).split(
            DataLoader.FILENAME_SPLITTER
        )[DataLoader.FIRST_PART]