import pathlib
import zipfile
import os


class Archive:

    def __init__(self):

        self.name = ''

    @staticmethod
    def filestrings(path: str):

        items = pathlib.Path(path).glob('*.json')
        return [str(item) for item in items if item.is_file()]

    def exc(self, path: str):

        filestrings = self.filestrings(path=path)

        zipfileobject = zipfile.ZipFile(path + '.zip', 'w')

        with zipfileobject:
            for filestring in filestrings:
                zipfileobject.write(filename=filestring, arcname=os.path.basename(filestring))

        zipfileobject.close()
