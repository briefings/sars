import io
import zipfile

import requests


class Dearchive:

    def __init__(self, into):
        """
        The constructor
        :param into: ...
        """

        self.into = into

    @staticmethod
    def read(urlstring: str) -> bytes:
        """
        :param urlstring: The URL of the archived file that would be de-archived locally
        :return: The file contents, in byte form
        """

        try:
            req = requests.get(url=urlstring)
            req.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise err

        return req.content

    def unzip(self, urlstring: str):
        """
        De-archives a zip archive file
        :param urlstring:
        :return:
        """

        obj = zipfile.ZipFile(io.BytesIO(self.read(urlstring=urlstring)))
        obj.extractall(path=self.into)
