import os


class Directories:

    def __init__(self):

        self.name = ''

    @staticmethod
    def cleanup(listof: list):
        """

        :param listof: the list of directories that will be deleted
        :return:
        """

        # Foremost, delete files
        for path in listof:
            files_ = [os.remove(os.path.join(base, file))
                      for base, _, files in os.walk(path) for file in files]

            if any(files_):
                raise Exception('Unable to delete all files within path {}'.format(path))

        # ... then, directories
        for path in listof:
            directories_ = [os.removedirs(os.path.join(base, directory))
                            for base, directories, _ in os.walk(path, topdown=False) for directory in directories
                            if os.path.exists(os.path.join(base, directory))]

            if any(directories_):
                raise Exception('Unable to delete all directories within path {}'.format(path))

    @staticmethod
    def create(listof: list):
        """

        :param listof: The list of directories that will be created
        :return:
        """

        for path in listof:
            if not os.path.exists(path):
                os.makedirs(path)
