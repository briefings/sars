import os


class Config:

    def __init__(self):
        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.paths: list = [os.path.join(self.warehouse, directory) for directory in ['graphing', 'modelling']]

    @staticmethod
    def case(string: str):
        return {
            'cancerriskby': 'cancer',
            'immunologicalriskby': 'immunological',
            'kidneyriskby': 'kidney',
            'liverriskby': 'liver',
            'neurologicalriskby': 'neurological',
            'respriskby': 'respiratory'
        }.get(string, LookupError('{} does not contain a recognised pattern'.format(string)))

    def names(self, source):
        """


        :param source:  A - path name + file name + extension - string
        :return:
        """

        basename = os.path.basename(source)
        basestring = os.path.splitext(basename)[0].lower()

        if not (basestring.endswith('group') | basestring.endswith('pollutant')):
            raise Exception('''Invalid file name suffix.  Each file name must end with 'group' or 'pollutant'.''')

        string, classification = (basestring.rstrip('group'), 'group') if basestring.endswith('group') else (
            basestring.rstrip('pollutant'), 'pollutant')
        case = self.case(string=string)

        # medical -case- / pollutant or source of pollution -classification-
        return case, classification
