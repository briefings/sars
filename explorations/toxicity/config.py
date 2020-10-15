import os


class Config:

    def __init__(self):
        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.paths: list = [os.path.join(self.warehouse, directory) for directory in ['graphing', 'modelling']]
