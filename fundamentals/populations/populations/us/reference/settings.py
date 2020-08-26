
class Settings:

    def __init__(self):

        self.apicounty = 'https://raw.githubusercontent.com/discourses/hub/develop/' \
                         'data/countries/us/population/api/counties.csv'

        self.apistate = 'https://www2.census.gov/programs-surveys/popest/datasets/{segment}/' \
                        'state/detail/SCPRC-EST{year}-18+POP-RES.csv'
