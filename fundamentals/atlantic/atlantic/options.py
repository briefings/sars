import os
import sys
import pandas as pd
import logging


def main():

    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # url
    url = 'https://raw.githubusercontent.com/briefings/sars/develop/fundamentals/atlantic/warehouse/baselines.csv'

    # Reading the data set of interest
    try:
        data = pd.read_csv(filepath_or_buffer=url,
                           header=0, usecols=['datetimeobject', 'STUSPS', 'positiveIncreaseRate'],
                           parse_dates=['datetimeobject'], encoding='utf-8')
    except OSError as err:
        raise Exception(err.strerror) from err

    # The text form of the date field
    data.loc[:, 'date'] = data['datetimeobject'].astype(str)
    data.drop(columns=['datetimeobject'], inplace=True)
    data = data[['date', 'STUSPS', 'positiveIncreaseRate']].copy()

    # Exploring lines
    for stusps in data['STUSPS'].unique():
        blob = data[data['STUSPS'] == stusps]
        blob.to_csv(path_or_buf=os.path.join(hub, stusps + '.csv'), 
                    index=False, header=True, encoding='utf-8')

    # Exploring nests
    data.to_csv(path_or_buf=os.path.join(hub, 'positiveIncreaseRateTidy.csv'),
                index=False, header=True, encoding='utf-8')

    # Pivot in relation to the best structure for D3 illustrations
    matrix = data.pivot(index='STUSPS', columns='date', values='positiveIncreaseRate')
    matrix.reset_index(drop=False, inplace=True)
    matrix.to_csv(path_or_buf=os.path.join(hub, 'positiveIncreaseRate.csv'), index=False, header=True, encoding='utf-8')
    logger.info('Graphics Matrix\n{}\n'.format(matrix.tail()))


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    # Data hub for graphics prototyping
    hub = os.path.join(root, 'hub')

    if not os.path.exists(hub):
        os.makedirs(hub)

    main()
