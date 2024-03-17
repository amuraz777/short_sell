import configparser

RENAME_COL = {'Detenteur de la position courte nette': 'hedge_fund',
              'Legal Entity Identifier detenteur': 'hf_id',
              'Emetteur / issuer': 'issuer',
              'Ratio': 'ratio',
              'code ISIN': 'isin',
              'Date de debut position': 'start_position',
              'Date de debut de publication position': 'start_publication',
              'Date de fin de publication position': 'end_position'
              }

config = configparser.RawConfigParser()
config.read('config.ini')

CONFIG = {}
CONFIG['EODH'] = dict(config.items('EODH'))
