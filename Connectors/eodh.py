
import pandas as pd
import requests
from Utils.constants import CONFIG


class Eodh:
    def __init__(self, api_token):
        self.api_token = api_token
        self.url_base = "https://eodhd.com/api/"

    def get_symbol(self, isin: str, exch_place: str = 'PA'):
        url_suffix = f"search/{isin}?api_token={self.api_token}&fmt=json"
        data = requests.get(f"{self.url_base}{url_suffix}").json()
        if len(data) == 0:
            return None
        group_data = pd.DataFrame(data).groupby("Exchange")
        if exch_place not in group_data.groups.keys():
            return None
        info = next(iter(group_data.get_group(exch_place).to_dict('index').values()))
        return f"{info['Code']}.{exch_place}"

if __name__ == '__main__':

    isin = "US36467W1099"
    symbol = Eodh().get_symbol(isin, exch_place="US")
    print(symbol)
