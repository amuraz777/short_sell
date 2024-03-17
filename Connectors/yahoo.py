import datetime as dt
import yfinance as yf


class Yahoo:
    def __init__(self):
        self.url_base = "token"

    @staticmethod
    def df_get_prices(ticker: str, dt_start: dt.datetime, dt_end: dt.datetime):
        data = yf.Ticker(ticker).history(start=dt_start.strftime("%Y-%m-%d"),  end=dt_end.strftime("%Y-%m-%d"), period="1d")
        data.index = data.index.tz_localize(None)
        return data


if __name__ == '__main__':
    ticker = "UBI.PA"
    dt_start = dt.datetime(2024,2,1)
    dt_end = dt.datetime(2024,2,10)
    data = Yahoo.df_get_prices(ticker, dt_start, dt_end)

    print(data)