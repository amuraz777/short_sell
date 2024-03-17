import pandas as pd
import os
import datetime as dt

from Connectors.eodh import Eodh
from Connectors.yahoo import Yahoo
from Utils.constants import RENAME_COL, CONFIG

if __name__ == '__main__':
    file_name = "export_od_vad_20240312111500_20240312123000.csv"
    file_path = os.path.join("Files", file_name)
    df = pd.read_csv(file_path, sep=";").rename(columns=RENAME_COL)

    # Check Missing Values size - count
    missing_values = {col: sum(df[col].isna()) for col in df.columns}
    print(pd.DataFrame([missing_values])/df.shape[0]*100)

    # Check Unique Value
    unique_values = {col: df[col].nunique() for col in df.columns}
    print(pd.DataFrame([unique_values]))

    # Check Data Type
    df["start_position"] = pd.to_datetime(df["start_position"])
    df["start_publication"] = pd.to_datetime(df["start_publication"])
    df["end_position"] = pd.to_datetime(df["end_position"])

    # Check numerical columns
    #df['ratio'].hist()

    # Describe categorical columns
    df.describe(include='object')

    df.groupby("hedge_fund").get_group("CAPITAL FUND MANAGEMENT")

    # How many Hedge Fund have a Short Position in the file ?
    # Ratio includes/excludes zero
    df_exclude = df[df["ratio"] > 0]
    df_duplicated = df[["hedge_fund", "hf_id"]].drop_duplicates(["hedge_fund", "hf_id"]).sort_values("hf_id")
    print(sum(df["hedge_fund"].isna()))
    hf_count = df["hedge_fund"].drop_duplicates().shape[0]
    print(f" There are {hf_count} HFs having a short position in the file, including zero ratio.")
    hf_exclude_count = df_exclude["hedge_fund"].drop_duplicates().shape[0]
    print(f" There are {hf_exclude_count} HFs having a short position in the file, excluding zero ratio.")

    # What is the top 5 HFs having the more short position on issuers ?
    df_exclude[["hedge_fund", "issuer"]].groupby(["hedge_fund"]).count()
    df_top_5_hf = df_exclude[["issuer","hedge_fund"]].drop_duplicates().groupby("hedge_fund").count().sort_values("issuer", ascending=False).head(5).reset_index()
    print(df_top_5_hf)
    print(f" The top 5 HFs having the more position on differents issuer is: \n {' / '.join(df_top_5_hf['hedge_fund'].to_list())}")

    # What is the top 5 issuers having the highest median ratio ?
    df_top_5_issuer = df_exclude[["issuer", "ratio"]].drop_duplicates().groupby("issuer").describe().sort_values(('ratio','50%'), ascending=False).head(5).reset_index()
    print(df_top_5_issuer)
    df_top_5_issuer = df_exclude[["issuer", "ratio"]].drop_duplicates().groupby("issuer").median().sort_values('ratio', ascending=False).head(5).reset_index()
    print(df_top_5_issuer)
    print(f" The top 5 issuers having the highest median ratio is: \n {' / '.join(df_top_5_issuer['issuer'].to_list())}")

    # What is the maximal holding period for a short position ? What Hf ? What issuer ?
    df_exclude["end_position"] = df_exclude["end_position"].fillna(dt.datetime.today())
    df_exclude["holding_period"] = df_exclude.apply(lambda x: (x["end_position"] - x["start_position"]).days, axis=1)
    dict_longest_holding = df_exclude.sort_values("holding_period").to_dict('records')[-1]
    dict_shortest_holding = df_exclude.sort_values("holding_period").to_dict('records')[0]
    print(f"{dict_longest_holding['hedge_fund']} shorts {dict_longest_holding['issuer']} for {dict_longest_holding['holding_period']} ")

    # What is the minimal detention period for a short position ? What Hf ? What issuer ?
    print(pd.DataFrame(df_exclude.sort_values("holding_period").to_dict('records')[:10]))
    print(f"{dict_shortest_holding['hedge_fund']} shorts {dict_shortest_holding['issuer']} for {dict_shortest_holding['holding_period']} ")

    # Plot the histogram for AirFrance KLM
    df_air_fr_klm = df_exclude.groupby("issuer").get_group("AIR FRANCE-KLM")
    df_air_fr_klm_top_10 = df_air_fr_klm.sort_values("ratio").head(10)
    print(df_air_fr_klm_top_10)

    # How many HFs have a short position on AIR FRANCE-KLM through the years ?
    df_air_fr_klm[["hedge_fund", "start_position"]].groupby("start_position").count()
    print(df_air_fr_klm)
    start_historic = df_air_fr_klm["start_position"].min()
    end_historic = df_air_fr_klm["end_position"].max()
    historic_index = pd.bdate_range(start=start_historic, end=end_historic)
    hedge_fund_columns = df_air_fr_klm["hedge_fund"].drop_duplicates().to_list()
    df_ratio_holders = pd.DataFrame(data=0, index=historic_index, columns=hedge_fund_columns)

    group_holders = df_air_fr_klm[["hedge_fund", "start_position", "end_position", "ratio"]].groupby("hedge_fund")
    for col in df_ratio_holders.columns:
        df_holder = group_holders.get_group(col)
        for index, row in df_holder.sort_values("start_position").iterrows():
            sr_start = historic_index >= row['start_position']
            sr_end = row['end_position'] >= historic_index
            df_ratio_holders.loc[sr_start & sr_end, col] = row['ratio']

    issuer_columns = df_exclude['isin'].drop_duplicates().to_list()
    df_prices = pd.DataFrame(data=0, index=historic_index, columns=issuer_columns)

    eodh = Eodh(CONFIG['EODH']['api_token'])
    yahoo = Yahoo()
    for isin in df_prices.columns:
        print(isin)
        ticker = eodh.get_symbol(isin)
        if ticker:
            data = yahoo.df_get_prices(ticker, start_historic, end_historic)
            close_price = data["Close"]
            close_price = close_price.interpolate()
            df_prices[isin] = close_price

    print(df_prices)

    df_air_fr_klm_prices = df_prices['FR0000031122']
    df_air_fr_klm_prices = df_air_fr_klm_prices.interpolate()

    df_air_fr_klm_ratio = df_ratio_holders[["CAPITAL FUND MANAGEMENT"]]
    df_air_fr_klm_ratio['prices'] = df_air_fr_klm_prices


