from tqdm import tqdm
import pandas as pd
import urllib.request, json 

year = 2018

base_url = "https://api.esios.ree.es/archives/70/download_json?locale=es&date="
ret = []
column_names = []

for date in tqdm(pd.date_range(start=f"{year}-01-01",end=f"{year}-12-31")):
    day_url = f"{base_url}{date.date()}"

    with urllib.request.urlopen(day_url) as url:
        data = json.load(url)
        ret.extend(pd.DataFrame(data["PVPC"]).values.tolist())

        tmp_col_names = list(data["PVPC"][0].keys())
        if len(tmp_col_names)>len(column_names):
            column_names = tmp_col_names

ret = pd.DataFrame(ret, columns=column_names)
ret.to_excel(f"pvpc_{year}.xlsx")

