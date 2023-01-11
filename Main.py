from tqdm import tqdm
import pandas as pd
import urllib.request, json 
from concurrent.futures import ThreadPoolExecutor

def download_data(date):
    global ret
    global column_names
    day_url = f"{base_url}{date.date()}"
    with urllib.request.urlopen(day_url) as url:
        data = json.load(url)
        ret.extend(pd.DataFrame(data["PVPC"]).values.tolist())
        tmp_col_names = list(data["PVPC"][0].keys())
        if len(tmp_col_names)>len(column_names):
            column_names = tmp_col_names


year = 2022
base_url = "https://api.esios.ree.es/archives/70/download_json?locale=es&date="
ret = []
column_names = []
dates = pd.date_range(start=f"{year}-01-01",end=f"{year}-12-31")

with ThreadPoolExecutor() as executor:
    for _ in tqdm(executor.map(download_data, dates), total=len(dates)):
        pass

ret = pd.DataFrame(ret, columns=column_names)
ret = ret.astype({"Dia":'datetime64[ns]'})
ret = ret.sort_values(["Dia", "Hora"]).reset_index(drop=True)
ret.to_csv(f"data/pvpc_{year}.csv", index=False)

ret = pd.read_csv(f"data/pvpc_{year}.csv", decimal=",")
ret = ret.astype({"Dia":'datetime64[ns]'})
ret.to_excel(f"data/pvpc_{year}.xlsx")
ret.to_csv(f"data/pvpc_{year}.csv")