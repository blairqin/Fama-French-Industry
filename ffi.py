import pandas as pd
import numpy as np
import io
import requests
import zipfile

if __name__ == "__main__":
    urls = [
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes49.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes48.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes38.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes30.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes17.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes12.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes10.zip"
        , "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes5.zip"
    ]
    for url in urls:
        dname = url.split("/")[-1].split(".")[0].lower()
        r = requests.get(url)
        f = io.BytesIO(r.content)
        f = zipfile.ZipFile(f)
        name = f.namelist()[0]
        with f.open(name) as file:
            data = file.read()
            dta = io.BytesIO(data)
        df = pd.read_fwf(fdta, widths=[2, 10000]
                         , names=["ffi", "desp"]
                         , dtype={"ffi": "object", "desp": "object"})
        df = df[pd.isna(df["desp"]) == False]
        df["ffi"] = pd.to_numeric(df["ffi"], errors="coerce")
        df["ffi"] = df["ffi"].astype("Int64")
        df["_ffi_desp"] = np.nan
        df.loc[pd.isna(df["ffi"]) == False, "_ffi_desp"] = df["desp"]
        df["ffi"] = df["ffi"].ffill()
        df["_ffi_desp"] = df["_ffi_desp"].ffill()
        df = df[df["desp"] != df["_ffi_desp"]]
        df["ffi_name"] = df["_ffi_desp"].apply(lambda x: x.split()[0])
        df["ffi_desp"] = df["_ffi_desp"].apply(lambda x: " ".join(x.split()[1:]))
        df = df.drop(["_ffi_desp"], axis=1)
        df["sic_s"] = df["desp"].apply(lambda x: x.split()[0].split("-")[0])
        df["sic_e"] = df["desp"].apply(lambda x: x.split()[0].split("-")[1])
        df["sic_desp"] = df["desp"].apply(lambda x: " ".join(x.split()[1:]))
        df = df.drop(["desp"], axis=1)
        cols = ["ffi_name", "ffi_desp", "sic_desp"]
        cols_len = []
        for col in cols:
            max_len = max(df[col].apply(len))
            max_len = ceil(max_len / 5) * 5
            cols_len.append(max_len)
        sql_type = {
            "ffi": Integer
            , "ffi_name": String(cols_len[0])
            , "ffi_desp": String(cols_len[1])
            , "sic_s": String(4)
            , "sic_e": String(4)
            , "Description": String(cols_len[2])
        }
        df.to_sql(dname, engine, schema="sec", if_exists="replace", index=False, dtype=sql_type)