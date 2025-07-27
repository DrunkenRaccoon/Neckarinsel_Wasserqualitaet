import os
import re
import io
import glob
import zipfile
from urllib.parse import urljoin

import pandas as pd
import requests


BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/"
CATEGORIES = {
    "air_temperature": "lufttemp",
    "moisture": "feuchtigkeit",
    "pressure": "druck",
    "soil_temperature": "boden",
    "solar": "strahlung",
    "sun": "sun",  # sunshine duration
}
ID_MIN = 4926
ID_MAX = 4933

DATA_DIR = os.path.join("Daten", "Wetter_2024")
RECENT_DIR = os.path.join(DATA_DIR, "recents")
os.makedirs(RECENT_DIR, exist_ok=True)


def fix_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["DATUM"] = df["MESS_DATUM"].astype(str).str.slice(0, 10)
    df["DATUM"] = pd.to_datetime(df["DATUM"], format="%Y%m%d%H")
    df.set_index("DATUM", inplace=True)
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    renames = {}
    return df.rename(columns=renames)


def remove_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["eor", "STATIONS_ID", "MESS_DATUM"], errors="ignore")


def download_file(url: str) -> bytes:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def list_files(url: str) -> list[str]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    pattern = re.compile(r"href=\"([^\"]+\.zip)\"")
    return pattern.findall(resp.text)


def extract_id(name: str) -> int | None:
    m = re.search(r"_(\d{5})_", name)
    if not m:
        return None
    return int(m.group(1))


def read_zip(content: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        txt_files = [f for f in z.namelist() if f.endswith(".txt")]
        if not txt_files:
            return pd.DataFrame()
        with z.open(txt_files[0]) as f:
            return pd.read_csv(f, sep=";", encoding="latin1")


def update_category(cat: str, short: str) -> None:
    url = urljoin(BASE_URL, f"{cat}/recent/")
    try:
        files = list_files(url)
    except Exception as exc:
        print(f"Failed listing {url}: {exc}")
        return

    target_dir = os.path.join(RECENT_DIR, cat)
    os.makedirs(target_dir, exist_ok=True)

    all_frames = []
    for file in files:
        sid = extract_id(file)
        if sid is None or sid < ID_MIN or sid > ID_MAX:
            continue
        dest_path = os.path.join(target_dir, file)
        if not os.path.exists(dest_path):
            try:
                content = download_file(urljoin(url, file))
                with open(dest_path, "wb") as fout:
                    fout.write(content)
            except Exception as exc:
                print(f"Failed download {file}: {exc}")
                continue
        else:
            with open(dest_path, "rb") as f:
                content = f.read()
        df = read_zip(content)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        return

    df_new = pd.concat(all_frames, ignore_index=True)
    raw_path = os.path.join(DATA_DIR, f"schnarrenberg_dwd_{short}.csv")
    if os.path.exists(raw_path):
        df_exist = pd.read_csv(raw_path, sep=";")
        df_new = pd.concat([df_exist, df_new], ignore_index=True)
        df_new.drop_duplicates(subset=["MESS_DATUM", "STATIONS_ID"], inplace=True)
    df_new.to_csv(raw_path, sep=";", index=False)

    df_clean = fix_dates(df_new.copy())
    df_clean = rename_columns(df_clean)
    df_clean = remove_duplicate_columns(df_clean)
    clean_path = os.path.join(DATA_DIR, f"clean_schnarrenberg_dwd_{short}.csv")
    if os.path.exists(clean_path):
        df_exist = pd.read_csv(clean_path)
        df_clean = pd.concat([df_exist, df_clean.reset_index()], ignore_index=True)
    df_clean.drop_duplicates(subset="DATUM", inplace=True)
    df_clean.to_csv(clean_path, index=False)


def aggregate_all() -> None:
    files = glob.glob(os.path.join(DATA_DIR, "clean_schnarrenberg_dwd_*.csv"))
    date_range = pd.date_range("2023-01-01", "2025-12-31 23:00:00", freq="h")
    df_agg = pd.DataFrame(index=date_range)
    for file in files:
        df_part = pd.read_csv(file)
        df_part["DATUM"] = pd.to_datetime(df_part["DATUM"])
        df_part.set_index("DATUM", inplace=True)
        df_part = remove_duplicate_columns(df_part)
        df_agg = df_agg.merge(df_part, how="left", left_index=True, right_index=True, suffixes=("", "_y"))
    df_agg.dropna(how="all", inplace=True)
    df_agg.to_csv(os.path.join(DATA_DIR, "clean_wetter_komplett.csv"))


if __name__ == "__main__":
    for cat, short in CATEGORIES.items():
        update_category(cat, short)
    aggregate_all()
