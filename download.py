import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import requests
from fake_useragent import UserAgent

if __name__ == "__main__":
    ua = UserAgent()

    download_path = Path("./download")

    df = pd.read_csv("./missing.csv")

    url_pattern = "https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/urbana/SHP/{entidad}/SHP/{upc}.zip"

    for idx, row in df.iterrows():
        entidad = row["entidad"]
        upc = row["upc"]

        dir_path = download_path / entidad / upc
        if dir_path.exists() and len(list(dir_path.iterdir())) > 0:
            print(f"Already downloaded {entidad} {upc}")
            continue

        url = url_pattern.format(entidad=entidad, upc=upc + "_s")

        r = requests.get(
            url, stream=True, timeout=200, headers={"User-Agent": ua.random}
        )

        if r.status_code == 200:
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_file:
                temp_file_path = Path(temp_file.name)

                for chunk in r.iter_content(chunk_size=8192):
                    temp_file.write(chunk)

            extract_path = download_path / entidad / upc
            extract_path.mkdir(parents=True, exist_ok=True)

            try:
                with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                    zip_ref.extractall(extract_path)
            except zipfile.BadZipFile:
                print(url)
                raise

            temp_file_path.unlink()

            print(idx / len(df))
        else:
            print(f"Failed to download {url}")
