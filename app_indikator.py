import concurrent.futures
import csv
import os
import random
import time

import curlify
import requests
from requests.adapters import HTTPAdapter
from rich import print
from selectolax.parser import HTMLParser
from urllib3.util import Retry

import optionsgeneral as op

NUM_THREADS = 5

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "dnt": "1",
    "origin": "https://bdsp2.pertanian.go.id",
    "priority": "u=1, i",
    "referer": "https://bdsp2.pertanian.go.id/bdsp/id/indikator",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}


def download_indikator(subsektor, komoditas, level, prov, kab, awal, akhir):
    subsektor_code = str(subsektor[0])
    subsektor_name = str(subsektor[1])

    komoditas_code = str(komoditas[0])
    komoditas_name = str(komoditas[1])

    level_code = str(level.get("flevelcd"))
    level_name = str(level.get("flevelnm"))

    prov_code = prov.get("fkode_prop")
    prov_name = prov.get("nama_prop")

    kab_code = kab.get("fkode_kab")
    kab_name = kab.get("nama_kab")

    data = {
        "subsektor": subsektor_name,
        "komoditas": komoditas_code,
        "level": level_code,
        "prov": prov_code,
        "kab": kab_code,
        "satuan": "00",
        "sts_angka": "6",
        "sumb_data": "00",
        "tahunAwal": str(awal),
        "tahunAkhir": str(akhir),
        "subsektorcd": subsektor_code,
        "subsektornm": subsektor_name,
        "level": level_code,
        "levelnm": level_name,
        "prov": prov_code,
        "provnm": prov_name,
        "kab": kab_code,
        "kabnm": kab_name,
        "sts_angka": "6",
        "sts_angkanm": "Angka Tetap",
        "sumb_data": "00",
        "sumb_datanm": "-- Pilih Sumber Data --",
        "satuannm": "-- Pilih Satuan --",
        "komoditas": komoditas_code,
        "komoditasnm": komoditas_name,
    }

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    url = "https://bdsp2.pertanian.go.id/bdsp/id/indikator/result"
    response = session.post(url, headers=HEADERS, data=data, timeout=180)

    curl_command = curlify.to_curl(response.request)
    print(curl_command)
    print(response.status_code)

    return response


def parse_html_tbl(r):
    if r.status_code != 200:
        return None
    h = HTMLParser(r.text)
    ht = h.css_first("table#example")
    thead = [head.text() for head in ht.css("thead tr > td")]
    # content
    table_content = []
    for row in ht.css("tbody tr"):
        row_content = []
        for cell in row.css("td"):
            row_content.append(cell.text())
        if len(row_content) != len(thead):
            print("WARNING: header length and column lenght is different")
            return None
        table_content.append({head: row_content[i] for i, head in enumerate(thead)})
    return table_content


def enrich_json_tbl(data, subsektor, komoditas, provinsi, kabupaten):
    if data:
        content = []
        for item in data.copy():
            item.update(
                {
                    "Subsektor": subsektor,
                    # "Indikator": indikator,
                    "Komoditas": komoditas,
                    "Provinsi": provinsi,
                    "Kabupaten": kabupaten,
                }
            )
            content.append(item)
        return content


def validate_number(num):
    factor = 1 / 100
    return "{:.2f}".format(float(num) * factor)


def apply_factor_to_column(df, awal, akhir):
    for n in range(int(awal), int(akhir) + 1):
        df[str(n)] = df[str(n)].apply(validate_number)
    return df


def save_json_tbl(data, filename):
    header = list(data[0].keys())
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)


# CAUTION: still contain hardcode inside this function
def scrape(kwargs):
    # args = subsektor, level, prov, kab, awal, akhir, datapath
    datapath = kwargs.get("datapath", "data")
    if not os.path.exists(datapath):
        os.makedirs(datapath)
    subsektor_target = kwargs.get("subsektor_target")
    level = kwargs.get("level")
    prov = kwargs.get("prov")
    kab = kwargs.get("kab")
    awal = kwargs.get("awal")
    akhir = kwargs.get("akhir")
    prov_name = prov.get("nama_prop")
    kab_nama = kab.get("nama_kab")
    level_nama = level.get("flevelnm")
    subsektors = op.subsektor
    for subsektor_cd, subsektor_nm in subsektors:
        # only on subsektor target
        if subsektor_nm == subsektor_target:
            for komoditas in op.get_list_komoditas(subsektor_cd):
                # filter only padi
                # if "padi" not in komoditas[1].lower():
                #     continue
                print(
                    subsektor_nm,
                    komoditas[1],
                    level_nama,
                    prov_name,
                    kab_nama,
                    awal,
                    akhir,
                )
                r = download_indikator(
                    (subsektor_cd, subsektor_nm),
                    komoditas,
                    level,
                    prov,
                    kab,
                    awal,
                    akhir,
                )
                if r.status_code != 200:
                    print(
                        "WARNING",
                        r.status_code,
                        prov_name,
                        kab_nama,
                        subsektor_nm,
                        komoditas[1],
                    )
                time.sleep(random.randint(5, 20) / 10)
                try:
                    data = parse_html_tbl(r)
                    if not data:
                        continue
                    fname = f"{datapath}/Indikator - {subsektor_nm.replace('/', '-')} - {komoditas[1].replace('/', '-')} - {prov_name} - {kab_nama}.csv"
                    data = enrich_json_tbl(
                        data,
                        subsektor=subsektor_nm,
                        komoditas=komoditas[1],
                        provinsi=prov_name,
                        kabupaten=kab_nama,
                    )
                    save_json_tbl(data, fname)
                except Exception as e:
                    print(e)
                    time.sleep(random.randint(1, 3))


def main(
    subsektor_target: str, level_target: str, awal: int, akhir: int, datapath: str
):
    subsektor_code = list(filter(lambda x: x[1] == subsektor_target, op.subsektor))[0][
        0
    ]
    prov_list = op.get_list_provinsi()
    for m, level in enumerate(op.get_list_level(subsektor_code)):
        if level.get("flevelnm") == level_target:
            for i, prov in enumerate(prov_list):
                # skip province
                # if (
                #     bool(re.findall(r"Sumatera", prov["nama_prop"]))
                #     or bool(re.findall(r"Riau", prov["nama_prop"]))
                #     or bool(re.findall(r"Bangka", prov["nama_prop"]))
                #     or bool(re.findall(r"Palembang", prov["nama_prop"]))
                #     or bool(re.findall(r"Lampung", prov["nama_prop"]))
                #     or bool(re.findall(r"Aceh", prov["nama_prop"]))
                #     or bool(re.findall(r"Jambi", prov["nama_prop"]))
                #     or bool(re.findall(r"Bengkulu", prov["nama_prop"]))
                #     or bool(re.findall(r"Jawa", prov["nama_prop"]))
                #     or bool(re.findall(r"Jakarta", prov["nama_prop"]))
                #     or bool(re.findall(r"Banten", prov["nama_prop"]))
                #     or bool(re.findall(r"Yogya", prov["nama_prop"]))
                #     or bool(re.findall(r"Kalimantan", prov["nama_prop"]))
                #     or bool(re.findall(r"Bali", prov["nama_prop"]))
                #     or bool(re.findall(r"Nusa Tenggara", prov["nama_prop"]))
                #     or bool(re.findall(r"Gorontalo", prov["nama_prop"]))
                #     or bool(re.findall(r"Sulawesi", prov["nama_prop"]))
                # ):
                #     continue
                prov_code = prov.get("fkode_prop")
                kab_list = list()
                if level_target == "Kabupaten":
                    kab_list = op.get_list_kabupaten(prov_code)
                if level_target == "Provinsi":
                    kab_list = [
                        {"fkode_kab": "00", "nama_kab": "--- Pilih Kabupaten ---"}
                    ]
                args = []
                for j, kab in enumerate(kab_list):
                    args.append(
                        {
                            "subsektor_target": subsektor_target,
                            "level": level,
                            "prov": prov,
                            "kab": kab,
                            "awal": str(awal),
                            "akhir": str(akhir),
                            "datapath": datapath,
                        }
                    )
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=NUM_THREADS
                ) as executor:
                    executor.map(scrape, args, timeout=25)


if __name__ == "__main__":
    subsektor_target = "Tanaman Pangan"
    level_target = "Kabupaten"
    datapath = "data/tanaman-pangan"
    main(subsektor_target, level_target, 1970, 2025, datapath)
