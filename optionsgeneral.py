import requests

subsektor = [
    ("01", "Tanaman Pangan"),
    ("04", "Hortikultura"),
    ("02", "Perkebunan"),
    ("03", "Peternakan"),
]

indikator = {
    "Tanaman Pangan": [
        # ("00", "-- Pilih Indikator --"),
        ("0103", "LUAS PANEN"),
        ("0104", "PRODUKSI"),
        ("0105", "PRODUKTIVITAS"),
        ("0119", "LUAS TANAM"),
    ],
    "Hortikultura": [
        # ("00", "-- Pilih Indikator --"),
        ("0403", "LUAS PANEN"),
        ("0404", "PRODUKSI"),
        ("0405", "PRODUKTIVITAS"),
        ("0422", "Tanaman Menghasilkan")
    ],
    "Perkebunan": [
        # ("00", "-- Pilih Indikator --"),
        ("0201", "LUAS AREAL"),
        ("0205", "PRODUKSI"),
        ("0206", "PRODUKTIVITAS")
    ],
    "Peternakan": [
        # ("00", "-- Pilih Indikator --"),
        ("0301", "POPULASI"),
        ("0302", "PRODUKSI"),
    ]
}

level = [
    # ("00","-- Pilih Level --"),
    ("01","Nasional"),
    ("02","Provinsi"),
    ("03","Kabupaten"),
]

headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'dnt': '1',
    'origin': 'https://bdsp2.pertanian.go.id',
    'priority': 'u=1, i',
    'referer': 'https://bdsp2.pertanian.go.id/bdsp/id/komoditas',
    'sec-ch-ua': '"Not?A_Brand";v="99", "Chromium";v="130"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

def get_list_level(subsektor_code):
    params = {'subsektor': str(subsektor_code)}
    url = "https://bdsp2.pertanian.go.id/bdsp/id/subsektor/getLevelBySubsektor"
    r = requests.get(url, params=params, headers=headers)
    return r.json()

def get_list_provinsi():
    r = requests.get("https://bdsp2.pertanian.go.id/bdsp/id/lokasi/getProv")
    return r.json()

def get_list_kabupaten(prov_code):
    payload = {'fkode_prop': str(prov_code)}
    r = requests.post('https://bdsp2.pertanian.go.id/bdsp/id/lokasi/getKab', headers=headers, data=payload)
    return r.json()

def get_commodity_by_subsector(code: str):
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'dnt': '1',
        'origin': 'https://bdsp2.pertanian.go.id',
        'priority': 'u=1, i',
        'referer': 'https://bdsp2.pertanian.go.id/bdsp/id/indikator',
        'sec-ch-ua': '"Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    resp = requests.post(
        'https://bdsp2.pertanian.go.id/bdsp/id/subsektor/getKomBySubsektor',
        headers=headers,
        data={'subsektorcd': str(code)},
    )
    return resp

def get_list_komoditas(subsektor_code: str):
    array = list()
    res = get_commodity_by_subsector(subsektor_code)
    if res.status_code != 200:
        print("failed to collect KOMODITAS option")
        return {}
    rawdata = res.json()
    for item in rawdata:
        array.append((item.get("fkomcd"), item.get("fkomnm")))
    return array