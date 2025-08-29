import requests 
import json
import os
from datetime import date

from ssl_v1_adapter import SSLv1Adapter

VEHICLES_URL = "https://api.cepik.gov.pl/pojazdy"

def get_vehicle_information(
        date_from:date, 
        date_to:date, 
        voivodeship:str,
        save_to_file=None
        ):
    params = {
        "wojewodztwo": voivodeship,
        "data-od": date_from.strftime("%Y%m%d"),
        "data-do": date_to.strftime("%Y%m%d"),
        "pokaz-wszystkie-pola": "true",
    }
    session = requests.session()
    session.mount("https://", SSLv1Adapter())
    response = session.get(VEHICLES_URL, params=params)
    response.raise_for_status()
    data=response.json()
    #TODO implementation of multi-page request return
    if save_to_file:
        os.makedirs(os.path.dirname(save_to_file) or ".", exist_ok=True)
        with open(save_to_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return None
    else:
        return data
    
get_vehicle_information(date(2025,8,20), date(2025,8,22), "04", "test_veh.json")