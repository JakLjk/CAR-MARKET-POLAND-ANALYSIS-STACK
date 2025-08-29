import requests
import os
import json
from enum import Enum


from ssl_v1_adapter import SSLv1Adapter

DICTIONARY_URL = "https://api.cepik.gov.pl/slowniki/{dict_name}"

class Dictionary(Enum):
    WOJEWODZTWA = "wojewodztwa"
    MARKA = "marka"
    RODZAJ_POJAZDU = "rodzaj-pojazdu"
    RODZAJ_PALIWA = "rodzaj-paliwa"
    POCHODZENIE_POJAZDU = "pochodzenie-pojazdu"
    SPOSOB_PRODUKCJI = "sposob-produkcji"

def get_dictionary(
        dict_name:Dictionary,
        save_to_file=None):
    session = requests.session()
    session.mount("https://", SSLv1Adapter())
    response = session.get(DICTIONARY_URL.format(
        dict_name=dict_name.value
        ))
    response.raise_for_status()
    data=response.json()
    if save_to_file:
        os.makedirs(os.path.dirname(save_to_file) or ".", exist_ok=True)
        with open(save_to_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return None
    else:
        return data
    
get_dictionary(Dictionary.WOJEWODZTWA, save_to_file="test_woj.json")