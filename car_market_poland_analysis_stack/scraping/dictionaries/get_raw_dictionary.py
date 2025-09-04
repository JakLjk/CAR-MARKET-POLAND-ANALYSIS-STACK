import requests
import os
import json
from enum import Enum
import logging

from cepik.scraping.ssl_v1_adapter import SSLv1Adapter, requests_session_farbic

logger = logging.getLogger(__name__)

DICTIONARY_URL = "https://api.cepik.gov.pl/slowniki/{dict_name}"

class Dictionary(Enum):
    WOJEWODZTWA = "wojewodztwa"
    MARKA = "marka"
    RODZAJ_POJAZDU = "rodzaj-pojazdu"
    RODZAJ_PALIWA = "rodzaj-paliwa"
    POCHODZENIE_POJAZDU = "pochodzenie-pojazdu"
    SPOSOB_PRODUKCJI = "sposob-produkcji"

def get_dictionary(
        dict_name:Dictionary
    ):
    logger.info(f"Fetching dictionary {dict_name.name}")
    session = requests_session_farbic()
    response = session.get(
        DICTIONARY_URL.format(
            dict_name=dict_name.value
        ),
        timeout=(5, 20)
        )
    response.raise_for_status()
    data=response.json()
    if "data" not in data:
        raise ValueError("Unexpected json response - 'data' key is missing.")
    logger.info(f"Returning dictionary json")
    return data
    

def write_dictionary(
        path:str, 
        dictionary:dict) -> str:
    logger.info(f"Saving dictionary to {path}")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(dictionary, ensure_ascii=False, indent=2))
    logger.info(f"Saved dictionary to {path}")
    return path


    
