import requests 
import json
import os
from datetime import date
import logging
from typing import Optional, Iterator, Iterable, Union

from .ssl_v1_adapter import SSLv1Adapter

logger = logging.getLogger(__name__)

VEHICLES_URL = "https://api.cepik.gov.pl/pojazdy"

def iter_vehicles(
        date_from:date,
        date_to:date,
        voivodeship_code:str,
        session:Optional[requests.Session]=None,
        request_timeout=60,
        show_all_fields:str="true",
        show_only_registered_vehicles:str="true",
        record_limit_per_page:Union[str,int]=500,
) -> Iterator[dict]:
    logger.info("Initialised vehicle iterator function")
    
    if date_from > date_to:
        raise ValueError("Incorrect date range")
    
    params = {
        "wojewodztwo": voivodeship_code,
        "data-od": date_from.strftime("%Y%m%d"),
        "data-do": date_to.strftime("%Y%m%d"),
        "pokaz-wszystkie-pola": show_all_fields,
        "tylko-zarejestrowane":show_only_registered_vehicles,
        "limit":str(record_limit_per_page)
    }

    url, use_params = VEHICLES_URL, True 
    owns = session is None
    if owns:
        logger.info("Creating own requests session with SSLv1 adapter")
        session = requests.session()
        session.mount("https://", SSLv1Adapter())
    try:
        while url:
            result = session.get(
                url, 
                params=params if use_params else None,
                timeout=request_timeout )
            result.raise_for_status()
            payload = result.json()
            rows = payload.get("data") or []
            for row in rows:
                yield row
            use_params = False
            url = (payload.get("links") or {}).get("next")
    finally:
        if owns:
            logger.info("Closing requests session")
            session.close()


def write_ndjson(path:str, records:Iterable[dict]) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.part"
    logger.info(f"Saving vehicle data to temporary file: {tmp}")
    with open(tmp, "w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
    os.replace(tmp, path)
    logger.info(f"Saved vehicle data to {path}")
    return path