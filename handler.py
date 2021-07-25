import logging
import os
from datetime import date
from json.decoder import JSONDecodeError
from typing import Dict, List

from supabase_py import Client, create_client
from tefas import Crawler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    today = date.today().isoformat()
    supabase = get_supabase_client()
    data = get_tefas_data(today)

    # Delete existing records
    res = supabase.table("prices").select("*").eq("date", today).execute()
    logger.info(f"Found {len(res['data'])} existing records for date={today}")
    if res["data"]:
        # Pass JSONDecodeError silently because of a bug in supabase-py
        # https://github.com/supabase/supabase-py/issues/22
        logger.info("Deleting existing records")
        try:
            supabase.table("prices").delete().eq("date", today).execute()
        except JSONDecodeError:
            pass

    # Insert new records
    logger.info("Inserting rew records")
    supabase.table("prices").insert(data).execute()


def get_tefas_data(date: str) -> List[Dict[str, str]]:
    client = Crawler()
    data = client.fetch(date)
    data["date"] = data["date"].apply(lambda x: x.isoformat())
    data = data[["date", "code", "price"]].to_dict("records")
    return data


def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    supabase = create_client(url, key)
    return supabase
