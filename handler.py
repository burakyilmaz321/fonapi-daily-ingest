import logging
import os
from datetime import date
from json.decoder import JSONDecodeError
from typing import Dict, List

from supabase_py import Client, create_client
from tefas import Crawler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Tables:
    PRICE_HISTORY: str = "price_history"
    FUNDS: str = "funds"


def run(event, context):
    today = date.today().isoformat()
    supabase = get_supabase_client()
    data = get_tefas_data(today)
    fund_ids = get_fund_ids(supabase)
    data = join_fund_ids(data, fund_ids)

    # Delete existing records
    res = supabase.table(Tables.PRICE_HISTORY).select("*").eq("date", today).execute()
    logger.info(f"Found {len(res['data'])} existing records for date={today}")
    if res["data"]:
        # Pass JSONDecodeError silently because of a bug in supabase-py
        # https://github.com/supabase/supabase-py/issues/22
        logger.info("Deleting existing records")
        try:
            supabase.table(Tables.PRICE_HISTORY).delete().eq("date", today).execute()
        except JSONDecodeError:
            pass

    # Insert new records
    logger.info("Inserting rew records")
    supabase.table(Tables.PRICE_HISTORY).insert(data).execute()


def get_tefas_data(date: str) -> List[Dict[str, str]]:
    client = Crawler()
    data = client.fetch(date)
    data["date"] = data["date"].apply(lambda x: x.isoformat())
    data = data[["date", "code", "price"]].to_dict("records")
    return data


def get_fund_ids(supabase: Client) -> Dict[str, int]:
    funds = supabase.table(Tables.FUNDS).select("code, id").execute().get("data")
    funds = {fund["code"]: fund["id"] for fund in funds}
    return funds


def join_fund_ids(data: List[Dict[str, str]], fund_ids: Dict[str, int]) -> List[Dict[str, str]]:
    joined_data = []
    for datum in data:
        try:
            datum["fund_id"] = fund_ids[datum["code"]]
        except KeyError:
            logger.warning(f"Cannot find {datum['code']} in `funds`.")
        else:
            del datum["code"]
            joined_data.append(datum)
    return joined_data


def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    supabase = create_client(url, key)
    return supabase
