import json
import time
from itertools import product
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from project_paths import paths
from rich.pretty import pprint

LOCATIONS = paths.locations

STREETLEVEL_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime"


def generate_months() -> list[str]:
    dates = [
        str(year) + "-" + str(month).rjust(2, "0")
        for year, month in product(range(2022, 2026), range(1, 13))
    ]
    return dates


def get_coords(polygon_file: Path) -> str:
    with open(polygon_file) as file:
        polygon = json.load(file)

    coords = polygon["coordinates"][0]
    logger.debug(f"coords created, length={len(coords)}")

    # rount to 4 digits - give precision of about 10 metres
    # chooseing to keep only every second coord to further reduce list
    # also, we are swapping over the coordinates - geojson stores coords in long, lat format, api takes lat, long points
    aprox_coords = list({(round(lat, 4), round(long, 4)) for long, lat in coords})[::2]
    logger.debug(f"aprox coords created, length={len(aprox_coords)}")

    formatted_coords = ":".join(
        [str(lon) + "," + str(lat) for lon, lat in aprox_coords]
    )

    return formatted_coords


def construct_url(location_names: list[str], dates: list[str]) -> list[tuple[str, str]]:
    params = product(dates, location_names)

    location_urls = [
        (
            location_name,
            STREETLEVEL_BASE_URL
            + "?date="
            + month
            + "&poly="
            + get_coords(LOCATIONS / (location_name + ".geojson")),
        )
        for month, location_name in params
    ]
    return location_urls


def make_request(url: str) -> list[dict] | None:
    try:
        response = requests.get(url=url)
    except requests.HTTPError:
        return None
    finally:
        time.sleep(0.25)

    logger.debug(f"status code: {response.status_code}")

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        logger.warning(f"404 status returned for {url}")


def find_lsoas() -> list[str]:
    files = LOCATIONS.iterdir()
    names = [file.parts[-1].removesuffix(".geojson") for file in files]
    return names


def format_data(data: list[dict[str, list[dict]]]) -> pl.DataFrame:
    flat_data = [
        {
            **crime,
            "lsoa": lsoa,
            "status": (crime.get("outcome_status") or {}).get("category"),
        }
        for report in data
        for lsoa, crimes in report.items()
        for crime in crimes
        if crimes != []
    ]

    pprint(flat_data)

    crimes_df = pl.DataFrame(flat_data)
    crimes_df = crimes_df.select(["lsoa", "month", "category", "status"])
    return crimes_df


def main():
    lsoas = [find_lsoas()[0]]
    lsoa_urls = construct_url(lsoas, generate_months())
    data: list[dict[str, list]] = [
        {lsoa: (make_request(url) or [])} for lsoa, url in lsoa_urls
    ]
    pprint(data)

    crimes_df = format_data(data)
    print(crimes_df)

    print(crimes_df.select("month").n_unique())


if __name__ == "__main__":
    main()
