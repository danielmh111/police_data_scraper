import json
from itertools import product
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from project_paths import paths
from ratelimit import limits
from requests.adapters import HTTPAdapter
from rich.pretty import pprint
from shapely.geometry import Polygon
from urllib3.util.retry import Retry

LOCATIONS = paths.locations
DATA = paths.data

STREETLEVEL_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime"


def generate_months() -> list[str]:
    dates = [
        str(year) + "-" + str(month).rjust(2, "0")
        for year, month in product(range(2022, 2026), range(1, 13))
    ]
    return dates


def format_coordinates(shape: Polygon) -> str:
    coords = list(shape.exterior.coords)
    formatted_coords = ":".join([str(lon) + "," + str(lat) for lon, lat in coords])
    return formatted_coords


def get_coords(polygon_file: Path) -> str:
    with open(polygon_file) as file:
        polygon = json.load(file)

    coords = polygon["coordinates"][0]
    logger.debug(f"coords created, length={len(coords)}")

    # round to 5 digits - give precision of about 1 metres
    # also, we are swapping over the coordinates - geojson stores coords in long, lat format, api takes lat, long points
    aprox_coords = [(round(lat, 5), round(long, 5)) for long, lat in coords]
    deduped_coords = list(
        dict.fromkeys(aprox_coords)
    )  # dedupes but keeps order - crucial for polygon

    # using the shapely library to simplify the polygon so that urls dont get so long they raise 414 errors
    area_polygon = Polygon(aprox_coords)

    formatted_coords = format_coordinates(area_polygon)
    tolerance = 0.000001
    while len(formatted_coords) > 300:
        area_polygon = area_polygon.simplify(tolerance, preserve_topology=True)
        formatted_coords = format_coordinates(area_polygon)
        tolerance *= 1.25  # increase by 50% each iteration

    logger.debug(f"simplified coords created, length={len(deduped_coords)}")

    return formatted_coords


def construct_url(location_names: list[str], dates: list[str]) -> list[tuple[str, str]]:
    location_coords = {
        location_name: get_coords(LOCATIONS / (location_name + ".geojson"))
        for location_name in location_names
    }

    params = product(dates, location_names)

    location_urls = [
        (
            location_name,
            STREETLEVEL_BASE_URL
            + "?date="
            + month
            + "&poly="
            + location_coords.get(location_name, ""),
        )
        for month, location_name in params
    ]
    return location_urls


@limits(calls=15, period=1)
def make_request(url: str, session: requests.Session) -> list[dict]:
    response = session.get(url=url)
    logger.debug(f"status code: {response.status_code}")

    if response.status_code == 404:
        return []  # return empty list - no crimes reported
    if response.status_code != 200:
        logger.warning(f"{response.status_code} returned for {url}")
        response.raise_for_status()

    return response.json()


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

    crimes_df = pl.DataFrame(flat_data)
    crimes_df = crimes_df.select(["lsoa", "month", "category", "status"])
    return crimes_df


def aggregate_stats(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.select(["lsoa", "month", "category"])
        .group_by(["lsoa", "month", "category"])
        .len()
        .rename({"len": "count"})
    )
    print(df)
    return df


def main():
    lsoas = find_lsoas()
    lsoa_urls = construct_url(lsoas, generate_months())

    retry_logic = Retry(
        total=3,
        status_forcelist=[429, 500],
        backoff_factor=1,
        respect_retry_after_header=True,
    )
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry_logic))
        data: list[dict[str, list]] = [
            {lsoa: (make_request(url, session))} for lsoa, url in lsoa_urls
        ]

    crimes_df = format_data(data)
    crimes_df.write_csv(DATA / "lsoa_crimes")

    crime_stats_df = aggregate_stats(crimes_df)
    crime_stats_df.write_csv(DATA / "lsoa_crime_stats.csv")


if __name__ == "__main__":
    main()
