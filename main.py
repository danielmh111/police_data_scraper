import json
from pathlib import Path

import requests
from loguru import logger
from project_paths import paths
from rich.pretty import pprint

LOCATIONS = paths.locations

STREETLEVEL_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime?"


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


def construct_url(location_names: list[str]) -> dict[str, str]:
    location_urls = {
        key: STREETLEVEL_BASE_URL + "poly=" + get_coords(LOCATIONS / (key + ".geojson"))
        for key in location_names
    }
    return location_urls


def make_request(url: str) -> list[dict]:
    response = requests.get(url=url)

    print(f"status code: {response.status_code}")

    if response.status_code == 200:
        return response.json()
    else:
        raise requests.HTTPError


def find_lsoas() -> list[str]:
    files = LOCATIONS.iterdir()
    names = [file.parts[-1].removesuffix(".geojson") for file in files]
    return names


def main():
    lsoas = [find_lsoas()[0]]
    lsoa_urls = construct_url(lsoas)
    data = {constituency: make_request(url) for constituency, url in lsoa_urls.items()}

    if data:
        pprint(data)


if __name__ == "__main__":
    main()
