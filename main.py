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

    # round to 3 digits - getting 414 error with full length. Then, use set so coords are unique when less precise.
    # 3 decimal points gives precision of about 100 metres
    # chooseing to keep only every fourth coord to further reduce list
    aprox_coords = list({(round(lat, 3), round(long, 3)) for lat, long in coords})[::4]
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


def make_request(url: str) -> None:
    response = requests.get(url=url)

    print(f"status code: {response.status_code}")

    if response.status_code == 200:
        print("\n\n")
        print("response:")
        pprint(response.text)
        print("\n", "=" * 80, "\n")

        print("json:", "\n")
        print(response.json())


def main():
    constituencies = [
        "bristol_east",
        "bristol_north",
        "bristol_south",
        "bristol_west",
    ]

    constituency_urls = construct_url(constituencies)

    # pprint(constituency_urls)

    for const, url in constituency_urls.items():
        print("*" * 80, "\n\n")
        print(const)
        make_request(url)


if __name__ == "__main__":
    main()
