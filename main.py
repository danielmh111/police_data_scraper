import json
from pathlib import Path

from project_paths import paths
from rich.pretty import pprint

LOCATIONS = paths.locations

STREETLEVEL_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime?"


def get_coords(polygon_file: Path) -> str:
    with open(polygon_file) as file:
        polygon = json.load(file)

    coords = polygon["coordinates"][0]
    formatted_coords = ":".join([str(lon) + "," + str(lat) for lon, lat in coords])

    return formatted_coords


def construct_url(location_names: list[str]) -> dict[str, str]:
    location_urls = {
        key: STREETLEVEL_BASE_URL + get_coords(LOCATIONS / (key + ".geojson"))
        for key in location_names
    }
    return location_urls


def main():
    constituencies = [
        "bristol_east",
        "bristol_north",
        "bristol_south",
        "bristol_west",
    ]

    constituency_urls = construct_url(constituencies)

    pprint(constituency_urls)


if __name__ == "__main__":
    main()
