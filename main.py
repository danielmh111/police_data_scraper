import json
from itertools import product
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from project_paths import paths
from ratelimit import limits
from requests.adapters import HTTPAdapter
from shapely.geometry import Polygon
from urllib3.util.retry import Retry

LOCATIONS = paths.locations
DATA = paths.data

STREETLEVEL_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime"


def generate_months(start_year: int, end_year: int) -> list[str]:
    """
    generate_months
    ---

    a function for creating all the months between the start and end year as strings in the format yyyy-mm.
    returns a list of strings.

    Params:
        start_year (int): the first year to include in the range of months. expects a four digit integer
        end_year (int): the last year to include in the range of months. expects a four digit integer

    Returns:
        months (list of strs): the list of formatted months.
    """
    dates = [
        str(year) + "-" + str(month).rjust(2, "0")
        for year, month in product(range(start_year, end_year), range(1, 13))
    ]
    return dates


def format_coordinates(shape: Polygon) -> str:
    """
    format_coordinates
    ---

    a function for formatting the coordinates in the format expected by the uk.police.data api,
    given a polygon of the custom area being queried.

    Params:
        shape (shapely.Polygon): the geometry of the custom area. Create a polygon using the shapely library and teh longitude-latitude coordinate pairs.

    Returns::
        coordinates (str): coordinates formatted to be included as a request parameter
    """
    coords = list(shape.exterior.coords)
    formatted_coords = ":".join([str(lon) + "," + str(lat) for lon, lat in coords])
    return formatted_coords


def get_coords(polygon_file: Path) -> str:
    """
    get_coords
    ---

    a function that takes a path to a geojson file and returns formatted coordinates ready to be used as a request parameter.
    The function will read from the file, extract the coordinates, simplify the polygon to fit in a url, format the coordinates.

    Params:
        polygon_file (pathlib.Path): the full or relative file path to the geojson

    Returns:
        formatted_coords (str): coordinates formatted to be included as a request parameter
    """
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
        tolerance *= 1.25  # increase by 25% each iteration

    logger.debug(f"simplified coords created, length={len(deduped_coords)}")

    return formatted_coords


def construct_url(location_names: list[str], dates: list[str]) -> list[tuple[str, str]]:
    """
    construct_url
    ---

    a function for formatting urls to use in requests to the uk.police.data api.
    Given a list of location names (lsoas), the function will call `get_coords`
    to extract formatted coordinates for that area from a file witht the matching name.
    These are included as parameters in the url, along with the given dates.

    Params:
        location_name (list of strs): the names of the lsoa areas to request
        dates (list of strs): the months in yyyy-mm format to request data for

    Returns;
        urls (list of tuples of strs): each tuple is a location_name - url pair.
                                    This is so the name of the area can be kept linked when the request is made.

    """
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
    """
    make_requests
    ---

    a function for making the request to the uk.police.data api given a single and a session. It is rate limitted to 15 requests per second.

    Params:
        url (str): the url to request data from

    Raises:
        HTTPError: raised for http error status codes unless the status code is 404

    Returns:
        data (list of dicts): deserialized json response - a list of a dictionary for each crime, or an empty dictionary if a 404 error occured (api has archived that month)
    """
    response = session.get(url=url)
    logger.debug(f"status code: {response.status_code}")

    if response.status_code == 404:
        return []  # return empty list - no crimes reported
    if response.status_code != 200:
        logger.warning(f"{response.status_code} returned for {url}")
        response.raise_for_status()

    return response.json()


def find_lsoas() -> list[str]:
    """
    find_lsoas
    ---

    this function discovers what files are in the locations folder, and returns that names of the lsoas based on the file names

    Returns:
        names (list of strs): a list of the names of lsoas that have a geometry file
    """
    files = LOCATIONS.iterdir()
    names = [file.parts[-1].removesuffix(".geojson") for file in files]
    return names


def format_data(data: list[dict[str, list[dict]]]) -> pl.DataFrame:
    """
    format_data
    ---

    take the data returned from all api calls and create a dataframe

    Params:
        data (list of dicts of strs and lists of dicts (sorry)): the data structure created by making all the api requests

    Returns:
        df (polars.Dataframe): a tabular dataframe of the data
    """
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
    """
    aggregate_stats
    ---

    take the raw dataframe and produce the count of crimes in each category, in each LSOA, in each month.

    Params:
        df (polars.DataFrame): the unaggregated data in a dataframe (as returned by `format_data`)

    Returns:
        aggregated_df (polars.DataFrame): the dataframe with the aggregated `count` column calculated
    """
    df = (
        df.select(["lsoa", "month", "category"])
        .group_by(["lsoa", "month", "category"])
        .len()
        .rename({"len": "count"})
    )
    print(df)
    return df


def main():
    """
    the main entrypoint for the program collection data from uk.police.data api per LSOA.
    """
    lsoas = find_lsoas()
    lsoa_urls = construct_url(lsoas, generate_months(2022, 2025))

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
