from pathlib import Path
from download_manager import args
from download_manager.config import config
import satellite_data
from data_processing import utils
import data_processing.building_footprints as bf
import json
import ee
import utm
import pandas as pd
import satellite_data
from data_processing.utils import load_json

SPACENET7_PATH = Path('C:/Users/shafner/urban_extraction/data/spacenet7/train')


def date_is_available(aoi_id: str, year: int, month: int):
    root_path = SPACENET7_PATH / aoi_id
    label_folder = root_path / 'labels_match'
    label_file = label_folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}_Buildings.geojson'
    return True if label_file.exists() else False


# uses udm to see whether there are unusable areas
def is_usable(aoi_id: str, year: int, month: int) -> bool:
    root_path = SPACENET7_PATH / aoi_id
    label_folder = root_path / 'labels'
    udm_file = label_folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}_UDM.geojson'

    feature_collection = load_json(udm_file)
    features = feature_collection['features']

    return True if len(features) == 0 else False


if __name__ == '__main__':

    # getting metadata from csv file
    metadata_file = SPACENET7_PATH.parent / 'sn7_metadata_v3.csv'
    metadata = pd.read_csv(metadata_file)

    for index, row in metadata.iterrows():
        aoi_id = str(row['aoi_id'])
        year = int(row['year'])
        month = int(row['month'])
        clouds = int(row['clouds'])
        udm_check = int(row['udm_check'])
        print(index, f'aoi_id: {aoi_id} - year: {year} - month: {month:02d} - clouds: {clouds}')

        if not date_is_available(aoi_id, year, month):
            raise Exception('Date not available')

        if not is_usable(aoi_id, year, month) and udm_check:
            raise Exception('Building footprints may be missing due to UDM')


