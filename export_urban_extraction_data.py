import json
import ee
from pathlib import Path
from gee import assets, sentinel1, sentinel2, export


def write_download_metadata(save_dir: Path, cities, year, patch_size, from_date, to_date, labels,
                            sentinel1_params, sentinel2_params):

    download_metadata = {
        'cities': cities,
        'year': year,
        'patch_size': patch_size,
        'from_date': from_date,
        'to_date': to_date,
        'labels': labels,
        'sentinel1': {
            'polarizations': sentinel1_params['polarizations'],
            'orbits': sentinel1_params['orbits'],
            'metrics': sentinel1_params['metrics']
        },
        'sentinel2': {
            'bands': sentinel2_params['bands'],
            'indices': sentinel2_params['indices'],
            'metrics': sentinel2_params['metrics']
        }
    }

    # print(download_metadata)
    with open(str(save_dir / 'download_metadata.json'), 'w', encoding='utf-8') as f:
        json.dump(download_metadata, f, ensure_ascii=False, indent=4)
    pass

if __name__ == '__main__':

    save_dir = Path('C:/Users/hafne/Desktop/projects/data/gee/')
    bucket = 'urban_extraction_stockholm_time_series'
    drive_folder = 'gee_test_exports'

    ee.Initialize()

    # cities to export
    cities = ['Stockholm']
    labels = ['bp']

    patch_size = 256

    # time series range
    year = 2018
    from_date = f'{year}-07-01'
    to_date = f'{year}-08-31'

    # sentinel 1 params
    sentinel1_params = {
        'polarizations': ['VV', 'VH'],
        'orbits': ['asc'],
        'metrics': ['mean', 'median',  'iqr', 'stdDev'],
        'include_count': False
    }

    # sentinel 2 params
    sentinel2_params = {
        'bands': ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2'],
        'indices': [],
        'metrics': ['median'],
        'include_count': False
    }


    for city in cities:

        bbox = assets.get_bbox(city)

        # sentinel 1
        sentinel1_params['orbit_numbers'] = assets.get_orbit_numbers(city)
        s1_features = sentinel1.get_time_series_features(bbox, from_date, to_date, **sentinel1_params)
        file_name = f'sentinel1_{city}_{year}'
        # export.to_drive(s1_features, bbox, drive_folder, file_name, patch_size)
        export.to_cloud(s1_features, bbox, bucket, 'sentinel1', file_name, patch_size)

        # sentinel 2
        s2_features = sentinel2.get_time_series_features(bbox, from_date, to_date, **sentinel2_params)
        file_name = f'sentinel2_{city}_{year}'
        # export.to_drive(s2_features, bbox, drive_folder, file_name, patch_size)
        export.to_cloud(s2_features, bbox, bucket, 'sentinel2', file_name, patch_size)

        # labels
        if 'wsf' in labels:
            if city == 'StockholmTest':
                wsf = assets.get_wsf('Stockholm')
            else:
                wsf = assets.get_wsf(city)
            file_name = f'wsf_{city}'
            # export.to_drive(wsf, bbox, drive_folder, file_name, patch_size)
            # export.to_cloud(wsf, bbox, bucket, 'wsf', file_name, patch_size)

        if 'realestate' in labels:
            real_estate = assets.get_real_estate_data_stockholm()
            file_name = f'realestate_{city}'
            # export.to_drive(real_estate, bbox, drive_folder, file_name, patch_size)
            # export.to_cloud(real_estate, bbox, bucket, 'realestate', file_name, patch_size)

        if 'bp' in labels:
            building_percentage = assets.get_building_percentage()
            file_name = f'bp_{city}'
            # export.to_drive(building_percentage, bbox, drive_folder, file_name, 0)
            export.to_cloud(building_percentage, bbox, bucket, 'bp', file_name, patch_size)

    write_download_metadata(save_dir, cities, year, patch_size, from_date, to_date, labels,
                            sentinel1_params, sentinel2_params)
