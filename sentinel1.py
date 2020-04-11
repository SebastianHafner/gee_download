import ee
from gee.utils import compute_time_series_metrics


# getting list of feature names based on input parameters
def get_feature_names(polarizations: list, orbits: list, metrics: list):
    names = []
    for orbit in orbits:
        for pol in polarizations:
            for metric in metrics:
                names.append(f'{pol}_{orbit}_{metric}')
    return names


# retrieve sentinel-2 data for city
def get_time_series_features(bbox: ee.Geometry, from_date: str, to_date: str, orbit_numbers: dict,
                             polarizations: list, orbits: list, metrics: list, include_count: bool = False) -> ee.Image:

    # getting all sentinel-1 Synthetic Aperture Radar imagery
    s1 = ee.ImageCollection('COPERNICUS/S1_GRD')

    # sub-setting imagery intersecting bounding box of city
    s1 = s1.filterBounds(bbox)

    # sub-setting imagery to input year
    s1 = s1.filterDate(from_date, to_date)

    # selecting polarizations and IW imagery
    s1 = s1.filterMetadata('instrumentMode', 'equals', 'IW').select(polarizations)

    # masking backscatter lower than - 25 dB
    s1 = s1.map(lambda img: img.updateMask(img.gte(-25)))

    # rescaling from [-25, 5] to [0, 1]
    s1 = s1.map(lambda img: img.unitScale(-25, 5).clamp(0, 1).copyProperties(img))

    # getting features for both orbits
    features = []
    for orbit in ['ASCENDING', 'DESCENDING']:
        time_series_orbit = s1.filterMetadata('orbitProperties_pass', 'equals', orbit)

        # separating time series according to selected orbit numbers
        orbit_key = 'asc' if orbit == 'ASCENDING' else 'desc'
        if len(orbit_numbers.get(orbit_key)) > 0:
            orbit_features = []
            for orbit_number in orbit_numbers.get(orbit_key):
                time_series_single_orbit = s1.filterMetadata('relativeOrbitNumber_start', 'equals', orbit_number)
                print(f'Number of Sentinel-1 scenes ({orbit_key}): {time_series_single_orbit.size().getInfo()}')
                features_single_orbit = compute_time_series_metrics(time_series_single_orbit, polarizations, metrics)
                orbit_features.append(features_single_orbit)
            orbit_features = ee.ImageCollection(orbit_features).mosaic()
        else:
            orbit_features = ee.Image.cat(len(polarizations) * len(metrics) * [ee.Image(0)])

        # including orbit in feature names
        old_names = [f'{pol}_{metric}' for pol in polarizations for metric in metrics]
        new_names = [f'{pol}_{orbit_key}_{metric}' for pol in polarizations for metric in metrics]
        orbit_features = orbit_features.select(old_names, new_names)

        features.append(orbit_features)

    features = ee.Image.cat(features)
    features = features.select(get_feature_names(polarizations, orbits, metrics))

    if include_count:
        features = features.addBands(s1.reduce(ee.Reducer.count()).rename('count'))

    return features.unmask().float()
