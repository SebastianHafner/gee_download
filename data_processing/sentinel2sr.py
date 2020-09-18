import ee


def add_patch_coverage(img: ee.Image) -> ee.Image:
    img_footprint = ee.Algorithms.GeometryConstructors.Polygon(
        ee.Geometry(img.get('system:footprint')).coordinates()
    )

    patch = ee.Geometry(img.get('patch'))
    patch_area = patch.area(0.001)

    intersection_poly = patch.intersection(img_footprint, ee.ErrorMargin(0.001))
    intersection_area = intersection_poly.area(0.001)
    coverage = ee.Number(intersection_area).divide(patch_area)

    img = img.set('patchCoverage', coverage)
    return img


def add_cloud_score(img: ee.Image) -> ee.Image:
    patch = ee.Geometry(img.get('patch'))
    cloud_probability = ee.Image(img.get('cloudProbability'))
    stats = cloud_probability.select('probability').reduceRegion(reducer=ee.Reducer.sum(),
                                                                 geometry=patch,
                                                                 scale=10,
                                                                 maxPixels=1e12)
    cloud_score = stats.get('probability')
    img = img.set('cloudScore', cloud_score)
    return img


def least_cloudy_scene(patch: ee.Geometry, date_range) -> ee.Image:
    s2sr = ee.ImageCollection('COPERNICUS/S2_SR') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(patch) \
        .map(lambda img: img.set('patch', patch))
    s2clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(patch)

    join_condition = ee.Filter.equals(leftField='system:index', rightField='system:index')
    s2sr = ee.Join.saveFirst('cloudProbability').apply(primary=s2sr,
                                                        secondary=s2clouds,
                                                        condition=join_condition)

    s2sr = s2sr.map(add_patch_coverage).filterMetadata('patchCoverage', 'equals', 1)

    s2sr = s2sr.map(add_cloud_score)
    s2sr = ee.ImageCollection(s2sr)

    img = s2sr.sort('cloudScore', False).mosaic()

    img = img.unitScale(0, 10_000).clamp(0, 1)

    return img


