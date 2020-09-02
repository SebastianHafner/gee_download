import ee


def add_cloud_band(img: ee.Image) -> ee.Image:
    qa60 = img.select(['QA60'])
    clouds = qa60.bitwiseAnd(1 << 10).Or(qa60.bitwiseAnd(1 << 11))
    return img.addBands(clouds.rename('clouds'))


def add_stats(img: ee.Image) -> ee.Image:

    patch = ee.Geometry(img.get('patch'))
    patch_area = patch.area(0.001)

    img_footprint = ee.Algorithms.GeometryConstructors.Polygon(
        ee.Geometry(img.get('system:footprint')).coordinates()
    )
    intersection = patch.intersection(img_footprint, ee.ErrorMargin(0.001))
    intersection_area = intersection.area(0.001)
    coverage = ee.Number(intersection_area).divide(patch_area)

    cloud_area_img = img.select('clouds').multiply(ee.Image.pixelArea())
    stats = cloud_area_img.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=patch,
        scale=10,
        maxPixels=1e12
    )
    cloud_area = stats.get('clouds')
    cloud_coverage = ee.Number(cloud_area).divide(patch_area)
    img = img.set('patchCoverage', coverage)
    img = img.set('patchCloudCoverage', cloud_coverage)

    score = coverage.multiply(cloud_coverage)
    img = img.set('patchScore', score)
    return img


def cloud_free_mosaic(patch: ee.Geometry, date_range) -> ee.Image:

    s2toa = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(patch) \
        .map(add_cloud_band) \
        .map(lambda img: img.set('patch', patch)) \
        .map(add_stats)

    cloud_free_candidates = s2toa \
        .filterMetadata('patchCoverage', 'equals', 1) \
        .filterMetadata('patchCloudCoverage', 'equals', 0)

    best = cloud_free_candidates \
        .filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'less_than', 5) \
        .sort('CLOUDY_PIXEL_PERCENTAGE') \
        .first()

    best_local = cloud_free_candidates \
        .sort('CLOUDY_PIXEL_PERCENTAGE') \
        .first()

    s2toa_no_clouds = s2toa.map(lambda img: img.updateMask(img.select('clouds').Not()))

    cloud_free_mosaic = s2toa_no_clouds.sort('patchScore', False).mosaic()

    final = ee.ImageCollection([cloud_free_mosaic, best_local, best]).mosaic()

    return final


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


# TODO: add logging for verbose == True
def least_cloudy_scene(patch: ee.Geometry, date_range, verbose: bool = True) -> ee.Image:

    s2toa = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(patch) \
        .map(lambda img: img.set('patch', patch))
    s2clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(patch)

    join_condition = ee.Filter.equals(leftField='system:index', rightField='system:index')
    s2toa = ee.Join.saveFirst('cloudProbability').apply(primary=s2toa,
                                                        secondary=s2clouds,
                                                        condition=join_condition)

    s2toa = s2toa.map(add_patch_coverage).filterMetadata('patchCoverage', 'equals', 1)

    s2toa = s2toa.map(add_cloud_score).sort('cloudScore')

    img = s2toa.first()

    # TODO: remove this
    # print(img.getInfo())

    return img

