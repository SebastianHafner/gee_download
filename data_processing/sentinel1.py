import ee


def single_orbit_mean(patch: ee.Geometry, date_range) -> ee.Image:

    # sup-setting data
    col = ee.ImageCollection('COPERNICUS/S1_GRD') \
        .filterBounds(patch) \
        .filterDate(date_range.start(), date_range.end()) \
        .filterMetadata('instrumentMode', 'equals', 'IW') \
        .filterMetadata('transmitterReceiverPolarisation', 'equals', ['VV', 'VH'])

    # masking noise
    col = col.map(lambda img: img.updateMask(img.gte(-25)))

    # using orbit with more scenes
    asc_col = col.filterMetadata('orbitProperties_pass', 'equals', 'ASCENDING')
    desc_col = col.filterMetadata('orbitProperties_pass', 'equals', 'DESCENDING')
    col = ee.Algorithms.If(ee.Number(asc_col.size()).gt(desc_col.size()), asc_col, desc_col)
    col = ee.ImageCollection(col)

    # getting distinct orbit numbers
    orbit_numbers = col \
        .toList(col.size()) \
        .map(lambda img: ee.Number(ee.Image(img).get('relativeOrbitNumber_start'))) \
        .distinct().getInfo()

    # computing separate mean backscatter image for each orbit number
    means = ee.ImageCollection([])
    for number in orbit_numbers:
        mean = col.filterMetadata('relativeOrbitNumber_start', 'equals', number).mean()
        means = means.merge(ee.ImageCollection([mean]))

    mosaic = means.mosaic()

    mosaic = mosaic.unitScale(-25, 0).clamp(0, 1)

    return mosaic


def single_orbit_metrics(patch: ee.Geometry, date_range) -> ee.Image:
    # sup-setting data
    col = ee.ImageCollection('COPERNICUS/S1_GRD') \
        .filterBounds(patch) \
        .filterDate(date_range.start(), date_range.end()) \
        .filterMetadata('instrumentMode', 'equals', 'IW')  \
        .filterMetadata('transmitterReceiverPolarisation', 'equals', ['VV', 'VH'])

    # masking noise
    col = col.map(lambda img: img.updateMask(img.gte(-25)))

    # using orbit with more scenes
    asc_col = col.filterMetadata('orbitProperties_pass', 'equals', 'ASCENDING')
    desc_col = col.filterMetadata('orbitProperties_pass', 'equals', 'DESCENDING')
    col = ee.Algorithms.If(ee.Number(asc_col.size()).gt(desc_col.size()), asc_col, desc_col)
    col = ee.ImageCollection(col)

    # getting distinct orbit numbers
    orbit_numbers = col \
        .toList(col.size()) \
        .map(lambda img: ee.Number(ee.Image(img).get('relativeOrbitNumber_start'))) \
        .distinct().getInfo()

    # computing separate mean backscatter image for each orbit number
    means = ee.ImageCollection([])
    stds = ee.ImageCollection([])
    for number in orbit_numbers:
        col_orbit = col.filterMetadata('relativeOrbitNumber_start', 'equals', number)
        mean = col_orbit.reduce(ee.Reducer.mean())
        std = col_orbit.reduce(ee.Reducer.stdDev())
        means = means.merge(ee.ImageCollection([mean]))
        stds = stds.merge(ee.ImageCollection([std]))

    mean_mosaic = means.mosaic()
    std_mosaic = stds.mosaic()

    # normalization
    mean_mosaic = mean_mosaic.unitScale(-25, 0).clamp(0, 1)
    std_mosaic = std_mosaic.unitScale(0, 10).clamp(0, 1)

    mosaic = mean_mosaic.addBands(std_mosaic)

    return mosaic

