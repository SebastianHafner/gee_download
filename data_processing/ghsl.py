import ee


def get_ghsl():
    ghsl = ee.ImageCollection('users/ghsl/S2_CNN').mosaic()
    return ghsl