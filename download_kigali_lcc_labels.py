import ee

from download_manager import args
from download_manager.config import config

if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    scale = cfg.PIXEL_SPACING
    seeds = ee.Algorithms.Image.Segmentation.seedGrid()

    pass