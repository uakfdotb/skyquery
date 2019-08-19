# convert geotiffs to pngs

import libtiff
import numpy
from PIL import Image
import scipy.misc
import sys

THRESHOLD = -32

im = libtiff.TIFF.open(sys.argv[1]).read_image()[:, :, 0:3]
surface = libtiff.TIFF.open(sys.argv[2]).read_image()
surface = numpy.logical_and(surface > -55, surface < THRESHOLD)
surface = surface.astype('uint8')*255
surface = scipy.misc.imresize(surface, (im.shape[0], im.shape[1]), interp='nearest')
Image.fromarray(im).save(sys.argv[3])
im = numpy.minimum(im, numpy.stack([surface, surface, surface], axis=2))
Image.fromarray(im).save(sys.argv[4])
