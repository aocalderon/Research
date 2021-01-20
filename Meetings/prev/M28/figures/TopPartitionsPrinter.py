from qgis.core import *
from qgis.gui import *
import qgis.utils

partitions = ["036", "048", "060", "072", "084", "096", "108"]
instance = QgsProject.instance()
for partition in partitions:
	name = "TopPartitions-{}".format(partition)
	layers = instance.mapLayersByName(name)
	for layer in layers:
		print(layer)
		for feature in layer.getFeatures():
			print(feature['duration'])
