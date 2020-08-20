from qgis.core import *
from qgis.gui import *
import qgis.utils

PATH = "file:///home/and/Research/Meetings/next/figures"
CRS=6423
WKT_FIELD="wkt"
partitions = ["036", "048", "060", "072", "084", "096", "108"]
instance = QgsProject.instance()
for partition in partitions:
	name = "partition-{}".format(partition)
	layers = instance.mapLayersByName(name)
	for layer in layers:
		instance.removeMapLayer(layer)
for partition in partitions:
	name = "TopPartitions-{}".format(partition)
	layers = instance.mapLayersByName(name)
	for layer in layers:
		instance.removeMapLayer(layer)		
iface.mapCanvas().refresh()

for partition in partitions:
	uri = "{}/partitions-{}.wkt?delimiter={}&useHeader=yes&crs=epsg:{}&wktField={}".format(PATH, partition, "\\t", CRS, WKT_FIELD)
	layer = iface.addVectorLayer(uri, "partition-{}".format(partition), "delimitedtext")
	target_field = 'duration'
	myRangeList = []
	
	symbol = QgsSymbol.defaultSymbol(layer.geometryType())
	symbol.setColor(QColor(255,0,0,1))
	myRange = QgsRendererRange(0, 999.99, symbol, 'Less than 1s')
	myRangeList.append(myRange)
	
	symbol = QgsSymbol.defaultSymbol(layer.geometryType())
	symbol.setColor(QColor(255,0,0,100))
	myRange = QgsRendererRange(1000.0, 4999.99, symbol, 'Between 1s and 5s')
	myRangeList.append(myRange)
	
	symbol = QgsSymbol.defaultSymbol(layer.geometryType())
	symbol.setColor(QColor(255,0,0,200))
	myRange = QgsRendererRange(5000.0, 30100.00, symbol, 'More than 5s')
	myRangeList.append(myRange)

	myRenderer = QgsGraduatedSymbolRenderer(target_field, myRangeList)
	myRenderer.setMode(QgsGraduatedSymbolRenderer.Custom)
	
	layer.setRenderer(myRenderer)

root = QgsProject.instance().layerTreeRoot()
TPgroup = "Top Partitions"
group = root.findGroup(TPgroup)
try:
	group.removeAllChildren()
	root.removeChildrenGroupWithoutLayers()
except:
	print("An exception occurred")
group = root.addGroup(TPgroup)
for partition in partitions:
	uri = "{}/partitionsM-{}.wkt?delimiter={}&useHeader=yes&crs=epsg:{}&wktField={}".format(PATH, partition, "\\t", CRS, WKT_FIELD)
	layer = iface.addVectorLayer(uri, "TopPartitions-{}".format(partition), "delimitedtext")
	group.addLayer(layer)
	root.removeLayer(layer)

	
