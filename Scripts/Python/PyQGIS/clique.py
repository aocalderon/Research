epsilon = 10
groupName="Bigger cliques"
root = QgsProject.instance().layerTreeRoot()
group = root.addGroup(groupName)
layer = QgsProject.instance().mapLayersByName('Cliques')
layer = layer[0]
i = 1
for feature in layer.getFeatures():
	if feature.attribute("field_3") > 5.0:
		clique = QgsVectorLayer("Polygon", "Clique{}".format(i), "memory")
		crs = clique.crs()
		crs.createFromId(6423)
		clique.setCrs(crs)
		provider = clique.dataProvider()
		provider.addAttributes([
			QgsField("center", QVariant.String),
			QgsField("radious",  QVariant.Double),
			QgsField("n", QVariant.Int)
		])
		clique.updateFields() 
		QgsProject.instance().addMapLayer(clique, False)
		group.addLayer(clique) 
		provider.addFeature(feature)
		clique.updateExtents()
		clique.reload()
		i = i + 1
