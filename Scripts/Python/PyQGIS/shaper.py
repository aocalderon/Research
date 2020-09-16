epsilon = 10.0
layer = QgsProject.instance().mapLayersByName('biggest')
layer = layer[0]

def isLeft(a: QgsPoint, b: QgsPoint, c: QgsPoint) -> bool:
	return ((b.x() - a.x()) * (c.y() - a.y()) - (b.y() - a.y()) * (c.x() - a.x())) > 0

segments = QgsVectorLayer("LineString", "Segments", "memory")
crs = segments.crs()
crs.createFromId(6423)
segments.setCrs(crs)
providerS = segments.dataProvider()
providerS.addAttributes([
	QgsField("fid", QVariant.Int),
	QgsField("length", QVariant.Double),
	QgsField("angle",  QVariant.Double)
])
segments.updateFields() 
symbolS = QgsLineSymbol.createSimple({'line_color': 'blue', 'line_width': '0.75'})
segments.renderer().setSymbol(symbolS)
segments.triggerRepaint()
QgsProject.instance().addMapLayer(segments)

triangles = QgsVectorLayer("LineString", "Triangles", "memory")
crs = triangles.crs()
crs.createFromId(6423)
triangles.setCrs(crs)
providerT = triangles.dataProvider()
providerT.addAttributes([
	QgsField("fid", QVariant.Int)
])
triangles.updateFields() 
symbolT = QgsLineSymbol.createSimple({'color': 'blue', 'line_style': 'dash'})
triangles.renderer().setSymbol(symbolT)
triangles.triggerRepaint()
QgsProject.instance().addMapLayer(triangles)

centroids = QgsVectorLayer("Point", "Centroids", "memory")
crs = centroids.crs()
crs.createFromId(6423)
centroids.setCrs(crs)
providerC = centroids.dataProvider()
providerC.addAttributes([
	QgsField("cfid", QVariant.Int)
])
centroids.updateFields() 
symbolC = QgsMarkerSymbol.createSimple({'color': 'red', 'border_color': 'red', 'size': '1.0'})
centroids.renderer().setSymbol(symbolC)
centroids.triggerRepaint()
QgsProject.instance().addMapLayer(centroids)

mbc = QgsVectorLayer("Point", "MBC", "memory")
crs = mbc.crs()
crs.createFromId(6423)
mbc.setCrs(crs)
providerM = mbc.dataProvider()
providerM.addAttributes([
	QgsField("fid", QVariant.Int)
])
mbc.updateFields() 
symbolM = QgsMarkerSymbol.createSimple({'color': 'green', 'border_color': 'green', 'size': '1.2'})
mbc.renderer().setSymbol(symbolM)
mbc.triggerRepaint()
QgsProject.instance().addMapLayer(mbc)

circles = QgsVectorLayer("Polygon", "Circles", "memory")
crs = circles.crs()
crs.createFromId(6423)
circles.setCrs(crs)
providerCi = circles.dataProvider()
providerCi.addAttributes([
	QgsField("cfid", QVariant.Int)
])
circles.updateFields() 
symbolCi = QgsFillSymbol.createSimple({'color_border': 'red', 'style': 'no', 'style_border': 'dash'})
circles.renderer().setSymbol(symbolCi)
circles.triggerRepaint()
QgsProject.instance().addMapLayer(circles)

for feature in layer.getFeatures():
	if feature.attribute("field_3") > 5.0:
		fid = feature.attribute("fid")
		geom = feature.geometry()
		vertices = []
		for vertex in geom.vertices():
			vertices.append(vertex)
		dists = []
		for i in range(0, len(vertices) - 1):
			for j in range(i + 1, len(vertices)):
				pi = vertices[i]
				pj = vertices[j]
				k = (pi, pj, pi.distance(pj))
				dists.append(k)
		d = max(dists, key=lambda dists: dists[2])
		mec = geom.minimalEnclosingCircle()
		c = QgsPoint(mec[1]) 
		m = QgsFeature()
		m.setGeometry(c)
		m.setAttributes([fid])
		providerM.addFeature(m)
		mbc.updateExtents()
		mbc.reload()
		a = d[0]
		b = d[1]
		if isLeft(a, b, c):
			p = (a, b)
		else:
			p = (b, a)
		dmax = d[2]
		segment = QgsLineString(iter(p))
		angle = segment.vertexAngle(QgsVertexId(0,0,1)) - math.pi / 2.0
		f = QgsFeature()
		f.setGeometry(segment)
		f.setAttributes([fid, dmax, angle])
		providerS.addFeature(f)
		segments.updateExtents()
		segments.reload()
		dx = math.sin(angle) * (epsilon / math.sqrt(3))
		dy = math.cos(angle) * (epsilon / math.sqrt(3)) 
		x = c.x() + dx
		y = c.y() + dy
		t1 = QgsPoint(x, y)
		angle2 = angle + 5.0 * math.pi / 6.0
		dx = math.sin(angle2) * epsilon
		dy = math.cos(angle2) * epsilon
		x = t1.x() + dx
		y = t1.y() + dy
		t2 = QgsPoint(x, y)
		angle2 = angle - 5.0 * math.pi / 6.0
		dx = math.sin(angle2) * epsilon
		dy = math.cos(angle2) * epsilon
		x = t1.x() + dx
		y = t1.y() + dy
		t3 = QgsPoint(x, y)
		p = (t1, t2, t3, t1)
		triangle = QgsLineString(iter(p))
		g = QgsFeature()
		g.setGeometry(triangle)
		g.setAttributes([fid])
		providerT.addFeature(g)
		triangles.updateExtents()
		triangles.reload()
		h1 = QgsFeature()
		c1 = QgsLineString(iter((t1, t2))).centroid() 
		h1.setGeometry(c1)
		h1.setAttributes([fid])
		providerC.addFeature(h1)
		h2 = QgsFeature()
		c2 = QgsLineString(iter((t2, t3))).centroid() 
		h2.setGeometry(c2)
		h2.setAttributes([fid])
		providerC.addFeature(h2)
		c3 = QgsLineString(iter((t3, t1))).centroid() 
		h3 = QgsFeature()
		h3.setGeometry(c3)
		h3.setAttributes([fid])
		providerC.addFeature(h3)
		centroids.updateExtents()
		centroids.reload()
		j1 = QgsFeature()
		j1.setGeometry(h1.geometry().buffer(epsilon / 2.0, 12))
		j1.setAttributes([fid])
		providerCi.addFeature(j1)
		j2 = QgsFeature()
		j2.setGeometry(h2.geometry().buffer(epsilon / 2.0, 12))
		j2.setAttributes([fid])
		providerCi.addFeature(j2)
		j3 = QgsFeature()
		j3.setGeometry(h3.geometry().buffer(epsilon / 2.0, 12))
		j3.setAttributes([fid])
		providerCi.addFeature(j3)
		circles.updateExtents()
		circles.reload()
