from PyQt5.QtWidgets import QMessageBox
from qgis.core import QgsProject, QgsFillSymbol, QgsMarkerSymbol, QgsPalLayerSettings, QgsVectorLayerSimpleLabeling
from qgis.utils import iface
from PyQt5 import uic
import subprocess
import os

DialogBase, DialogType = uic.loadUiType(os.path.join(os.path.dirname(__file__), 'loader.ui'))

class LoaderDialog(DialogType, DialogBase):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)
        with open(os.path.join(os.path.dirname(__file__), 'history.txt')) as f:
            history = f.readlines()
        history = [x.strip() for x in history]
        self.crsText.setPlainText(history.pop())
        self.hostCombobox.setCurrentText(history.pop())
        self.layersText.setPlainText("\n".join(history))
        self.loadButton.clicked.connect(self.getLayers)

    def getLayers(self):
        text = self.layersText.toPlainText()
        names = text.split("\n")
        crs = self.crsText.toPlainText()
        host = self.hostCombobox.currentText()
        if host == "hn":
            for name in names:
                subprocess.run(["scp", "acald013@hn:/tmp/edges{}.wkt".format(name), "/home/and/tmp/edges/edges{}.wkt".format(name)])
        if host == "local":
            for name in names:
                subprocess.run(["cp", "/tmp/edges{}.wkt".format(name), "/home/and/tmp/edges/edges{}.wkt".format(name)])
        instance = QgsProject.instance()
        for name in names:
            layers = instance.mapLayersByName(name)
            for layer in layers:
                instance.removeMapLayer(layer)
        iface.mapCanvas().refresh()
        for name in names:
            uri = "file:///home/and/tmp/edges/edges{}.wkt?delimiter={}&useHeader=no&crs=epsg:{}&wktField={}".format(name, "\\t", crs, "field_1")
            layer = iface.addVectorLayer(uri, name, "delimitedtext")
            if name == "Cells" or name == "GGrids":
                symbol = QgsFillSymbol.createSimple({'color_border': 'blue', 'style': 'no', 'style_border': 'dash'})
                layer.renderer().setSymbol(symbol)
                layer.triggerRepaint()
            if name == "C_prime" or name == "Circles":
                symbol = QgsFillSymbol.createSimple({'color_border': 'black', 'style': 'no', 'style_border': 'dash'})
                layer.renderer().setSymbol(symbol)
                layer.triggerRepaint()
            if name == "C" or name == "Centers":
                props = layer.renderer().symbol().symbolLayer(0).properties()
                props['color'] = 'black'
                props['name'] = 'cross'
                props['size'] = '2'
                layer.renderer().setSymbol(QgsMarkerSymbol.createSimple(props))
                
                layer.triggerRepaint()
            if name == "LGrids":
                symbol = QgsFillSymbol.createSimple({'color_border': 'red', 'style': 'no', 'style_border': 'dash'})
                layer.renderer().setSymbol(symbol)
                layer.triggerRepaint()
            if name == "Faces":
                symbol = QgsFillSymbol.createSimple({'color': 'green'})
                layer.renderer().setSymbol(symbol)
                pal_layer = QgsPalLayerSettings()
                pal_layer.fieldName = 'field_2'
                pal_layer.enabled = True
                pal_layer.placement = QgsPalLayerSettings.Free
                labels = QgsVectorLayerSimpleLabeling(pal_layer)
                layer.setLabeling(labels)
                layer.setLabelsEnabled(True)
                layer.triggerRepaint()
        with open(os.path.join(os.path.dirname(__file__), 'history.txt'), 'w') as f:
            f.write("{}\n{}\n{}".format(text, host, crs))
        
