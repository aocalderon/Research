ogr2ogr -f CSV -overwrite -sql "SELECT CAST(field_2 AS REAL) AS oid, ST_X(geom) AS x, ST_Y(geom) AS y, CAST(field_3 AS REAL) AS t  FROM 'Points'" points points.gpkg
sed 's/,/\t/g' points.csv > points.tsv
