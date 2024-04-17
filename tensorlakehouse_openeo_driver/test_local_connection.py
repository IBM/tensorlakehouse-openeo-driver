# from openeo.local import LocalConnection
# from openeo_geodn_driver.local.connection import GeodnLocalConnection

# def test_local_connection():
#     path = "/Users/ltizzei/Projects/Orgs/GeoDN-Discovery/openeo-geodn-driver/examples/test/test_openeo_Globalweather-ERA5-.nc"
#     directory = "/Users/ltizzei/Projects/Orgs/GeoDN-Discovery/openeo-geodn-driver/examples/test/"
#     local_conn = GeodnLocalConnection(directory)

#     list_collections = local_conn.list_collections()
#     coll = local_conn.describe_collection(path)
#     west = coll["cube:dimensions"]["x"]["extent"][0]
#     east = coll["cube:dimensions"]["x"]["extent"][1]
#     south = coll["cube:dimensions"]["y"]["extent"][0]
#     north = coll["cube:dimensions"]["y"]["extent"][1]
#     coords = list()
#     for lat in [south, north]:
#         for lon in [west, east]:
#             coords.append([lon, lat])
#     datacube = local_conn.load_collection(path)
#     geometries = { "type": "Feature", "properties": {}, "geometry": { "type": "Polygon", "coordinates": [ coords ] } }
#     datacube = datacube.aggregate_spatial(reducer="mean", geometries=geometries)
#     data = datacube.execute()
#     print(data)