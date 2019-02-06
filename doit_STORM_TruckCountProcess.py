"""
Query a database for the count of snow plow trucks active in Maryland and then update a hosted table in ArcGIS Online.
Queries the CHART database for active snow plow trucks. A sql query gets a count of records in the active trucks table.
A connection with ArcGIS Online is established and a hosted feature layer (table) is accessed. The table has no spatial
data, just a truck count value. This truck count value is updated using the value pulled from the CHART database. The
hosted table is intended to feed a widget in the STORM web application map and feed the truck count stat shown on the
main map page.
Author: CJuice
Created: 20190205
Revisions:

"""


def main():

    # IMPORTS
    from datetime import datetime
    start_time = datetime.now()

    from arcgis.gis import GIS
    import configparser
    import pyodbc

    # GET CREDENTIAL TYPE ITEMS
    parser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    storm_credentials_config = r"doit_STORM_credentials.cfg"
    parser.read(filenames=[storm_credentials_config])

    # ____________________
    # Database Portion of the Process
    # ____________________

    database_name = parser["DATABASE"]["DB_NAME"]
    database_user = parser["DATABASE"]["USER_NAME"]
    database_password = parser["DATABASE"]["PASSWORD"]
    database_connection_string = f"DSN={database_name};UID={database_user};PWD={database_password}"
    sql_query_for_count = parser["DATABASE"]["TRUCK_COUNT_SQL"]

    # Need a connection to query table and get truck count
    with pyodbc.connect(database_connection_string) as connection:
        curs = connection.cursor()
        try:
            curs.execute(sql_query_for_count)
        except pyodbc.DataError as pde:
            print(pde, sql_query_for_count)
            exit()

        results = curs.fetchall()   # Returns list with a single row tuple with a single value
    truck_count = results[0][0]

    # ____________________
    # GIS Portion of the Process
    # ____________________

    agol_password = parser["AGOL"]["PASSWORD"]
    agol_root_url = parser["AGOL"]["ROOT_URL"]
    agol_username = parser["AGOL"]["USER_NAME"]
    agol_layer_id = parser["AGOL"]["LAYER_ID"]

    # Need an agol connection session thingy
    gis = GIS(url=agol_root_url, username=agol_username, password=agol_password)

    # Need to the hosted feature layer based on id. Hosted table style previously used but WebApp couldn't consume it.
    truck_feature_layer_agol = gis.content.get(agol_layer_id)
    truck_layers_list = truck_feature_layer_agol.layers
    truck_feature_layer = truck_layers_list[0]

    # Need to get feature set for layer, isolate record, and change attribute value. Used ESRI dev docs for guidance
    truck_features_feature_set = truck_feature_layer.query()
    truck_features_list = truck_features_feature_set.features
    if len(truck_features_list) != 1:
        print(f"WARNING: More than one feature in the truck count feature layer. Expecting length == 1\n{truck_features_list}")
        exit()
    first_record = truck_features_list[0]
    first_record.attributes["TRUCK_COUNT"] = truck_count

    # Need to change the existing count value to the newest value pulled from the database
    update_result = truck_feature_layer.edit_features(updates=[truck_features_list[0]])

    # Print out some info for Visual Cron job documentation
    print(f"Truck Count Updated in AGOL: {update_result}")
    print(f"Truck Count Value: {truck_count}")
    print(f"Process run time: {datetime.now() - start_time}")

    # NOTE: Accidentally created a second record and had to delete it
    # truck_count_table.edit_features(deletes='2')  # the number 2 was the object id of the 'feature' in the table


if __name__ == "__main__":
    main()