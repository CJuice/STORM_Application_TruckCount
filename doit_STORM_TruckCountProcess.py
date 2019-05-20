"""
Query a database for the count of trucks active in Maryland and then update a hosted feature service in ArcGIS Online.
Use a SQL query on the CHART database to get a count of records in the active trucks table. A connection with ArcGIS
Online is established and a hosted feature layer is accessed. The feature layer is a single point geometry centered
on the Maryland State House and has no meaning. A hosted table was not consumable by the WebApp widget at the time
of design so we had to make a hosted feature layer with a single geometry record. The truck count value is attached
to the point. The truck count value is updated using the value pulled from the CHART database. The hosted feature
layer feeds a widget in the STORM web application map and the truck count is shown on the main map page.
Author: CJuice
Created: 20190205
Revisions:
20190506 - Redesigned to use a hosted feature layer instead of a hosted feature table as the table is not
consumable by the WebApp widget used to display the truck count. Also added timing print outs to see what portions were
taking the longest with the intention of trying to speed up the process. The imports takes nearly half of the total
run time. Researched options but didn't implement socket server setup we read about. Instead, staggered Visual Cron
task to start 5 seconds before CHART AVL Ingestion job. That job takes about 2.5 seconds while the imports take about
8 to 9 seconds. We kick off this process and while the imports are running the ingestion job runs and finishes. We
shaved off a few seconds this way.
20190520 - Added exception handling for periodic Runtime Errors during agol interactions

"""


def main():

    # IMPORTS
    from datetime import datetime
    start_time = datetime.now()

    from arcgis.gis import GIS
    import configparser
    import pyodbc
    print(f"\nImports complete. Time passed since start = {datetime.now() - start_time}")

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

    print(f"Database variable parsing complete. Time passed since start = {datetime.now() - start_time}")

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

    print(f"Database part complete. Time passed since start = {datetime.now() - start_time}")

    # ____________________
    # GIS Portion of the Process
    # ____________________

    agol_password = parser["AGOL"]["PASSWORD"]
    agol_root_url = parser["AGOL"]["ROOT_URL"]
    agol_username = parser["AGOL"]["USER_NAME"]
    agol_layer_id = parser["AGOL"]["LAYER_ID"]

    print(f"AGOL variable parsing complete. Time passed since start = {datetime.now() - start_time}")

    # Need an agol connection session thingy
    gis = GIS(url=agol_root_url, username=agol_username, password=agol_password)

    print(f"AGOL connection established. Time passed since start = {datetime.now() - start_time}")

    # Need to the hosted feature layer based on id. Hosted table style previously used but WebApp couldn't consume it.
    truck_feature_layer_agol = gis.content.get(agol_layer_id)
    truck_layers_list = truck_feature_layer_agol.layers
    truck_feature_layer = truck_layers_list[0]

    # Need to get feature set for layer, isolate record, and change attribute value. Used ESRI dev docs for guidance
    try:
        truck_features_feature_set = truck_feature_layer.query()
        truck_features_list = truck_features_feature_set.features
        if len(truck_features_list) != 1:
            print(f"WARNING: More than one feature in the truck count feature layer. Expected 1\n{truck_features_list}")
            exit()
        first_record = truck_features_list[0]
        first_record.attributes["TRUCK_COUNT"] = truck_count

        # Need to change the existing count value to the newest value pulled from the database
        update_result = truck_feature_layer.edit_features(updates=[truck_features_list[0]])
    except RuntimeError as rte:
        print(f"Runtime Error raised: {rte}")
        exit()

    # Print out some info for Visual Cron job documentation
    print(f"Truck Count Updated in AGOL: {update_result}")
    print(f"Truck Count Value: {truck_count}")
    print(f"Process run time: {datetime.now() - start_time}")

    # NOTE: Accidentally created a second record and had to delete it. Could need to use again in future so keeping it.
    # truck_feature_layer.edit_features(deletes='2')  # the #2 was the obj id of the extra 'feature'


if __name__ == "__main__":
    main()