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
    feature_type = "Feature Service"
    owner = parser["AGOL"]["TABLE_OWNER"]
    title = "STORM_Truck_Count_Table"

    # Need an agol connection session thingy
    gis = GIS(url=agol_root_url, username=agol_username, password=agol_password)

    # Need to find and get a reference to the hosted table
    query_for_truck_count_table_layer = f"title: {title} AND owner: {owner} AND type: {feature_type}"
    truck_query_layer_results = gis.content.search(query=query_for_truck_count_table_layer)

    # Need to make sure there is only one item. If some other item with same name gets created then bail.
    if len(truck_query_layer_results) > 1:
        print(f"Woah Nelly, more than one table was found in the search using... '{query_for_truck_count_table_layer}'")
        exit()

    # Need to isolate tables from spatial layers and extract the value of interest. Used ESRI dev docs for guidance here
    truck_tables = truck_query_layer_results[0].tables
    truck_count_feature_set = truck_tables[0].query()
    truck_count_table = truck_tables[0]
    truck_features_list = truck_count_feature_set.features
    truck_features_list[0].attributes["TRUCK_COUNT"] = truck_count

    # Need to change the existing count value to the newest value pulled from the database
    update_result = truck_count_table.edit_features(updates=[truck_features_list[0]])

    # Print out some info for Visual Cron job documentation
    print(f"Truck Count Updated in AGOL: {update_result}")
    print(f"Truck Count Value: {truck_count}")
    print(f"Process run time: {datetime.now() - start_time}")

    # NOTE: Accidentally created a second record and had to delete it
    # truck_count_table.edit_features(deletes='2')  # the number 2 was the object id of the 'feature' in the table


if __name__ == "__main__":
    main()