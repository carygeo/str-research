# Import required modules
import csv
import sqlite3
 
redfin = '/Users/cary/Documents/dev/str-research/redfin/'
# https://www.redfin.com/news/data-center/
# https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz
# https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz
# https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz

# Connecting to the geeks database
connection = sqlite3.connect('./redfin/redfin.db')
 
# Creating a cursor object to execute
# SQL queries on a database table
cursor = connection.cursor()
 
# Table Definition
create_zip_table = '''CREATE TABLE redfin_zipcode(
                period_begin TEXT NOT NULL,
                period_end TEXT NOT NULL,
                period_duration TEXT NOT NULL,
                region_type TEXT NOT NULL,
                region_type_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                is_seasonally_adjusted TEXT NOT NULL,
                region TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                state_code TEXT NOT NULL,
                property_type TEXT NOT NULL,
                property_type_id TEXT NOT NULL,
                median_sale_price INTEGER NOT NULL,
                median_sale_price_mom TEXT NOT NULL,
                median_sale_price_yoy TEXT NOT NULL,
                median_list_price INTEGER NOT NULL,
                median_list_price_mom TEXT NOT NULL,
                median_list_price_yoy TEXT NOT NULL,
                median_ppsf TEXT NOT NULL,
                median_ppsf_mom TEXT NOT NULL,
                median_ppsf_yoy TEXT NOT NULL,
                median_list_ppsf TEXT NOT NULL,
                median_list_ppsf_mom TEXT NOT NULL,
                median_list_ppsf_yoy TEXT NOT NULL,
                homes_sold TEXT NOT NULL,
                homes_sold_mom TEXT NOT NULL,
                homes_sold_yoy TEXT NOT NULL,
                pending_sales TEXT NOT NULL,
                pending_sales_mom TEXT NOT NULL,
                pending_sales_yoy TEXT NOT NULL,
                new_listings TEXT NOT NULL,
                new_listings_mom TEXT NOT NULL,
                new_listings_yoy TEXT NOT NULL,
                inventory TEXT NOT NULL,
                inventory_mom TEXT NOT NULL,
                inventory_yoy TEXT NOT NULL,
                months_of_supply TEXT NOT NULL,
                months_of_supply_mom TEXT NOT NULL,
                months_of_supply_yoy TEXT NOT NULL,
                median_dom TEXT NOT NULL,
                median_dom_mom TEXT NOT NULL,
                median_dom_yoy TEXT NOT NULL,
                avg_sale_to_list TEXT NOT NULL,
                avg_sale_to_list_mom TEXT NOT NULL,
                avg_sale_to_list_yoy TEXT NOT NULL,
                sold_above_list TEXT NOT NULL,
                sold_above_list_mom TEXT NOT NULL,
                sold_above_list_yoy TEXT NOT NULL,
                price_drops TEXT NOT NULL,
                price_drops_mom TEXT NOT NULL,
                price_drops_yoy TEXT NOT NULL,
                off_market_in_two_weeks TEXT NOT NULL,
                off_market_in_two_weeks_mom TEXT NOT NULL,
                off_market_in_two_weeks_yoy TEXT NOT NULL,
                parent_metro_region TEXT NOT NULL,
                zipcode TEXT PRIMARY KEY,
                last_updated TEXT NOT NULL);
                '''
 
# Table Definition
create_neighborhood_table = '''CREATE TABLE redfin_neighborhood(
                period_begin TEXT NOT NULL,
                period_end TEXT NOT NULL,
                period_duration TEXT NOT NULL,
                region_type TEXT NOT NULL,
                region_type_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                is_seasonally_adjusted TEXT NOT NULL,
                region TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                state_code TEXT NOT NULL,
                property_type TEXT NOT NULL,
                property_type_id TEXT NOT NULL,
                median_sale_price INTEGER NOT NULL,
                median_sale_price_mom TEXT NOT NULL,
                median_sale_price_yoy TEXT NOT NULL,
                median_list_price INTEGER NOT NULL,
                median_list_price_mom TEXT NOT NULL,
                median_list_price_yoy TEXT NOT NULL,
                median_ppsf TEXT NOT NULL,
                median_ppsf_mom TEXT NOT NULL,
                median_ppsf_yoy TEXT NOT NULL,
                median_list_ppsf TEXT NOT NULL,
                median_list_ppsf_mom TEXT NOT NULL,
                median_list_ppsf_yoy TEXT NOT NULL,
                homes_sold TEXT NOT NULL,
                homes_sold_mom TEXT NOT NULL,
                homes_sold_yoy TEXT NOT NULL,
                pending_sales TEXT NOT NULL,
                pending_sales_mom TEXT NOT NULL,
                pending_sales_yoy TEXT NOT NULL,
                new_listings TEXT NOT NULL,
                new_listings_mom TEXT NOT NULL,
                new_listings_yoy TEXT NOT NULL,
                inventory TEXT NOT NULL,
                inventory_mom TEXT NOT NULL,
                inventory_yoy TEXT NOT NULL,
                months_of_supply TEXT NOT NULL,
                months_of_supply_mom TEXT NOT NULL,
                months_of_supply_yoy TEXT NOT NULL,
                median_dom TEXT NOT NULL,
                median_dom_mom TEXT NOT NULL,
                median_dom_yoy TEXT NOT NULL,
                avg_sale_to_list TEXT NOT NULL,
                avg_sale_to_list_mom TEXT NOT NULL,
                avg_sale_to_list_yoy TEXT NOT NULL,
                sold_above_list TEXT NOT NULL,
                sold_above_list_mom TEXT NOT NULL,
                sold_above_list_yoy TEXT NOT NULL,
                price_drops TEXT NOT NULL,
                price_drops_mom TEXT NOT NULL,
                price_drops_yoy TEXT NOT NULL,
                off_market_in_two_weeks TEXT NOT NULL,
                off_market_in_two_weeks_mom TEXT NOT NULL,
                off_market_in_two_weeks_yoy TEXT NOT NULL,
                parent_metro_region TEXT NOT NULL,
                zipcode TEXT NOT NULL,
                last_updated TEXT NOT NULL);
                '''

# Table Definition
create_city_table = '''CREATE TABLE redfin_city(
                period_begin TEXT NOT NULL,
                period_end TEXT NOT NULL,
                period_duration TEXT NOT NULL,
                region_type TEXT NOT NULL,
                region_type_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                is_seasonally_adjusted TEXT NOT NULL,
                region TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                state_code TEXT NOT NULL,
                property_type TEXT NOT NULL,
                property_type_id TEXT NOT NULL,
                median_sale_price INTEGER NOT NULL,
                median_sale_price_mom TEXT NOT NULL,
                median_sale_price_yoy TEXT NOT NULL,
                median_list_price INTEGER NOT NULL,
                median_list_price_mom TEXT NOT NULL,
                median_list_price_yoy TEXT NOT NULL,
                median_ppsf TEXT NOT NULL,
                median_ppsf_mom TEXT NOT NULL,
                median_ppsf_yoy TEXT NOT NULL,
                median_list_ppsf TEXT NOT NULL,
                median_list_ppsf_mom TEXT NOT NULL,
                median_list_ppsf_yoy TEXT NOT NULL,
                homes_sold TEXT NOT NULL,
                homes_sold_mom TEXT NOT NULL,
                homes_sold_yoy TEXT NOT NULL,
                pending_sales TEXT NOT NULL,
                pending_sales_mom TEXT NOT NULL,
                pending_sales_yoy TEXT NOT NULL,
                new_listings TEXT NOT NULL,
                new_listings_mom TEXT NOT NULL,
                new_listings_yoy TEXT NOT NULL,
                inventory TEXT NOT NULL,
                inventory_mom TEXT NOT NULL,
                inventory_yoy TEXT NOT NULL,
                months_of_supply TEXT NOT NULL,
                months_of_supply_mom TEXT NOT NULL,
                months_of_supply_yoy TEXT NOT NULL,
                median_dom TEXT NOT NULL,
                median_dom_mom TEXT NOT NULL,
                median_dom_yoy TEXT NOT NULL,
                avg_sale_to_list TEXT NOT NULL,
                avg_sale_to_list_mom TEXT NOT NULL,
                avg_sale_to_list_yoy TEXT NOT NULL,
                sold_above_list TEXT NOT NULL,
                sold_above_list_mom TEXT NOT NULL,
                sold_above_list_yoy TEXT NOT NULL,
                price_drops TEXT NOT NULL,
                price_drops_mom TEXT NOT NULL,
                price_drops_yoy TEXT NOT NULL,
                off_market_in_two_weeks TEXT NOT NULL,
                off_market_in_two_weeks_mom TEXT NOT NULL,
                off_market_in_two_weeks_yoy TEXT NOT NULL,
                parent_metro_region TEXT NOT NULL,
                zipcode TEXT NOT NULL,
                last_updated TEXT NOT NULL);
                '''

# Creating the table into our``
# database
cursor.execute(create_zip_table)
cursor.execute(create_neighborhood_table)
cursor.execute(create_city_table)

## ZIPCODES
# Opening the file
file = open(redfin + 'zip_code_market_tracker.tsv000')

# Reading the contents of the file
contents = csv.reader(file, delimiter="\t")
next(contents)  # Skip header row.

# SQL query to insert data into the
# person table
insert_zip_records = "INSERT OR IGNORE INTO redfin_zipcode (period_begin, period_end, period_duration, region_type, region_type_id, table_id, is_seasonally_adjusted, region, city, state, state_code, property_type, property_type_id, median_sale_price, median_sale_price_mom, median_sale_price_yoy, median_list_price, median_list_price_mom, median_list_price_yoy, median_ppsf, median_ppsf_mom, median_ppsf_yoy, median_list_ppsf, median_list_ppsf_mom, median_list_ppsf_yoy, homes_sold, homes_sold_mom, homes_sold_yoy, pending_sales, pending_sales_mom, pending_sales_yoy, new_listings, new_listings_mom, new_listings_yoy, inventory, inventory_mom, inventory_yoy, months_of_supply, months_of_supply_mom, months_of_supply_yoy, median_dom, median_dom_mom, median_dom_yoy, avg_sale_to_list, avg_sale_to_list_mom, avg_sale_to_list_yoy, sold_above_list, sold_above_list_mom, sold_above_list_yoy, price_drops, price_drops_mom, price_drops_yoy, off_market_in_two_weeks, off_market_in_two_weeks_mom, off_market_in_two_weeks_yoy, parent_metro_region, zipcode, last_updated) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

# Importing the contents of the file
# into our person table
cursor.executemany(insert_zip_records, contents)

## NEIGHBORHOODS
# Opening the file
file = open(redfin + 'neighborhood_market_tracker.tsv000')

# Reading the contents of the file
contents = csv.reader(file, delimiter="\t")
next(contents)  # Skip header row.

insert_neigh_records = "INSERT OR IGNORE INTO redfin_neighborhood (period_begin, period_end, period_duration, region_type, region_type_id, table_id, is_seasonally_adjusted, region, city, state, state_code, property_type, property_type_id, median_sale_price, median_sale_price_mom, median_sale_price_yoy, median_list_price, median_list_price_mom, median_list_price_yoy, median_ppsf, median_ppsf_mom, median_ppsf_yoy, median_list_ppsf, median_list_ppsf_mom, median_list_ppsf_yoy, homes_sold, homes_sold_mom, homes_sold_yoy, pending_sales, pending_sales_mom, pending_sales_yoy, new_listings, new_listings_mom, new_listings_yoy, inventory, inventory_mom, inventory_yoy, months_of_supply, months_of_supply_mom, months_of_supply_yoy, median_dom, median_dom_mom, median_dom_yoy, avg_sale_to_list, avg_sale_to_list_mom, avg_sale_to_list_yoy, sold_above_list, sold_above_list_mom, sold_above_list_yoy, price_drops, price_drops_mom, price_drops_yoy, off_market_in_two_weeks, off_market_in_two_weeks_mom, off_market_in_two_weeks_yoy, parent_metro_region, zipcode, last_updated) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

# Importing the contents of the file
# into our person table
cursor.executemany(insert_neigh_records, contents)

## CITIES
# Opening the file
file = open(redfin + 'city_market_tracker.tsv000')

# Reading the contents of the file
contents = csv.reader(file, delimiter="\t")
next(contents)  # Skip header row.

insert_city_records = "INSERT OR IGNORE INTO redfin_city (period_begin, period_end, period_duration, region_type, region_type_id, table_id, is_seasonally_adjusted, region, city, state, state_code, property_type, property_type_id, median_sale_price, median_sale_price_mom, median_sale_price_yoy, median_list_price, median_list_price_mom, median_list_price_yoy, median_ppsf, median_ppsf_mom, median_ppsf_yoy, median_list_ppsf, median_list_ppsf_mom, median_list_ppsf_yoy, homes_sold, homes_sold_mom, homes_sold_yoy, pending_sales, pending_sales_mom, pending_sales_yoy, new_listings, new_listings_mom, new_listings_yoy, inventory, inventory_mom, inventory_yoy, months_of_supply, months_of_supply_mom, months_of_supply_yoy, median_dom, median_dom_mom, median_dom_yoy, avg_sale_to_list, avg_sale_to_list_mom, avg_sale_to_list_yoy, sold_above_list, sold_above_list_mom, sold_above_list_yoy, price_drops, price_drops_mom, price_drops_yoy, off_market_in_two_weeks, off_market_in_two_weeks_mom, off_market_in_two_weeks_yoy, parent_metro_region, zipcode, last_updated) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

# Importing the contents of the file
# into our person table
cursor.executemany(insert_city_records, contents)

## INDEXES
index_zip = "CREATE UNIQUE INDEX idx_zipcodes ON redfin_zipcode (zipcode)"
cursor.execute(index_zip)

index_neigh = "CREATE UNIQUE INDEX idx_neighborhoods ON redfin_neighborhood (region)"
cursor.execute(index_neigh)

index_city = "CREATE UNIQUE INDEX idx_cities ON redfin_city (region)"
cursor.execute(index_city)

# Committing the changes
connection.commit()
 
# closing the database connection
connection.close()