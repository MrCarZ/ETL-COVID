create_table = ("CREATE TABLE IF NOT EXISTS {table_name}( "
               "location_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
               "location VARCHAR(255) NOT NULL,"
               "lat FLOAT NOT NULL,"
               "lon FLOAT NOT NULL,"
               "confirmed INT NOT NULL,"
               "recovered INT NOT NULL,"
               "deaths INT NOT NULL,"
               "updated DATETIME NOT NULL)")

insert_to_table = ("INSERT INTO {table_name} "
                  "(location, lat, lon, confirmed, recovered, deaths, updated) "
                  "VALUES (%s, %s, %s, %s, %s, %s, %s)")

select_from_table = ("SELECT {data} FROM {table_name} ")

