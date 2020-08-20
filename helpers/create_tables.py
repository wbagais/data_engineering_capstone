import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES
immigration_table_drop = "DROP TABLE IF EXISTS immigration cascade;"
state_drop = "DROP TABLE IF EXISTS state cascade;"
country_drop = "DROP TABLE IF EXISTS country cascade;"
data_table_drop = "DROP TABLE IF EXISTS date cascade;"
temperature_table_drop = "DROP TABLE IF EXISTS temperature cascade;"

# CREATE TABLES
immigration_table_create= ("""CREATE TABLE immigration(
                                cicid               INTEGER PRIMARY KEY,
                                i94yr               FLOAT,
                                i94mon              FLOAT,
                                i94cit              INTEGER,
                                i94res              INTEGER,
                                arrdate             datetime,
                                i94addr             VARCHAR,
                                depdate             datetime,
                                dtadfile            INTEGER,
                                visapost            VARCHAR,
                                occup               VARCHAR,
                                biryear             FLOAT,
                                gender              VARCHAR,
                                airline             VARCHAR,
                                fltno               VARCHAR,
                                visatype            VARCHAR);
""")

state_table_create = ("""CREATE TABLE state(
                                code           VARCHAR PRIMARY KEY,
                                state          VARCHAR);
""")

country_table_create = ("""CREATE TABLE country(
                                code           INTEGER PRIMARY KEY,
                                country        VARCHAR);
""")

temperature_table_create = ("""CREATE TABLE IF NOT EXISTS temperature(
                                month                            int PRIMARY KEY,
                                AverageTemperature               FLOAT NOT NULL,
                                Country                          VARCHAR,
                                Latitude                         VARCHAR,
                                Longitude                        VARCHAR);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS date(
                        date                 datetime PRIMARY KEY, 
                        day                  int, 
                        week                 int, 
                        month                int, 
                        year                 int, 
                        weekday              int);
""")