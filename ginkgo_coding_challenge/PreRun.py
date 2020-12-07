#!/usr/bin/python
import mariadb
from settings import db_server, db_user, db_password, db_database

conn = mariadb.connect(
    user=db_user,
    password=db_password,
    host=db_server,
    database=db_database)
cur = conn.cursor()

# retrieving information
cur.execute("drop table if exists sample_measurements")
cur.execute("drop table if exists samples")

cur.execute("create table samples(id int primary key auto_increment,"
            " parent_id int, experiment_id int, "
            "ts timestamp not null default CURRENT_TIMESTAMP, "
            "foreign key(parent_id) references samples(id) on delete cascade)")

cur.execute("create table sample_measurements(sample_id int, "
            "measurement_type varchar(10), "
            "value decimal(16,6), "
            "foreign key(sample_id) references samples(id) on delete cascade)")

# insert information
try:
    cur.execute("""insert into samples (parent_id, experiment_id) values
                (null, 1), (1,1), (1,1), (1,1),
                (null, 1), (2,1), (5,1), (7,1),
                (2, 1), (9,1), (10,1), (9,1),
                (null, 2), (13,2), (13,2), (13,2),
                (10, 1), (17,1), (17,1), (11,1)""")

    cur.execute("""insert into sample_measurements values 
                                    (2, 'vol', 500), (3, 'vol', 400), 
                                    (6, 'vol', 51), (9, 'vol', 50), 
                                    (10, 'vol', 10.5), (12, 'vol', 40.3), 
                                    (17, 'vol', 10.2), (8, 'vol', 40.8), 
                                    (19, 'vol', 10), (20, 'vol', 40.7), 
                                    (2, 'ph', 5.0), (3, 'ph', 7.0), 
                                    (6, 'ph', 5.1), (9, 'ph', 7.2), 
                                    (10, 'ph', 5.2), (12, 'ph', 7.4), 
                                    (17, 'ph', 5.0), (8, 'ph', 7.4), 
                                    (19, 'ph', 5.25), (20, 'ph', 7.34) , (20, 'size', 7.34)""")

except mariadb.Error as e:
    print(f"Error: {e}")

conn.commit()

conn.close()
