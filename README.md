--- experimental data in DB ----
Given the SQL script below (runs in MYSQL), 
please create an ETL process that will maintain and populate a new table “experiment_measurements” with following columns
experiment_id - from the samples 
top_parent_id - samples.id from samples where parent_id is null 
sample_id - sample id which has measurements attached 
mesurement_vol, measurement_ph - columns created dynamically based on values in ‘measurement_type’

Expected to see these entries in the result table, as an example
|experiment_id|top_parent_id|sample_id|mesurement_vol|measurement_ph|
|1            |1            | 17      | 10.2         | 5.0          |
|1            |5            | 8       | 40.          | 7.4          | 





Please consider:
Number of samples is growing daily and already in millions 
Number of levels in the tree could be limited by 1000
New types of measurements can be added/removed occasionally 

----------------------------------------
drop table if exists sample_measurements;
drop table if exists samples;

create table samples(id int primary key auto_increment, parent_id int, 
experiment_id int, ts timestamp not null default CURRENT_TIMESTAMP, 
foreign key(parent_id) references samples(id) on delete cascade);

create table sample_measurements(sample_id int, measurement_type varchar(10), value decimal(16,6), 
foreign key(sample_id) references samples(id) on delete cascade);

insert into samples  (parent_id, experiment_id) values
(null, 1), (1,1), (1,1), (1,1),
(null, 1), (2,1), (5,1), (7,1),
(2, 1), (9,1), (10,1), (9,1),
(null, 2), (13,2), (13,2), (13,2),
(10, 1), (17,1), (17,1), (11,1);

insert into sample_measurements values
(2, 'vol', 500), (3, 'vol', 400),
(6, 'vol', 51), (9, 'vol', 50),
(10, 'vol', 10.5), (12, 'vol', 40.3),
(17, 'vol', 10.2), (8, 'vol', 40.8),
(19, 'vol', 10), (20, 'vol', 40.7),
(2, 'ph', 5.0), (3, 'ph', 7.0),
(6, 'ph', 5.1), (9, 'ph', 7.2),
(10, 'ph', 5.2), (12, 'ph', 7.4),
(17, 'ph', 5.0), (8, 'ph', 7.4),
(19, 'ph', 5.25), (20, 'ph', 7.34);



+-----------+------------------+------------+
| sample_id | measurement_type | value      |
+-----------+------------------+------------+
|         2 | vol              | 500.000000 |
|         3 | vol              | 400.000000 |
|         6 | vol              |  51.000000 |
|         9 | vol              |  50.000000 |
|        10 | vol              |  10.500000 |
|        12 | vol              |  40.300000 |
|        17 | vol              |  10.200000 |
|         8 | vol              |  40.800000 |
|        19 | vol              |  10.000000 |
|        20 | vol              |  40.700000 |
|         2 | ph               |   5.000000 |
|         3 | ph               |   7.000000 |
|         6 | ph               |   5.100000 |
|         9 | ph               |   7.200000 |
|        10 | ph               |   5.200000 |
|        12 | ph               |   7.400000 |
|        17 | ph               |   5.000000 |
|         8 | ph               |   7.400000 |
|        19 | ph               |   5.250000 |
|        20 | ph               |   7.340000 |
+-----------+------------------+------------+


+----+-----------+---------------+---------------------+
| id | parent_id | experiment_id | ts                  |
+----+-----------+---------------+---------------------+
|  1 |      NULL |             1 | 2020-11-28 19:55:41 |
|  2 |         1 |             1 | 2020-11-28 19:55:41 |
|  3 |         1 |             1 | 2020-11-28 19:55:41 |
|  4 |         1 |             1 | 2020-11-28 19:55:41 |
|  5 |      NULL |             1 | 2020-11-28 19:55:41 |
|  6 |         2 |             1 | 2020-11-28 19:55:41 |
|  7 |         5 |             1 | 2020-11-28 19:55:41 |
|  8 |         7 |             1 | 2020-11-28 19:55:41 |
|  9 |         2 |             1 | 2020-11-28 19:55:41 |
| 10 |         9 |             1 | 2020-11-28 19:55:41 |
| 11 |        10 |             1 | 2020-11-28 19:55:41 |
| 12 |         9 |             1 | 2020-11-28 19:55:41 |
| 13 |      NULL |             2 | 2020-11-28 19:55:41 |
| 14 |        13 |             2 | 2020-11-28 19:55:41 |
| 15 |        13 |             2 | 2020-11-28 19:55:41 |
| 16 |        13 |             2 | 2020-11-28 19:55:41 |
| 17 |        10 |             1 | 2020-11-28 19:55:41 |
| 18 |        17 |             1 | 2020-11-28 19:55:41 |
| 19 |        17 |             1 | 2020-11-28 19:55:41 |
| 20 |        11 |             1 | 2020-11-28 19:55:41 |
+----+-----------+---------------+---------------------+
20 rows in set (0.037 sec)

![alt text](https://github.com/sunakshisaxena23/Ginkgo_coding_challenge/blob/main/ginkgo.jpg?raw=true)

modeling query:


Q 3:

select a.name , 
case when d.student_id is null then 0 else 1 end as subscription_flag
from classes a
left joins lecturer_class_link b on a.class_id = b.class_id
left joins lecturers c on b.lecturer_id = c.lecturer_id
left joins (select * from student_class_link where student_id = current_user) d on a.class_id = d.class_id
where a.category = 'mechanical' and a.current_status = 'Y'
order by rating desc


Q 5:

select a.name 
from classes a
left joins (select class_id, count(*) as currently_enrolled_count from student_class_link group by class_id) b on a.class_id = b.class_id
where a.capacity/2 > currently_enrolled_count
