-- make buildings table
drop table if exists buildings;
create table buildings ( the_geom GEOMETRY, height double );
-- Insert 4 buildings
INSERT INTO buildings VALUES (ST_GeomFromText('MULTIPOLYGON (((0 20 0,20 20 0,20 60 0,0 60 0,0 20 0)))'),10);
INSERT INTO buildings VALUES (ST_GeomFromText('MULTIPOLYGON (((20 0 0,100 0 0, 100 20 0,20 20 0, 20 0 0)))'),15);
INSERT INTO buildings  VALUES (ST_GeomFromText('MULTIPOLYGON (((80 30 0,80 90 0,-10 90 0,-10 70 0,60 70 0,60 30 0,80 30 0)))'),5);
INSERT INTO buildings  VALUES (ST_GeomFromText('POLYGON ((137 89 0, 137 109 0, 153 109 0, 153 89 0, 137 89 0))'),10);
INSERT INTO buildings  VALUES (ST_GeomFromText('MULTIPOLYGON (((140 0 0,230 0 0, 230 60 0, 140 60 0,140 40 0,210 40 0,210 20 0, 140 20 0, 140 0 0)))'),20);


drop table if exists sound_source;
create table sound_source(ID integer, the_geom geometry, db_m63 double,db_m125 double,db_m250 double,db_m500 double,db_m1000 double,db_m2000 double,db_m4000 double,db_m8000 double);
insert into sound_source values (1,'POINT (110 60 1)'::geometry, 80, 80, 80, 80, 80, 80, 80, 80);
insert into sound_source values (2,'POINT (26 61 1)'::geometry, 100, 100, 100, 100, 100, 100, 100, 100);

drop table if exists receivers;
create table receivers(ID integer, the_geom geometry);
insert into receivers values (1,'POINT (10 8 2)'::geometry);
insert into receivers values (2,'POINT (100 50 2)'::geometry);
