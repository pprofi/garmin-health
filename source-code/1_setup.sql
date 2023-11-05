/*
Initial Version 0.1
17-05-2023

Structures for storing and loading data

 */

 use role sysadmin;

-- 1. create database for storing health data
 create database health_data;

-- 2. create schemas : raw, curated, transformed, presentation
 create schema  raw;
 create schema curated;
 create schema transformed;
 create schema presentation;


-- 3. create named internal stage for loading data in raw schema

use schema raw;

create  or replace stage raw_load
 directory= (enable=true);
