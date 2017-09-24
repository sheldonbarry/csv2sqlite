#!/usr/bin/env python
# -*- coding: utf-8 -*-

# csv2sqlite package folder must be in same directory as this Python script

from csv2sqlite import csv2sqlite

# CSV database info
csvFile = 'Cellar.csv'
csvDelimiter = ';'
csvQuotechar = '"'

# SQLite database info
sqlFile = 'wineCellar.db'
sqlTable = 'Cellar'
keyField = 'BIN'

# lists to be added to database
myData = csv2sqlite.csv_parse(csvFile, csvDelimiter)

# create data dictionary with field names and data types
data_dict = csv2sqlite.create_data_dict(csvFile, csvDelimiter, csvQuotechar, keyField)

# perform create table function
csv2sqlite.do_table(sqlFile, sqlTable, myData, data_dict)

