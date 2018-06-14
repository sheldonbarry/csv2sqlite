#!/usr/bin/env python
# -*- coding: utf-8 -*-

# a simple module to convert CSV file to SQLite database
# all fields treated as text

# Copyright SR Barry 2017

import csv
import sqlite3

def log_error(error_text):
    # write errors to log
    with open('error_log.txt', 'a') as a:
        a.write(error_text + '\n')

# extract csv file and put it in a list (rows) of lists (fields)
def csv_parse(filename, Delimiter, Quotechar='"'):
    with open(filename, 'rU') as a:
        data = csv.reader(a, delimiter=Delimiter, quotechar=Quotechar)
        parsed_list = []
        # extract data and return a list of lists
        for row in data:        
            parsed_list.append(row)
    return parsed_list

# function to create table
def do_table(db_name, tb_name, tb_data, data_dict=None):

    # set table header field data
    if data_dict == None:    
        field_data = ''
        for field in tb_data[0]:
            field_data = field_data + '%s TEXT, ' % field
    else:
        field_data = ''
        for field in data_dict:
            field_data = field_data + field[0] + ' ' + field[1] + ', ' 
    # trim trailing comma and space
    field_data = field_data[:-2]

    # create a list to hold data not added to database table
    errors = []

    with sqlite3.connect(db_name) as con:
        
        # need this to work with special characters
        con.text_factory = str
        # set cursor
        cur = con.cursor()
        
        # check to see if table exists and make a fresh table.
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        cur.execute("CREATE TABLE %s (%s)" % (tb_name, field_data))
        con.commit()

        # insert data into table
        for row in tb_data[1:]:
            try:
                # create string of row values to insert into database
                values = ''
                for field in row:
                    # add field value and allow for single quote char
                    fieldvalue = field.decode('utf-8').replace("'","''")
                    values = values + "'" + fieldvalue + "'" + ','
                values = values[:-1]
                values = '(' + values + ')'
            
                # insert data and update
                cur.execute("INSERT INTO %s VALUES %s" % (tb_name, values))                
                con.commit()
            except:
                log_error(str(row))

def create_data_dict(filename, Delimiter, Quotechar, key=None):
    """Create a data dictionary in list format of field names and data types"""
    with open(filename, 'rU') as a:
        data = csv.reader(a, delimiter=Delimiter, quotechar=Quotechar)
        # get header info
        field_names = [field for field in next(data)] 
        # determine data type and store in data_types list variable       
        data_types = ['' for item in field_names]
        for row in data:
            for row_index in range(len(row)):
                try:
                    if int(row[row_index]) % 1 == 0 and data_types[row_index] != 'TEXT':
                        data_types[row_index] = 'INTEGER'
                except:
                    data_types[row_index] = 'TEXT' 
        if key in field_names:
            data_types[field_names.index(key)] += ' PRIMARY KEY'                     
    return zip(field_names, data_types)

