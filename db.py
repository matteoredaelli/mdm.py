# -*- coding: utf-8 -*-

#    Copyright (C) 2016-2017 Matteo.Redaelli@gmail.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import config
import logging
import json
import sqlite3
import csv
from pathlib import Path

def connect():
    conn = sqlite3.connect(config.settings["db"]["name"])
    cursor = conn.cursor()
    cursor.execute('PRAGMA encoding="UTF-8"')
    ##cursor.execute('.nullvalue NULL')
    return {"connection": conn, "cursor": cursor}

def close(dbo):
    dbo["connection"].close()

def commit(dbo):
    dbo["connection"].commit()
    
def execute(dbo, sql, values=None):
    logging.warning(sql)
    if values:
        return dbo["cursor"].execute(sql, values)
    
    return dbo["cursor"].execute(sql)

def fetchall(dbo):
    return dbo["cursor"].fetchall()
    
def setup(dbo):
    tablename = config.settings["db"]["tablename_s_data"]
    sql = "create table IF NOT EXISTS %s (source text not null, id text not null, json, ts timestamp, PRIMARY KEY (source, id))" % tablename
    execute(dbo, sql)
    
    tablename = config.settings["db"]["tablename_fields"]
    ##sql = 'drop table if exists %s' % tablename
    ##db.execute(dbo, sql)
    sql = 'create table if not exists %s (field primary key, rename_to, ts timestamp)' % tablename
    execute(dbo, sql)
    
    tablename = config.settings["db"]["tablename_fields_values"]
    sql = 'create table if not exists %s (field, value, rename_to, ts timestamp, primary key (field, value))' % tablename
    execute(dbo, sql)

    commit(dbo)
    
def sql2csv(dbo, csvfile, tablename=None, sql=None):
    if sql is None:
        if tablename is None:
            logging.error("sql2csv: tablename or sql must exist")
            return
        sql = "select * from %s" % tablename
        
    outpath = config.settings["fs"]["target_dir"]
    filename  = Path(outpath).joinpath(csvfile + ".csv")
    spamWriter = csv.writer(open(filename, 'w', newline=''),
                                delimiter=',') #,
                                ##quotechar='',
                                ##quoting=csv.QUOTE_MINIMAL)
    for row in dbo["cursor"].execute(sql):
        spamWriter.writerow(row)

def csv2sql(dbo, csvfile, tablename):
    path = config.settings["fs"]["target_dir"]
    filename  = Path(path).joinpath(csvfile + ".csv")
    with open(filename, newline='') as f:
        sql = "delete from %s" % tablename
        execute(dbo, sql)
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            question_marks = ", ?" * len(row)
            values = tuple(row)
            sql = "insert or replace into %s values(%s)" % (tablename, question_marks[1:])
            execute(dbo, sql, values)
        commit(dbo)

            
