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
import db
import utils

dbo = db.connect()

tablename_staging = config.settings["db"]["tablename_staging_data"]
tablename_working = config.settings["db"]["tablename_working_data"]
tablename_fields = config.settings["db"]["tablename_fields"]

## extract the  fields of teh new production tble
sql = "select rename_to as field from fields where rename_to is not null UNION select field from fields where rename_to is null or rename_to = ''"
db.execute(dbo, sql)
fields = dbo["cursor"].fetchall()
fields = ['"' + x[0] +'"' for x in fields]
fields_string = ", ".join(fields)

## extract the  mapping
sql = "select field, rename_to  from fields where rename_to is not null"
db.execute(dbo, sql)
mapping = dbo["cursor"].fetchall()
mapping = dict(mapping)

sql = 'drop table if exists %s' % tablename_working
db.execute(dbo, sql)

sql = 'create table %s (%s, primary key (source, id))' % (tablename_working, fields_string)
db.execute(dbo, sql)

sql = 'select json from %s' % tablename_staging

db.execute(dbo, sql)
rows = db.fetchall(dbo)

for row in rows:
    doc = json.loads(row[0])
    #doc1 = {}.fromkeys(fields)
    doc = utils.dict_convert_lower(doc)
    doc = utils.dict_rename_keys(doc, mapping)
    doc_keys = doc.keys()
    doc_fields = ['"' + x +'"' for x in doc_keys]
    doc_fields_string = ", ".join(doc_fields)
    question_marks = ", ?" * len(doc_keys)
    values = tuple(doc.values())
    sql_insert = "insert into %s (%s) values (%s)" % (tablename_working, doc_fields_string, question_marks[1:])
    db.execute(dbo, sql_insert, values)

## export csv
db.sqlToCSV(dbo, tablename_working, tablename=tablename_working)

db.commit(dbo)
db.close(dbo)
