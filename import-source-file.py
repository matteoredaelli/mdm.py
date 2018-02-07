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
import sys
import json
import sqlite3

filename = sys.argv[1]

uid = config.settings["mdm"]["uid"]

conn = sqlite3.connect(config.settings["db"]["name"])
cursor = conn.cursor()

tablename = config.settings["db"]["staging_data"]

sql = "create table IF NOT EXISTS %s (id PRIMARY KEY, json)" % tablename
cursor.execute(sql)

sql = 'insert or replace into %s (id, json) values(?, ?)' % tablename

with open(filename, 'r') as f:
    for line in f:
        line = line.strip()
        doc = json.loads(line)

    
        if uid in doc:
            id = doc[uid]
        else:
            id = doc["id"]

        if "source"  in doc:
            source = doc["source"]
        else:
            source = "_unknown"
        id = "%s-%s" % (source, id)    
        logging.warning("Parsing file %s with id %s from source %s" % (filename, id, source) )
        values = (id, json.dumps(doc),)
        logging.debug(sql)
        cursor.execute(sql, values)
    
conn.commit()
conn.close()
