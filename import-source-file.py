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

cursor.execute('PRAGMA encoding="UTF-8"')

tablename = config.settings["db"]["staging_data"]
sql = "create table IF NOT EXISTS %s (source text not null, id text not null, json, ts, PRIMARY KEY (source, id))" % tablename
cursor.execute(sql)

sql = 'insert or replace into %s (source, id, json) values(?, ?, ?)' % tablename

with open(filename, 'r', encoding="utf-8") as f:
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
        
        logging.warning("file %s: processing id = %s, source = %s" % (filename, id, source) )
        values = (source, id, json.dumps(doc, ensure_ascii=False))
        logging.debug(sql)
        cursor.execute(sql, values)

sql = "update %s set ts=CURRENT_TIMESTAMP where ts is null" % tablename
logging.debug(sql)
cursor.execute(sql)

conn.commit()
conn.close()
