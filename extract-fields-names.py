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

conn = sqlite3.connect(config.settings["db"]["name"])
cursor = conn.cursor()

tablename = config.settings["db"]["staging_data"]

keys = set()

sql = 'select json from %s' % tablename
for row in cursor.execute(sql):
    new_keys = json.loads(row[0]).keys()
    keys = keys.union(new_keys)

tablename = config.settings["db"]["fields"]

sql = 'drop table if exists %s' % tablename
cursor.execute(sql)

sql = 'create table if not exists %s (field primary key)' % tablename
cursor.execute(sql)

sql = 'insert into %s values (?)' % tablename
for v in list(keys):
    logging.warning("adding to table %s the value '%s'" % (tablename, v))
    values = (v,)
    cursor.execute(sql, values)
                        
conn.commit()
conn.close()

print(keys)
