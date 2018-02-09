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
from datetime import datetime
now = datetime.now()

dbo = db.connect()


tablename_data = config.settings["db"]["tablename_staging_data"]

keys = set()

sql = 'select json from %s' % tablename_data
for row in db.execute(dbo, sql):
    new_keys = json.loads(row[0]).keys()
    new_keys = set(k.lower() for k in new_keys)
    keys = keys.union(new_keys)

tablename = config.settings["db"]["tablename_fields"]

##sql = 'delete from %s' % tablename
##db.execute(dbo, sql)

sql = 'insert or replace into %s (field, ts) values (?, ?)' % tablename
for v in list(keys):
    logging.warning("adding to table %s the value '%s'" % (tablename, v))
    values = (v, now, )
    db.execute(dbo, sql, values)

## remove old records
sql = 'delete from %s where ts < (?)' % tablename
values = (now,)
db.execute(dbo, sql, values)

## export csv
db.sqlToCSV(dbo, tablename, tablename=tablename)

db.commit(dbo)
db.close(dbo)

