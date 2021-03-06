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
import db
from datetime import datetime

def load_s_data(dbo, filename):
    now = datetime.now()
    uid = config.settings["mdm"]["uid"]

    tablename = config.settings["db"]["tablename_s_data"]
    db.setup(dbo)

    sql = 'insert or replace into %s (source, id, json, ts) values(?, ?, ?, ?)' % tablename

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
            values = (source, id, json.dumps(doc, ensure_ascii=False), now)
            logging.debug(sql)
            db.execute(dbo, sql, values)

    sql = "update %s set ts=CURRENT_TIMESTAMP where ts is null" % tablename
    logging.debug(sql)
    db.execute(dbo, sql)

    db.commit(dbo)

