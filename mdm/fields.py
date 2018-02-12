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

def get_fields(dbo):
    tablename_fields = config.settings["db"]["tablename_fields"]
    ## extract the  fields of teh new production tble
    sql = "select rename_to as field from fields where rename_to is not null UNION select field from fields where rename_to is null or rename_to = ''"
    db.execute(dbo, sql)
    return dbo["cursor"].fetchall()
    
def get_fields_mapping(dbo):
    tablename_fields = config.settings["db"]["tablename_fields"]
    ## extract the  mapping
    sql = "select field, rename_to  from %s where rename_to is not null" % tablename
    db.execute(dbo, sql)
    mapping = dbo["cursor"].fetchall()
    return dict(mapping)
