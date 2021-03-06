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


import db
import mdm.staging
import argparse

parser = argparse.ArgumentParser(prog='shell.py')
parser.add_argument('cmd')
parser.add_argument('--filename', help=' of the %(prog)s program')
args = parser.parse_args()

dbo = db.connect()

if cmd == "load_s_data":
    filename = args.filename
    mdm.staging.load_s_data(dbo, filename)



db.commit(dbo)
db.close(dbo)
