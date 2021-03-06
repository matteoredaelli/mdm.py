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

import csv
import yaml
import re
import pandas as pd
import os

def clean_text(text):
    if text is None or text=="":
        return text
    #t = re.sub("[\t\n\r\"']", " ", text)

    t = re.sub('[\t\n\r"]', " ", str(text)).replace(u'\xa0', u' ')
    return re.sub(" +", u" ", t).strip()

def clean_dict(d):
    for i in list(d.keys()):
        d[i] = clean_text(d[i])
        if d[i] is None or d[i] == "" or d[i] == "missing":
            del d[i]
    return d

def json2df(infile):
    df = pd.read_json(infile, lines=True)
    return df

def extractDataFromFile(infile, outfile, fields):
    df = json2df(infile)
    df[fields].apply(lambda x: x.astype(str).str.upper()).drop_duplicates().sort_values(fields).to_csv(outfile, index=False,mode="a", header=False)

def list2dict(list, sep=":"):
    result = {}
    for e in list:
        m = e.split(sep)
        if len(m) == 2:
            result[m[0].strip()] = m[1].strip()
    return result
    
def load_csv_to_set(filename):
    if not os.path.exists(filename):
        return {}
    with open(filename, "r") as fd:
            lines = fd.read().splitlines()
            return set(lines)

def load_csv_to_dict(filename):
    if not os.path.exists(filename):
        return {}
    with open(filename) as f:
        return dict(filter(None, csv.reader(f)))

def load_yaml_to_dict(filename):
    if not os.path.exists(filename):
        return {}
    with open(filename, 'r') as f:
        return yaml.load(f)

def dict_convert_lower(dic):
    return dict((k.lower(), v) for k, v in dic.items())

def merge2dicts(item1, item2, append=False):
    ## TODO: append=True dow not work
    for f in item2:
        if not f in item1:
            item1[f] = item2[f]
        elif append:
            ## multivalues are allowed
            v1 = item1[f]
            if not isinstance(v1, list):
                v1 = [v1]
            v2 = item2[f]
            if not isinstance(v2, list):
                v2 = [v2]
            ## only unique values
            v = list(set(v1 + v2))
            ## value will be a string if there is only 1 element in the lust
            if len(v) == 1:
                v = v[0]
            item1[f] = v
    return item1

def dict_rename_keys(data, fields):
        data_new = data
        keys = fields.keys()
        for k in list(data.keys()):
            if k in keys:
                data_new[fields[k]] = data[k]
                data_new.pop(k)
        return data_new
