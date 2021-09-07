import jdatetime
import re
import os

arrFilename = os.listdir('../780/EN TARAKONESH')
date_pattern = re.compile(r'\b(\d{4})(\d{2})(\d{2})\b')

def get_date(filename):
    matched = date_pattern.search(filename)

    if not matched:
        return None
    m, d, y = map(int, matched.groups())
    return jdatetime.date(m, d, y).togregorian()


dates = (get_date(fn) for fn in arrFilename)
dates = (d for d in dates if d is not None)
last_date = max(dates)
# last_date = last_date.strftime('%Y-%m-%d')
last_j = jdatetime.datetime.fromgregorian(day=int(last_date.strftime('%d')), month=int(last_date.strftime('%m')),
                                          year=int(last_date.strftime('%Y'))).strftime("%Y%m%d")
filenames = [fn for fn in arrFilename if last_j in fn]
for fn in filenames:
    print(fn)
