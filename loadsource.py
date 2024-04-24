#!/usr/bin/env python

import sqlite3
import sys
import os.path

def main():
    if len(sys.argv) < 3:
        print(f'Usage: {sys.argv[0]} [dbname] [inputfile] ...', file=sys.stderr)
        sys.exit(-1)

    db = sqlite3.connect(sys.argv[1])
    db.execute(u'CREATE TABLE IF NOT EXISTS pairs (key string, value string)')

    for fname in sys.argv[2:]:
        print(f'processing {os.path. basename(fname)}...')
        with open(fname) as fp:
            offset = 0
            for line in fp:
                key = f"{os.path.basename(fname)}:{offset:9d}"
                value = line.rstrip('\r\n')
                offset += len(line)

                db.execute("INSERT INTO pairs VALUES (?, ?)", (key, value))

    db.commit()
    db.close()
if __name__ == '__main__':
    main()
