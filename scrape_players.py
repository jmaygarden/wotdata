#!/usr/bin/env python

""" This script scrapes the necessary statistics for calculating the WN8 of all players on the US World of Tanks server."""

__author__ = "Judge Maygarden"
__copyright__ = "Copyright 2015, Judge Maygarden"


from config import Config
from api import *
from sqlite3 import connect
import sys
import time


def init_db():
    connection = connect('wotdata.db')
    cursor = connection.cursor()

    fmt = 'CREATE TABLE IF NOT EXISTS {0}({1})'

    fields = ','.join([
            'account_id INTEGER PRIMARY KEY',
            'clan_id INTEGER',
            'nickname TEXT',
            'created_at TIMESTAMP',
            'last_battle_time TIMESTAMP',
            'logout_at TIMESTAMP',
            'updated_at TIMESTAMP',
            'statistics_all_battles INTEGER',
            'statistics_all_damage_dealt INTEGER',
            'statistics_all_frags INTEGER',
            'statistics_all_spotted INTEGER',
            'statistics_all_wins INTEGER',
            'statistics_all_dropped_capture_points INTEGER',
            'player_retrieved_at TIMESTAMP',
            'tanks_retrieved_at TIMESTAMP',
            'tank_details_retrieved_at TIMESTAMP',
            ])
    sql = fmt.format('players', fields)
    cursor.execute(sql)

    fields = ','.join([
            'account_id INTEGER NOT NULL',
            'tank_id INTEGER NOT NULL',
            'all_battles INTEGER',
            'all_damage_dealt INTEGER',
            'all_dropped_capture_points INTEGER',
            'all_frags INTEGER',
            'all_spotted INTEGER',
            'all_wins INTEGER',
            'FOREIGN KEY(account_id) REFERENCES players(account_id)',
            'PRIMARY KEY(account_id, tank_id)'
            ])
    sql = fmt.format('tanks', fields)
    cursor.execute(sql)

    return connection


def insert_account_info(db, info):
    sql = 'INSERT INTO players VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    values = [
            (
                k,
                v['clan_id'],
                v['nickname'],
                v['created_at'],
                v['last_battle_time'],
                v['logout_at'],
                v['updated_at'],
                v['statistics']['all']['battles'],
                v['statistics']['all']['damage_dealt'],
                v['statistics']['all']['frags'],
                v['statistics']['all']['spotted'],
                v['statistics']['all']['wins'],
                v['statistics']['all']['dropped_capture_points'],
                int(time.time()),
                0,
                0
                )
            for (k, v) in info.iteritems()
            if v is not None
            ]
    db.executemany(sql, values)
    db.commit()
    return len(values)


def insert_account_tanks(db, tanks):
    sql = 'INSERT INTO tanks(account_id, tank_id, all_battles) VALUES (?,?,?)'
    for account_id, data in tanks.iteritems():
        values = [
                (account_id, tank['tank_id'], tank['statistics']['battles'])
                for tank in data
                ]
        db.executemany(sql, values)
    db.commit()
    return len(values)


def main():
    db = init_db()

    start = db.execute('SELECT MAX(account_id) FROM players').fetchone()[0] \
            or Config.MIN_ACCOUNT_ID
    if start > Config.MIN_ACCOUNT_ID:
        start += 1
    total = 0
    consecutive_empty_batches = 0

    sys.stdout.write('Start: {0}\n'.format(time.asctime()))

    for account_id in xrange(start, Config.MAX_ACCOUNT_ID, Config.MAX_BATCH_SIZE):
        sys.stdout.write('\rAccount #{0}-{1}'.format(
            account_id, account_id + Config.MAX_BATCH_SIZE))
        sys.stdout.flush()

        batch = xrange(account_id, account_id + Config.MAX_BATCH_SIZE)
        info = get_account_info(batch)
        n = insert_account_info(db, info)
        total += n
        if 0 == n:
            consecutive_empty_batches += 1
            #if consecutive_empty_batches >= 10:
            #    break
        else:
            consecutive_empty_batches = 0

        batch = [
                int(key)
                for (key, value) in info.iteritems()
                if value is not None
                ]
        if 0 < len(batch):
            tanks = get_account_tanks(batch)
            insert_account_tanks(db, tanks)

    sys.stdout.write('\nStop:  {0}\nTotal: {1}\n'.format(time.asctime(), total))


if __name__ == '__main__':
    main()

