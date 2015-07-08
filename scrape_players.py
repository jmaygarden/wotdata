#!/usr/bin/env python


""" This script scrapes the necessary statistics for calculating the WN8 of all players on the US World of Tanks server."""

__author__ = "Judge Maygarden"
__copyright__ = "Copyright 2015, Judge Maygarden"


from config import config
import json
import os
import requests
import sqlite3
import sys
import time


CONNECTION_TIMEOUT = 10.0
MIN_ACCOUNT_ID = 1000000000
MAX_ACCOUNT_ID = 1100000000
MAX_BATCH_SIZE = 100


def fetch_json(url):
    r = requests.get(
            url,
            headers={'X-Requested-With': 'XMLHttpRequest'},
            timeout=CONNECTION_TIMEOUT)
    return json.loads(r.text)


def fetch_wot(endpoint, params=None, fields=None):
    url = 'http://api.worldoftanks.com/wot/{0}/?application_id={1}'.format(
            endpoint, config['WOT_APPLICATION_ID'])
    if params is not None:
        url += '&{0}'.format('&'.join(params))
    if fields is not None:
        url += '&fields=%s' % ','.join(fields)
    return fetch_json(url)


def get_account(endpoint, players, fields=None):
    r =  fetch_wot(
            endpoint,
            [ 'account_id={0}'.format(','.join(map(str, players))) ],
            fields)
    if 'data' in r:
        return r['data']
    else:
        raise Exception(r)


def get_account_info(players):
    fields = [
            'clan_id',
            'created_at',
            'last_battle_time',
            'logout_at',
            'nickname',
            'updated_at',
            'statistics.all.battles',
            'statistics.all.damage_dealt',
            'statistics.all.frags',
            'statistics.all.spotted',
            'statistics.all.wins',
            'statistics.all.dropped_capture_points',
            ]
    return get_account('account/info', players, fields)


def get_account_tanks(players):
    fields = [ 'tank_id', 'statistics.battles' ]
    return get_account('account/tanks', players, fields)


def init_db():
    connection = sqlite3.connect('wotdata.db')
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
            or MIN_ACCOUNT_ID
    total = 0

    for account_id in xrange(start, MAX_ACCOUNT_ID, MAX_BATCH_SIZE):
        sys.stdout.write('\rAccount #{0}-{1}'.format(
            account_id, account_id + MAX_BATCH_SIZE))

        batch = xrange(account_id, account_id + MAX_BATCH_SIZE)
        info = get_account_info(batch)
        n = insert_account_info(db, info)
        total += n
        if 0 == n:
            break

        batch = [
                int(key)
                for (key, value) in info.iteritems()
                if value is not None
                ]
        tanks = get_account_tanks(batch)
        insert_account_tanks(db, tanks)

    sys.stdout.write('\nTotal: {0}\n'.format(total))


if __name__ == '__main__':
    main()

