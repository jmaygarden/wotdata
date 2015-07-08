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
FIELDS = [
        'clan_id',
        'created_at',
        'last_battle_time',
        'logout_at',
        'nickname',
        'updated_at',
        'statistics.all.battles'
        ]


def fetch_json(url):
    r = requests.get(
            url,
            headers={'X-Requested-With': 'XMLHttpRequest'},
            timeout=CONNECTION_TIMEOUT)
    return json.loads(r.text)


def get_players(players, fields=None):
    url = 'http://api.worldoftanks.com/wot/account/info/?'
    params = 'application_id={0}&account_id={1}'.format(
            config['WOT_APPLICATION_ID'],
            ','.join(map(str, players))
            )
    if fields is not None:
        params += '&fields=%s' % ','.join(fields)
    try:
        return fetch_json(url + params)['data']
    except KeyError:
        return None


def init_db():
    connection = sqlite3.connect('wotdata.db')
    cursor = connection.cursor()

    fmt = 'CREATE TABLE IF NOT EXISTS {0}({1})'

    fields = ','.join([
            'account_id INTEGER',
            'clan_id INTEGER',
            'nickname TEXT',
            'created_at TIMESTAMP',
            'last_battle_time TIMESTAMP',
            'logout_at TIMESTAMP',
            'updated_at TIMESTAMP',
            'statistics_all_battles INTEGER',
            'retrieved_at TIMESTAMP',
            ])
    sql = fmt.format('players', fields)
    cursor.execute(sql)

    fields = ','.join([
            'account_id INTEGER',
            'tank_id INTEGER',
            'all_battles INTEGER',
            'all_damage_dealt INTEGER',
            'all_dropped_capture_points INTEGER',
            'all_frags INTEGER',
            'all_spotted INTEGER',
            'all_wins INTEGER'
            ])
    sql = fmt.format('tanks', fields)
    cursor.execute(sql)

    return connection


def insert_players(db, players):
    sql = 'INSERT INTO players VALUES (?,?,?,?,?,?,?,?,?)'
    values = {
            (
                k,
                v['clan_id'],
                v['nickname'],
                v['created_at'],
                v['last_battle_time'],
                v['logout_at'],
                v['updated_at'],
                v['statistics']['all']['battles'],
                int(time.time())
                )
            for (k, v) in players.iteritems()
            if v is not None
            }
    db.executemany(sql, values)
    db.commit()
    return len(values)


def main():
    db = init_db()

    start = db.execute('SELECT MAX(account_id) FROM players').fetchone()[0] \
            or MIN_ACCOUNT_ID

    for account_id in xrange(start, MAX_ACCOUNT_ID, MAX_BATCH_SIZE):
        print account_id
        batch = xrange(account_id, account_id + MAX_BATCH_SIZE)
        players = get_players(batch, FIELDS)
        n = insert_players(db, players)
        if 0 == n:
            break


if __name__ == '__main__':
    main()

