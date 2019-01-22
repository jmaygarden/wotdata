#!/usr/bin/python -i

""" This module provides functions for retrieving data from the WoT API. """

__author__ = "Judge Maygarden"
__copyright__ = "Copyright 2015, Judge Maygarden"


from config import Config
import json
import requests


def fetch_json(url):
    r = requests.get(
            url,
            headers={'X-Requested-With': 'XMLHttpRequest'},
            timeout=Config.CONNECTION_TIMEOUT)
    return json.loads(r.text)


def fetch_wot(endpoint, params=None, fields=None):
    url = 'http://api.worldoftanks.com/wot/{0}/?application_id={1}'.format(
            endpoint, Config.WOT_APPLICATION_ID)
    if params is not None:
        url += '&{0}'.format('&'.join(params))
    if fields is not None:
        url += '&fields={0}'.format(','.join(fields))
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


def get_clan(endpoint, clanId, fields=None):
    r =  fetch_wot(
            endpoint,
            [ 'clan_id={0}'.format(clanId) ],
            fields)
    if 'data' in r:
        return r['data']
    else:
        raise Exception(r)


def get_globalmap_claninfo(clanId):
    fields = [
            'name',
            'tag'
            ]
    return get_clan('globalmap/claninfo', clanId, fields)


def get_globalmap_clanbattles(clanId):
    fields = [
            'attack_type',
            'competitor_id',
            'front_name',
            'province_name',
            'time',
            'type'
            ]
    return get_clan('globalmap/clanbattles', clanId, fields)


