
from collections import namedtuple
from config import Config
from api import *
from sqlite3 import connect


Stats = namedtuple('Stats', ['damage_dealt', 'frags', 'spotted', 'wins', 'dropped_capture_points'])


def init_db():
    connection = connect(Config.DB_PATH)
    cursor = connection.cursor()
    return cursor


class ExpectedTable(object):
    def __init__(self, filename):
        with open(filename) as fin:
            lut = json.loads(fin.read())
            def remove_id(entry):
                r = dict(entry)
                del r['IDNum']
                return r
            self.lut = {
                    entry['IDNum']: remove_id(entry)
                    for entry in lut['data']
                    }

    def get_expected(self, tanks):
        damage_dealt, frags, spotted, wins, dropped_capture_points = (
                0.0, 0.0, 0.0, 0.0, 0.0
                )
        for tank_id, battles in tanks:
            lut = self.lut[tank_id]
            damage_dealt += battles * lut['expDamage']
            frags += battles * lut['expFrag']
            spotted += battles * lut['expSpot']
            wins += battles * lut['expWinRate'] / 100.0
            dropped_capture_points += battles * lut['expDef']
        return Stats(damage_dealt, frags, spotted, wins, dropped_capture_points)


def calculate_wn8(actual, expected):
    rDAMAGE = actual.damage_dealt / expected.damage_dealt
    rSPOT = actual.spotted / expected.spotted
    rFRAG = actual.frags / expected.frags
    rDEF = actual.dropped_capture_points / expected.dropped_capture_points
    rWIN = actual.wins / expected.wins

    rWINc = max(0, (rWIN - 0.71) / (1.0 - 0.71))
    rDAMAGEc = max(0, (rDAMAGE - 0.22) / (1.0 - 0.22))
    rFRAGc = max(0, min(rDAMAGEc + 0.2, (rFRAG - 0.12) / (1.0 - 0.12)))
    rSPOTc = max(0, min(rDAMAGEc + 0.1, (rSPOT - 0.38) / (1.0 - 0.38)))
    rDEFc = max(0, min(rDAMAGEc + 0.1, (rDEF - 0.10) / (1.0 - 0.10)))

    WN8 = \
            980 * rDAMAGEc + \
            210 * rDAMAGEc * rFRAGc + \
            155 * rFRAGc * rSPOTc + \
            75 * rDEFc * rFRAGc + \
            145 * min(1.8, rWINc)

    return int(round(WN8))


def lookup_stats(db, lut, account_id):
    query = db.execute('SELECT tank_id, all_battles FROM tanks WHERE account_id=?', (account_id,))
    expected = lut.get_expected(query)

    query = db.execute('SELECT statistics_all_damage_dealt,statistics_all_frags,statistics_all_spotted,statistics_all_wins,statistics_all_dropped_capture_points FROM players WHERE account_id=?', (account_id,))
    actual = Stats(*query.fetchone())

    return expected, actual


if __name__ == '__main__':
    db = init_db()
    account_id = '1006905943'
    lut = ExpectedTable('expected_tank_values_20.json')
    expected, actual = lookup_stats(db, lut, account_id)
    wn8 = calculate_wn8(actual, expected)
    print wn8
    response = get_account_info([account_id])
    print response
    response = get_account_tanks([account_id])
    tanks = [
            (tank['tank_id'], tank['statistics']['battles'])
            for tank in response[account_id]
            ]
    print tanks

