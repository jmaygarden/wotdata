
from config import Config
from api import *
from sqlite3 import connect


def init_db():
    connection = connect(Config.DB_PATH)
    cursor = connection.cursor()
    return cursor


def load_table(filename):
    with open(filename) as fin:
        table = json.loads(fin.read())
        def remove_id(entry):
            r = dict(entry)
            del r['IDNum']
            return r
        return { entry['IDNum']: remove_id(entry) for entry in table['data'] }


def calculate_wn8(account_id):
    db = init_db()

    table = load_table('expected_tank_values_20.json')

    query = db.execute('SELECT tank_id, all_battles FROM tanks WHERE account_id=?', (account_id,))

    total = {
            'expDamage': 0.0,
            'expWinRate': 0.0,
            'expDef': 0.0,
            'expSpot': 0.0,
            'expFrag': 0.0,
            }

    for record in query:
        tank_id, battles = record
        exp = table[tank_id]
        for key in total:
            total[key] += battles * exp[key]
    total['expWinRate'] /= 100.0

    query = db.execute('SELECT statistics_all_damage_dealt,statistics_all_frags,statistics_all_spotted,statistics_all_wins,statistics_all_dropped_capture_points FROM players WHERE account_id=?', (account_id,))
    account = query.fetchone()

    rDAMAGE = account[0] / total['expDamage']
    rSPOT = account[2] / total['expSpot']
    rFRAG = account[1] / total['expFrag']
    rDEF = account[4] / total['expDef']
    rWIN = account[3] / total['expWinRate']

    rWINc = max(0, (rWIN - 0.71) / (1.0 - 0.71))
    rDAMAGEc = max(0, (rDAMAGE - 0.22) / (1.0 - 0.22))
    rFRAGc = max(0, min(rDAMAGEc + 0.2, (rFRAG - 0.12) / (1.0 - 0.12)))
    rSPOTc = max(0, min(rDAMAGEc + 0.1, (rSPOT - 0.38) / (1.0 - 0.38)))
    rDEFc = max(0, min(rDAMAGEc + 0.1, (rDEF - 0.10) / (1.0 - 0.10)))

    WN8 = 980*rDAMAGEc + 210*rDAMAGEc*rFRAGc + 155*rFRAGc*rSPOTc + 75*rDEFc*rFRAGc + 145*min(1.8,rWINc)

    return int(round(WN8))


if __name__ == '__main__':
    account_id = '1006905943'
    wn8 = calculate_wn8(account_id)
    print wn8

