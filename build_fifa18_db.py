import sqlite3
import csv


# used for getting rows from a csv file called filename
def csv_row_generator(filename):
    with open(filename, encoding='utf-8') as f:
        # creates a dictionary with each column name being a key
        # and a cell in that column for some given row as the value
        reader = csv.DictReader(f)
        for row in reader:
            # yield reads one row (as a dictionary) and then pauses
            # this function to continue on with rest of code in the loop
            # this function is used in --> conserves memory & allows pipelining
            yield row


# schema for the fifa18 sqlite3 database
def create_tables(conn):
    cur = conn.cursor()

    # drop tables in case they already exist
    cur.execute('DROP TABLE IF EXISTS club')
    cur.execute('DROP TABLE IF EXISTS league')
    cur.execute('DROP TABLE IF EXISTS body_type')
    cur.execute('DROP TABLE IF EXISTS nationality')
    cur.execute('DROP TABLE IF EXISTS player')

    cur.execute('CREATE TABLE club(club_id INTEGER PRIMARY KEY, '
                'club TEXT UNIQUE, club_logo TEXT UNIQUE)')
    cur.execute('CREATE TABLE league(league_id INTEGER PRIMARY KEY, '
                'league TEXT)')
    cur.execute('CREATE TABLE body_type(body_type_id INTEGER PRIMARY KEY, '
                'body_type TEXT UNIQUE)')
    cur.execute('CREATE TABLE nationality(nationality_id INTEGER PRIMARY KEY, '
                'nationality TEXT UNIQUE, flag TEXT UNIQUE)')
    cur.execute('CREATE TABLE player(player_id INTEGER PRIMARY KEY, '
                'ID INTEGER, name TEXT, full_name TEXT, '
                'club_id INTEGER, special INTEGER, age INTEGER, '
                'league_id INTEGER, birth_date TEXT, height_cm INTEGER, '
                'weight_kg INTEGER, body_type_id INTEGER, real_face TEXT, '
                'nationality_id INTEGER, photo TEXT UNIQUE, '
                'eur_value INTEGER, eur_wage INTEGER, '
                'eur_release_clause INTEGER, overall INTEGER, '
                'FOREIGN KEY(club_id) REFERENCES club(club_id), '
                'FOREIGN KEY(league_id) REFERENCES league(league_id), '
                'FOREIGN KEY(body_type_id) '
                'REFERENCES body_type(body_type_id), '
                'FOREIGN KEY(nationality_id) REFERENCES '
                'nationality(nationality_id))')

    conn.commit()


# handles inserts into the club table
# returns club_id of entity just inserted
def insert_club(conn, club, club_logo):
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO club(club, club_logo) VALUES(?, ?)',
                (club, club_logo))
    conn.commit()

    # get id of club just inserted
    cur.execute('SELECT club_id FROM club WHERE club = ?', (club,))
    club_id = cur.fetchone()[0]

    return club_id


# handles inserts into the league table
# returns league_id of entity just inserted
def insert_league(conn, league):
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO league(league) VALUES(?)',
                (league,))
    conn.commit()

    # get id of club just inserted
    cur.execute('SELECT league_id FROM league WHERE league = ?', (league,))
    league_id = cur.fetchone()[0]

    return league_id


# handles inserts into the body_type table
# returns body_type_id of entity just inserted
def insert_body_type(conn, body_type):
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO body_type(body_type) VALUES(?)',
                (body_type,))
    conn.commit()

    # get id of club just inserted
    cur.execute('SELECT body_type_id FROM body_type WHERE body_type = ?',
                (body_type,))
    body_type_id = cur.fetchone()[0]

    return body_type_id


# handles inserts into the nationality table
# returns nationality_id of entity just inserted
def insert_nationality(conn, nationality, flag):
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO nationality(nationality, flag) '
                'VALUES(?, ?)', (nationality, flag))
    conn.commit()

    # get id of club just inserted
    cur.execute('SELECT nationality_id FROM nationality WHERE nationality = ?',
                (nationality,))
    nationality_id = cur.fetchone()[0]

    return nationality_id


# handles inserts into the player table
# nothing to return
def insert_player(conn, ID, name, full_name, club_id, special, age,
                  league_id, birth_date, height_cm, weight_kg, body_type_id,
                  real_face, nationality_id, photo, eur_value, eur_wage,
                  eur_release_clause, overall):
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO '
                'player(ID, name, full_name, club_id, special, age, '
                'league_id, birth_date, height_cm, weight_kg, body_type_id, '
                'real_face, nationality_id, photo, eur_value, eur_wage, '
                'eur_release_clause, overall) '
                'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (ID, name, full_name, club_id, special, age, league_id,
                 birth_date, height_cm, weight_kg, body_type_id,
                 real_face, nationality_id, photo, eur_value, eur_wage,
                 eur_release_clause, overall))
    conn.commit()


def main():
    conn = sqlite3.connect('fifa18.sqlite')

    create_tables(conn)

    for row in csv_row_generator('fifa18.csv'):
        club_id = insert_club(conn, row['club'], row['club_logo'])
        league_id = insert_league(conn, row['league'])
        body_type_id = insert_body_type(conn, row['body_type'])
        nationality_id = insert_nationality(conn, row['nationality'],
                                            row['flag'])
        insert_player(conn, row['ID'], row['name'], row['full_name'], club_id,
                      row['special'], row['age'], league_id, row['birth_date'],
                      row['height_cm'], row['weight_kg'], body_type_id,
                      row['real_face'], nationality_id, row['photo'],
                      row['eur_value'], row['eur_wage'],
                      row['eur_release_clause'], row['overall'])


if __name__ == '__main__':
    main()
