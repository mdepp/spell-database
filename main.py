import csv
import pickle
import sqlite3
from os import path
from typing import List

import click
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from tabulate import tabulate


@click.group()
def cli():
    pass


def authenticate():
    # As https://developers.google.com/sheets/api/quickstart/python
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = None
    if path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scopes)
            creds = flow.run_local_server()

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


@cli.command()
def download():
    service = build('sheets', 'v4', credentials=authenticate())
    spreadsheet_id = '1cuwb3QSvWDD7GG5McdvyyRBpqycYuKMRsXgyrvxvLFI'
    sheet_title = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets'][0]['properties']['title']
    rows = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_title).execute()['values']
    for row in rows:
        for i in range(len(row)):
            if row[i] in ('NULL', ''):
                row[i] = None
    with open('spells.csv', 'w') as file:
        csv.writer(file).writerows(rows)


def guess_column_type(rows: List[List[str]], index: int) -> str:
    is_int = True
    is_float = True

    for row in rows[1:]:
        cell = row[index]
        if cell is None:
            continue
        try:
            _ = int(cell)
        except ValueError:
            is_int = False
        try:
            _ = float(cell)
        except ValueError:
            is_float = False

    if is_int:
        return 'INTEGER'
    elif is_float:
        return 'REAL'
    else:
        return 'TEXT'


@cli.command()
def make_database():
    with open('spells.csv') as file:
        rows = []
        for row in csv.reader(file):
            rows.append([cell if cell != '' else None for cell in row])

    headers = rows[0]
    col_spec = ', '.join(f'{column} {guess_column_type(rows, index)}' for index, column in enumerate(headers))
    db = sqlite3.connect('spells.db')
    cursor = db.cursor()
    cursor.execute('DROP TABLE IF EXISTS spells')
    cursor.execute(f'CREATE TABLE spells ({col_spec})')

    col_names = ', '.join(headers)
    value_placeholders = ', '.join('?' * len(rows[0]))
    for row in rows[1:]:
        cursor.execute(f'INSERT INTO spells ({col_names}) VALUES ({value_placeholders})', row)
    db.commit()
    db.close()


@cli.command()
@click.argument('sql', type=str)
@click.option('--style', type=str)
def query(sql: str, style: str):
    db = sqlite3.connect('spells.db')
    cursor = db.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    db.close()
    headers = [d[0] for d in cursor.description]
    style = style or 'fancy_grid'
    print(tabulate(rows, headers, style))


if __name__ == '__main__':
    cli()
