import logging
import os
import time
import re

from bs4 import BeautifulSoup
import gspread
from lxml import html
import pickle
import requests
import sqlite3 as sql
import time

import db
from db import DBManager
from models import Match, Team

log = logging.getLogger(__name__)

def get_credentials(args):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    from apiclient import discovery
    from oauth2client import client
    from oauth2client import tools
    from oauth2client.file import Storage

    # If modifying these scopes, delete your previously saved credentials
    # at ~/.credentials/sheets.googleapis.com-python-quickstart.json
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    CLIENT_SECRET_FILE = 'secrets/client_secret.json'
    APPLICATION_NAME = 'CLI'

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if args:
            credentials = tools.run_flow(flow, store, args)
        else:
            credentials = tools.run(flow, store)
        log.debug('Storing credentials to ' + credential_path)
    return credentials


class Scraper:
    def __init__(self, eventid, num_daysadvance=1):
        self.db = None

        self.teams = {}
        self.matches = {}

        self.regex_matchid = re.compile('matches\/([0-9]{2,10})')
        self.regex_teamid = re.compile('team\/([0-9]{2,10})')

        self.eventid = eventid
        self.num_daysadvance = num_daysadvance

    def update(self):
        self.upcoming_response = requests.get(f'http://www.hltv.org/matches?event={self.eventid}')
        self.upcoming_soup = BeautifulSoup(self.upcoming_response.content, "html.parser")

        self.results_response = requests.get(f'http://www.hltv.org/results?event={self.eventid}')
        self.results_soup = BeautifulSoup(self.results_response.content, "html.parser")

        teams_overview_link = self.upcoming_soup.find("a", {"class": "event-nav inactive"})
        teams_overview_link = "https://www.hltv.org" + teams_overview_link["href"]
        self.teams_response = requests.get(teams_overview_link)
        self.teams_soup = BeautifulSoup(self.teams_response.content, "html.parser")

    def get_matches(self):
        return self.get_ongoing_matches(), self.get_upcoming_matches()

    def get_teams(self):
        groups_container = self.teams_soup.find("div", {"class": "groups-container"})
        table = groups_container.find("table")

        rows = table.find_all("tr")
        for row in rows:
            if not row.has_attr("class"):
                continue

            anchor = row.find("a")
            t_id = int(re.search(self.regex_teamid, anchor["href"]).group(1))
            t_name = anchor.text

            if t_id in self.teams.keys():
                pass
            else:
                self.teams[t_id] = Team(id=t_id, name=t_name)


    def get_upcoming_matches(self):
        matchdays = self.upcoming_soup.find_all("div", {"data-zonedgrouping-headline-classes": "standard-headline"}, limit=self.num_daysadvance)

        if matchdays is None:
            return []

        for s in matchdays[0].find_all("div", {"class": "match-day"}):
            match_day = s.find("span", {"class": "standard-headline"})
            match_day = match_day.text

            for matchdata in s.find_all("a"):
                m_id = int(re.search(self.regex_matchid, matchdata["href"]).group(1))
                m_unix_ts = int(matchdata["data-zonedgrouping-entry-unix"])
                m_state = 1

                m_map = ''

                table = matchdata.find("tr")
                teams = []
                for tag in table.find_all("div", class_="team map-text".split()):
                    if tag["class"][0] == "team":
                        teams.append(tag.text)
                    elif tag["class"][0] == "map-text":
                        m_map = tag.text

                m_teamname1 = teams[0]
                m_teamname2 = teams[1]

                match = Match(id=m_id, unix_ts=m_unix_ts, state=m_state, map=m_map, teamname1=m_teamname1, teamname2=m_teamname2)
                self.matches[m_id] = match

                log.info(f"Finished upcoming match (id: {match.id})")

            log.info(f"Finished upcoming matchday (date: {match_day})")

    def get_ongoing_matches(self):
        live_matches = self.upcoming_soup.find("div", {"class": "live-matches"})

        if live_matches is None:
            return []

        for live_match in live_matches.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['live-match']):
            matchdata = live_match.find("a")

            table = matchdata.find("table")
            m_id = int(table["data-livescore-match"])

            m_map = ''
            bestof = table.find("td", class_="bestof").text
            multiple_maps = False
            if "1" in bestof:
                m_map = table.find("td", class_="map").text.lower()
            else:
                multiple_maps = True
                map_count = table.find("td", class_="map")
                m_map = "bo" + len(map_count)

            teamscores = []
            if multiple_maps:
                for scorechart in table.find_all("tr"):
                    if scorechart.has_attr("class"):
                        continue

                    container = scorechart.find("td", class_="total")
                    try:
                        score = int(container.span.text)
                    except ValueError:
                        score = 0

                    teamscores.append(score)
            else:
                for scorechart in table.find_all("tr"):
                    if scorechart.has_attr("class"):
                        continue

                    container = scorechart.find("td", class_="livescore")
                    try:
                        score = int(container.span.text)
                    except ValueError:
                        score = 0

                    teamscores.append(score)

            m_teamscore1 = teamscores[0]
            m_teamscore2 = teamscores[1]

            if m_id in self.matches.keys():
                self.matches[m_id].state = 0
                self.matches[m_id].map = m_map

                self.matches[m_id].teamscore1 = m_teamscore1
                self.matches[m_id].teamscore2 = m_teamscore2

            else:
                m_unix_ts = int(time.time())
                m_state = 0
                teams = []
                for tag in table.find_all("span", class_="team-name"):
                    teams.append(tag.text)

                m_teamname1 = teams[0]
                m_teamname2 = teams[1]

                match = Match(id=m_id, unix_ts=m_unix_ts, state=m_state, map=m_map, teamname1=m_teamname1, teamname2=m_teamname2, teamscore1=m_teamscore1, teamscore2=m_teamscore2)

                self.matches[m_id] = match

                log.info(f"Finished ongoing match (id: {m_id}")

    def get_results(self):
        results = self.results_soup.find_all("div", {"class": "result-con"})

        if results is None:
            return []

        for result in results:
            table = result.find("a")

            m_id = int(re.search(self.regex_matchid, table["href"]).group(1))

            td = table.find("td", class_="date-cell")
            span = td.find("span")
            m_unix_ts = int(span["data-unix"])
            m_state = -1

            teams = []
            for data in table.find_all("td", class_="team-cell"):
                temp = data.find("div", class_="line-align")
                teams.append(temp.find("div").text)

            m_teamname1 = teams[0]
            m_teamname2 = teams[1]

            t_resultscores = table.find("td", class_="result-score")

            scores = []
            for data in t_resultscores.find_all("span"):
                scores.append(int(data.text))

            m_teamscore1 = scores[0]
            m_teamscore2 = scores[1]

            m_map = table.find("div", class_="map-text").text

            if m_id in self.matches.keys():
                self.matches[m_id].state = -1
                self.matches[m_id].map = m_map
                self.matches[m_id].teamscore1 = m_teamscore1
                self.matches[m_id].teamscore2 = m_teamscore2
                self.matches[m_id].determine_winner()

            else:
                match = Match(id=m_id, unix_ts=m_unix_ts, state=m_state, map=m_map, teamname1=m_teamname1, teamname2=m_teamname2, teamscore1=m_teamscore1, teamscore2=m_teamscore2)

                match.determine_winner()

                self.matches[m_id] = match

            log.info(f"Finished match results (id: {m_id}, unix: {m_unix_ts})")


class Sheets:
    def __init__(self, credentials):
        self.credentials = credentials
        self.client = gspread.authorize(credentials)

        self.sskey = None
        self.wsheets = {}

    def get_spreadsheet(self, sskey):
        self.sskey = sskey
        self.sheet = self.client.open_by_key(self.sskey)

    def open_worksheet(self, index=None, ask=False):
        available_worksheets = self.sheet.worksheets()

        if ask:
            print("Worksheets available:")
            i = 0
            for ws in available_worksheets:
                print(f"{i}: {ws}")
                i += 1

            selection = int(input("> "))
        else:
            if index is None:
                selection = 0
            else:
                selection = index

        self.wsheets[selection] = self.sheet.get_worksheet(selection)

    def update_sheet(self, hltv_matches):
        sheet = self.wsheets[0]
        sheet_ids = sheet.col_values(1)

        boolSheetUpdated = False
        for index in range(len(hltv_matches)):
            match = hltv_matches[index]

            del match[2]

            if str(match[0]) in sheet_ids:
                row = sheet.find(str(match[0])).row
                row_values = sheet.row_values(row)

                if list(map(str, match)) == row_values:
                    log.info(f"Match data is not different: ({match})")
                    continue

                cell_list = sheet.range(row, 1, row, len(match)-1)
                i = 0
                for cell in cell_list:
                    cell.value = match[i]
                    i += 1

                sheet.update_cells(cell_list)
                log.info(f"Updating match (id: {match[0]}) with new data: ({match})")
                boolSheetUpdated = True
                continue

            sheet.append_row(hltv_matches[index])

        if boolSheetUpdated:
            log.debug("Sheet updated")

        log.info(f"Finished updating sheet (number of matches updated: {len(hltv_matches)})")


class Database:
    def __init__(self, eventid):
        pass

def db_get_matches(match_fetch, session):
    matches = session.query(Match).order_by(Match.id)

    for match in matches:
        match_fetch.matches[match.id] = match
        session.add(match)

def db_get_teams(match_fetch, session):
    teams = session.query(Team).order_by(Team.id)

    for team in teams:
        match_fetch.teams[team.id] = team
        session.add(team)


def main(args):
    g_credentials = get_credentials(args)
    ssmanager = Sheets(g_credentials)
    ssmanager.get_spreadsheet(args.sskey)
    ssmanager.open_worksheet()

    DBManager.init(args.eventid)
    db.create_tables(DBManager.engine)
    session = DBManager.create_session()
    log.info(session.identity_map.values())
    db_add = True

    match_fetch = Scraper(args.eventid, args.numdaysadvance)

    db_get_matches(match_fetch, session)
    db_get_teams(match_fetch, session)

    exit = False
    while not exit:
        match_fetch.update()

        match_fetch.get_teams()
        match_fetch.get_matches()
        match_fetch.get_results()

        db_add = False

        log.info(session.dirty)
        session.commit()

        #ssmanager.update_sheet(upcoming)

        time.sleep(120)

