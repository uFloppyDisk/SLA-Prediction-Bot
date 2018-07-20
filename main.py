import datetime
import httplib2
import json
import logging
import time
import re

from bs4 import BeautifulSoup
import gspread
from lxml import html
import requests
import sqlite3 as sql
import time

import db
from db import DBManager
from models import Match, Team, Definition

from utils import get_credentials, tryconvert, range_to_file, get_creds

log = logging.getLogger(__name__)

class Scraper:
    def __init__(self, eventid, num_daysadvance=1):
        self.db = None

        self.teams = {}
        self.matches = {}
        self.definitions = {}

        self.regex_matchid = re.compile('matches\/([0-9]{2,10})')
        self.regex_teamid = re.compile('team\/([0-9]{2,10})')

        self.eventid = eventid
        self.num_daysadvance = num_daysadvance

    def update(self, session):
        self.session = session

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

            dictTeam = {}
            dictTeam["id"] = int(re.search(self.regex_teamid, anchor["href"]).group(1))
            dictTeam["name"] = anchor.text

            if dictTeam["id"] in self.teams.keys():
                self.teams[dictTeam["id"]].set(**dictTeam)
            else:
                team = Team(**dictTeam)
                self.teams[dictTeam["id"]] = team

                self.session.add(team)

            if dictTeam["id"] in self.definitions.keys():
                self.definitions[dictTeam["id"]].set(TEAM_ID=dictTeam["id"], DEF_HLTV=dictTeam["name"])
            else:
                definiton = Definition(DEF_TYPE="team", TEAM_ID=dictTeam["id"], DEF_HLTV=dictTeam["name"], DEF_SHEET=dictTeam["name"])
                self.definitions[dictTeam["id"]] = definiton

                self.session.add(definiton)


    def get_upcoming_matches(self):
        matchdays = self.upcoming_soup.find_all("div", {"data-zonedgrouping-headline-classes": "standard-headline"}, limit=self.num_daysadvance)

        if matchdays is None:
            return []

        for s in matchdays[0].find_all("div", {"class": "match-day"}):
            match_day = s.find("span", {"class": "standard-headline"})
            match_day = match_day.text

            for matchdata in s.find_all("a"):
                dictMatch = {}
                dictMatch["id"] = int(re.search(self.regex_matchid, matchdata["href"]).group(1))
                dictMatch["unix_ts"] = int(matchdata["data-zonedgrouping-entry-unix"])
                dictMatch["state"] = 1

                dictMatch["map"] = '?'

                table = matchdata.find("tr")
                teams = []
                for tag in table.find_all("div", class_="team map-text".split()):
                    if tag["class"][0] == "team":
                        teams.append(tag.text)
                    elif tag["class"][0] == "map-text":
                        dictMatch["map"] = tag.text

                dictMatch["teamname1"] = teams[0]
                dictMatch["teamname2"] = teams[1]

                if dictMatch["id"] in self.matches.keys():
                    self.matches[dictMatch['id']].set(**dictMatch)
                    self.matches[dictMatch['id']].ms_unix_to_unix()
                else:
                    match = Match(**dictMatch)
                    match.ms_unix_to_unix()
                    self.matches[dictMatch['id']] = match

                    self.session.add(match)

                log.debug(f"Finished upcoming match (id: {dictMatch['id']})")

            log.debug(f"Finished upcoming matchday (date: {match_day})")

    def get_ongoing_matches(self):
        live_matches = self.upcoming_soup.find("div", {"class": "live-matches"})

        if live_matches is None:
            return []

        for live_match in live_matches.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['live-match']):
            matchdata = live_match.find("a")

            table = matchdata.find("table")

            dictMatch = {}
            dictMatch["id"] = int(table["data-livescore-match"])
            dictMatch["state"] = 0

            dictMatch["map"] = '?'

            bestof = table.find("td", class_="bestof").text
            multiple_maps = False
            if "1" in bestof:
                dictMatch["map"] = table.find("td", class_="map").text.lower()
            else:
                multiple_maps = True
                map_count = table.find("td", class_="map")
                dictMatch["map"] = "bo" + len(map_count)

            teams = []
            for tag in table.find_all("span", class_="team-name"):
                teams.append(tag.text)

            dictMatch["teamname1"] = teams[0]
            dictMatch["teamname2"] = teams[1]

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

            dictMatch["teamscore1"] = teamscores[0]
            dictMatch["teamscore2"] = teamscores[1]

            if dictMatch["id"] in self.matches.keys():
                self.matches[dictMatch['id']].set(**dictMatch)
                self.matches[dictMatch['id']].ms_unix_to_unix()

            else:
                dictMatch["unix_ts"] = int(time.time())

                match = Match(**dictMatch)
                match.ms_unix_to_unix()

                self.matches[dictMatch['id']] = match

                self.session.add(match)

                log.debug(f"Finished ongoing match (id: {dictMatch['id']}")

    def get_results(self):
        results = self.results_soup.find_all("div", {"class": "result-con"})

        if results is None:
            return []

        for result in results:
            table = result.find("a")

            dictMatch = {}
            dictMatch["id"] = int(re.search(self.regex_matchid, table["href"]).group(1))

            td = table.find("td", class_="date-cell")
            span = td.find("span")
            dictMatch["unix_ts"] = int(span["data-unix"])
            dictMatch["state"] = -1

            teams = []
            for data in table.find_all("td", class_="team-cell"):
                temp = data.find("div", class_="line-align")
                teams.append(temp.find("div").text)

            dictMatch["teamname1"] = teams[0]
            dictMatch["teamname2"] = teams[1]

            t_resultscores = table.find("td", class_="result-score")

            scores = []
            for data in t_resultscores.find_all("span"):
                scores.append(int(data.text))

            dictMatch["teamscore1"] = scores[0]
            dictMatch["teamscore2"] = scores[1]

            dictMatch["map"] = table.find("div", class_="map-text").text

            if dictMatch["id"] in self.matches.keys():
                self.matches[dictMatch['id']].set(**dictMatch)
                self.matches[dictMatch['id']].ms_unix_to_unix()
                self.matches[dictMatch['id']].determine_winner()

            else:
                match = Match(**dictMatch)
                match.ms_unix_to_unix()
                match.determine_winner()

                self.matches[dictMatch['id']] = match

                self.session.add(match)

            log.debug(f"Finished match results (id: {dictMatch['id']}, unix: {dictMatch['unix_ts']})")


class Sheets:
    def __init__(self, credentials=None, authlib_session=None):
        self.credentials = credentials
        self.client = gspread.authorize(self.credentials)
        #self.client = Client(None, authlib_session)

        self.sheet = None

        self.sskey = None
        self.wsheets = {}

        self.wsheet_range = {}

        self.database = None

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

    def get_worksheet_range(self, index=None):
        if not index:
            index = 0

        # column_keys = {
        #      1: "id",          2: "date",
        #      3: "teamname1",   4: "versus",
        #      5: "teamname2",   6: "map",
        #      7: "teamscore1",  8: "score-divider",
        #      9: "teamscore2", 10: "selection",
        #     11: "certainty",  12: "flags",
        #     13: "winner"
        # }

        _range = self.wsheets[index].range(2, 1, 400, 13)

        temp = {}

        for cell in _range:
            if cell.row not in temp.keys():
                temp[cell.row] = []
                temp[cell.row].append(cell)
            else:
                if cell.value == '':
                    cell.value = None

                temp[cell.row].append(cell)

        entries_to_delete = []
        for _entry in temp.keys():
            entry = temp[_entry]

            if entry[0].value is None or entry[0].value == '':
                entries_to_delete.append(_entry)
                continue

        for _entry in entries_to_delete:
            del temp[_entry]

        self.wsheet_range[index] = temp

        return

    def append_matches(self, hltv_matches):
        sheet = self.wsheets[0]
        rows_taken = self.wsheet_range[0].keys()
        sheet_ids = [(lambda v: tryconvert(v))(self.wsheet_range[0][entry][0].value) for entry in rows_taken]
        index = max(rows_taken) + 1

        for match_key in hltv_matches.keys():
            match = hltv_matches[match_key]

            if match.id not in sheet_ids:
                definitions = {
                    "map": self.database.get_map_definition(match.map),
                    "teamname1": self.database.get_team_definition(match.teamname1),
                    "teamname2": self.database.get_team_definition(match.teamname2),
                    "winner": self.database.get_team_definition(match.winner)
                }

                cells = sheet.range(index, 1, index, 13)

                cells[0].value = match.id
                cells[1].value = datetime.datetime.fromtimestamp(match.unix_ts).strftime("%m/%d/%Y")
                cells[2].value = definitions["teamname1"]
                cells[3].value = "vs"
                cells[4].value = definitions["teamname2"]
                cells[5].value = definitions["map"]
                cells[6].value = match.teamscore1
                cells[7].value = "-"
                cells[8].value = match.teamscore2
                cells[11].value = match.flags
                cells[12].value = definitions["winner"]

                sheet.update_cells(cells, value_input_option='USER_ENTERED')

                log.info(f"Appended match to spreadsheet (id: {match.id} '{match.teamname1} vs {match.teamname2}')")

                index += 1

    def update_matches(self, hltv_matches):
        index_keys = {
            0: "id",
            1: "unix_ts",        # special
            2: "teamname1",      # special
            3: "versus",         # do not touch
            4: "teamname2",      # special
            5: "map",            # special
            6: "teamscore1",
            7: "score-divider",  # do not touch
            8: "teamscore2",
            9: "selection",      # do not touch
            10: "certainty",     # do not touch
            11: "flags",         # do not touch
            12: "winner"         # special
        }

        do_not_touch = [3, 7, 9, 10, 11]
        special = [1, 2, 4, 5, 12]

        sheet = self.wsheets[0]

        cells = []
        cells_to_update = []
        for _entry in self.wsheet_range[0].keys():
            entry = self.wsheet_range[0][_entry]

            if entry[0].value is None or entry[0].value == '':
                continue

            if int(entry[0].value) in hltv_matches.keys():
                match = hltv_matches[int(entry[0].value)]

                definitions = {
                    "unix_ts": match.date(),
                    "map": self.database.get_map_definition(match.map),
                    "teamname1": self.database.get_team_definition(match.teamname1),
                    "teamname2": self.database.get_team_definition(match.teamname2),
                    "winner": self.database.get_team_definition(match.winner)
                }

                rowChanged = False
                i = 1
                for cell in entry[1:]:
                    if i in do_not_touch:
                        pass
                    else:
                        db_value = getattr(match, index_keys[i])

                        if i in special:
                            db_value = definitions[index_keys[i]]

                        sheet_value = tryconvert(cell.value, default=cell.value)

                        if sheet_value == db_value:
                            pass
                        elif db_value is None:
                            pass
                        else:
                            if not rowChanged:
                                rowChanged = True

                            cell.value = db_value
                            cells_to_update.append(cell)

                    cells.append(cell)

                    i += 1

            else:
                for cell in entry[1:]:
                    cells.append(cell)

        if len(cells_to_update) > 0:
            sheet.update_cells(cells, value_input_option='USER_ENTERED')
            range_to_file(cells_to_update, filename="update.txt")

            log.info(f"Updated {len(cells_to_update)} cells.")


class Database:
    def __init__(self):
        self.session = None

    def update_session(self, session):
        self.session = session

    def get_matches(self, _dict):
        matches = self.session.query(Match).order_by(Match.id)

        for match in matches:
            if match.id == 0:
                continue

            _dict.matches[match.id] = match
            self.session.add(match)

    def get_teams(self, _dict):
        teams = self.session.query(Team).order_by(Team.id)

        for team in teams:
            if team.id == 0:
                continue

            _dict.teams[team.id] = team
            self.session.add(team)

    def get_definitions(self, _dict):
        defs = self.session.query(Definition).filter(Definition.DEF_TYPE == "team").order_by(Definition.TEAM_ID)

        for _def in defs:
            _dict.definitions[_def.TEAM_ID] = _def
            self.session.add(_def)

    def get_team_definition(self, def_hltv):
        _def = self.session.query(Definition).filter(Definition.DEF_TYPE == "team").filter(Definition.DEF_HLTV == def_hltv).first()

        if _def is None:
            return def_hltv
        else:
            return _def.DEF_SHEET

    def get_map_definition(self, def_hltv):
        _def = self.session.query(Definition).filter(Definition.DEF_TYPE == "map").filter(Definition.DEF_HLTV == def_hltv).first()

        if _def is None:
            return def_hltv
        else:
            return _def.DEF_SHEET


def main(args):
    g_credentials = get_credentials(args)
    ssmanager = Sheets(credentials=g_credentials)

    ssmanager.get_spreadsheet(args.sskey)
    ssmanager.open_worksheet()

    DBManager.init(args.eventid)
    db.create_tables(DBManager.engine)
    session = DBManager.create_session()

    database = Database()
    database.update_session(session)

    match_fetch = Scraper(args.eventid, args.numdaysadvance)

    database.get_matches(match_fetch)
    database.get_teams(match_fetch)
    database.get_definitions(match_fetch)

    exit = False
    while not exit:
        try:
            database.update_session(session)
            ssmanager.database = database
            match_fetch.update(session)

            match_fetch.get_teams()
            match_fetch.get_results()
            match_fetch.get_matches()

            session.flush()
            session.commit()

            ssmanager.get_worksheet_range()
            range_to_file(ssmanager.wsheet_range[0])

            ssmanager.update_matches(match_fetch.matches)
            ssmanager.append_matches(match_fetch.matches)

        except gspread.exceptions.APIError as err:
            err_json = json.loads(err.response.text)

            if err_json["error"]["code"] == 401:
                log.warning("OAuth 2.0 access token has expired. Generating a new one.")

                ssmanager.client.login()
            else:
                err_print = json.dumps(err_json, sort_keys=True, indent=4)
                log.critical(f"Unhandled APIError:\n{err_print}")

        finally:
            log.info("Finished update. Waiting 120 seconds...")

        time.sleep(120)
