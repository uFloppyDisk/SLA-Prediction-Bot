import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.types import BLOB
from sqlalchemy.dialects.mysql import TEXT

from db import Base

log = logging.getLogger(__name__)

class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    unix_ts = Column(Integer)
    state = Column(Integer, default=1)
    teamname1 = Column(TEXT, nullable=False)
    teamname2 = Column(TEXT, nullable=False)
    teamscore1 = Column(Integer, default=0)
    teamscore2 = Column(Integer, default=0)
    map = Column(TEXT)
    winner = Column(TEXT)
    flags = Column(TEXT)

    def __init__(self, **options):
        self.id = None
        self.unix_ts = None
        self.state = 1
        self.teamname1 = None
        self.teamname2 = None
        self.teamscore1 = 0
        self.teamscore2 = 0
        self.map = None
        self.winner = None
        self.flags = None

        self.set(**options)

    def __repr__(self):
        return f"Match(id: {self.id}) [{self.teamname1} vs {self.teamname2}]"

    def set(self, **options):
        self.id = options.get('id', self.id)
        self.unix_ts = options.get('unix_ts', self.unix_ts)
        self.state = options.get('state', self.state)
        self.teamname1 = options.get('teamname1', self.teamname1)
        self.teamname2 = options.get('teamname2', self.teamname2)
        self.teamscore1 = options.get('teamscore1', self.teamscore1)
        self.teamscore2 = options.get('teamscore2', self.teamscore2)
        self.map = options.get('map', self.map)
        self.winner = options.get('winner', self.winner)
        self.flags = options.get('flags', self.flags)

    @classmethod
    def new(self, **options):
        instance = self(**options)
        return instance

    def determine_winner(self):
        if self.teamscore1 is not None and self.teamscore2 is not None:
            if self.teamscore1 > self.teamscore2:
                self.winner = self.teamname1
                log.info(f"Teamscore1 is greater than Teamscore2, winner is {self.winner}")
            elif self.teamscore2 > self.teamscore1:
                self.winner = self.teamname2
                log.info(f"Teamscore2 is greater than Teamscore1, winner is {self.winner}")

            return
        
        else:
            return

class Definition(Base):
    __tablename__ = "definitions"

    DEF_ID = Column(Integer, primary_key=True, autoincrement=True)
    DEF_TYPE = Column(TEXT)
    TEAM_ID = Column(Integer)
    DEF_HLTV = Column(TEXT, nullable=False)
    DEF_SHEET = Column(TEXT, nullable=False)

    def __init__(self, **options):
        self.DEF_ID = 0
        self.DEF_TYPE = None
        self.TEAM_ID = None
        self.DEF_HLTV = None
        self.DEF_SHEET = None

        self.set(**options)

    def __repr__(self):
        return f"Definition(type: {self.DEF_TYPE}) {self.DEF_HLTV} = {self.DEF_SHEET}"

    def set(self, **options):
        self.DEF_ID = options.get('DEF_ID', self.DEF_ID)
        self.DEF_TYPE = options.get('DEF_TYPE', self.DEF_TYPE)
        self.TEAM_ID = options.get('TEAM_ID', self.TEAM_ID)
        self.DEF_HLTV = options.get('DEF_HLTV', self.DEF_HLTV)
        self.DEF_SHEET = options.get('DEF_SHEET', self.DEF_SHEET)

class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    name = Column(TEXT, nullable=False)
    previous_aliases = Column(BLOB)

    def __init__(self, **options):
        self.id = None
        self.name = None
        self.previous_aliases = None

        self.set(**options)

    def __repr__(self):
        return f"Team(id: {self.id}) '{self.name}'"

    def set(self, **options):
        self.id = options.get('id', self.id)
        self.name = options.get('name', self.name)
        self.previous_aliases = options.get('previous_aliases', self.previous_aliases)