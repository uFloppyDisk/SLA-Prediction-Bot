import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

log = logging.getLogger(__name__)


def create_tables(engine):
	from models import Match, Definition, Team

	try:
		Base.metadata.create_all(engine)
	except Exception as e:
		log.exception(f"Exception caught in db.create_tables: {e}")
		sys.exit(1)


class DBManager:
	def init(eventid):
		DBManager.engine = create_engine(f"sqlite:///database/{eventid}.db")
		DBManager.Session = sessionmaker(bind=DBManager.engine, autoflush=False)

	def create_session(**options):
		try:
			return DBManager.Session(**options)

		except:
			log.exception("Unhandled exception in DBManager.create_session")

		return None

	def session_add_expunge(object, **options):
		if 'expire_on_commit' not in options:
		    options['expire_on_commit'] = False

		session = DBManager.create_session(**options)
		try:
		    session.add(object)
		    session.commit()
		    session.expunge(object)
		except:
		    session.rollback()
		    raise
		finally:
		    session.close()