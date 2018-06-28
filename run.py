import argparse
import logging

from oauth2client import tools

import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-e', "--eventid", type=int, required=True)
    parser.add_argument('-k', "--sskey", required=True)
    parser.add_argument('-n', "--numdaysadvance", type=int, default=1)
    parser.add_argument("-l", "--log", choices=['debug', 'info', 'warning', 'error', 'critical'], default="info", type=str, required=False, help="Set minimum logging level for messages to be logged to console")

    args = parser.parse_args()

    logging_levels = {
        "debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING,
        "error": logging.ERROR, "critical": logging.CRITICAL
    }

    logging.basicConfig(format='<%(asctime)s> :%(name)s:%(lineno)s: [%(levelname)s] %(message)s', level=logging_levels[args.log])
    log = logging.getLogger(__name__)

    file_log = logging.FileHandler("logs/temp/errors.log")
    file_log.setLevel(logging.WARNING)

    log.addHandler(file_log)

    try:
    	main.main(args)

    except KeyboardInterrupt as e:
    	log.info(f"Script stopped manually: {repr(e)}")

    # except Exception as e:
    # 	log.critical(f"Script failed to run: {repr(e)}")
