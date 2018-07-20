import logging
import os

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


def get_creds():
    creds_file = "secrets/conf.json"
    # scopes = [
    #     "https://www.googleapis.com/auth/spreadsheets"
    # ]

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]

    return create_assertion_session(creds_file, scopes)


def create_assertion_session(conf_file, scopes, subject=None):
    import json
    from authlib.client import AssertionSession

    with open(conf_file, 'r') as f:
        conf = json.load(f)

    token_url = conf['token_uri']
    issuer = conf['client_email']
    key = conf['private_key']
    key_id = conf.get('private_key_id')

    header = {'alg': 'RS256'}
    if key_id:
        header['kid'] = key_id

    # Google puts scope in payload
    claims = {'scope': ' '.join(scopes)}
    return AssertionSession(
        grant_type=AssertionSession.JWT_BEARER_GRANT_TYPE,
        token_url=token_url,
        issuer=issuer,
        audience=token_url,
        claims=claims,
        subject=subject,
        key=key,
        header=header,
    )


def tryconvert(value, default=-1):
    if value is None:
        return default

    try:
        return(int(value))
    except ValueError:
        return(default)


def range_to_file(ws_range, filename="range.txt"):
    if os.path.isfile(filename):
        os.remove(filename)

    with open(filename, "a+") as file:
        if isinstance(ws_range, dict):
            for index in ws_range.keys():
                file.write(f"{index}: {repr(ws_range[index])}\r\n")
        elif isinstance(ws_range, list):
            for entry in ws_range:
                file.write(f"{repr(entry)}\r\n")

    return
