import os
import logging
import time
import random
from configparser import ConfigParser
from google.cloud import bigquery
from google.cloud import logging as cloudlogging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pygsheets
import jinja2


def setup_logging(name, project):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    lg_client = cloudlogging.Client(project=project)
    lg_handler = lg_client.get_default_handler()
    # lg_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # lg_handler.setFortmatter(lg_format)

    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)

    logger.addHandler(c_handler)
    logger.addHandler(lg_handler)

    return logger


def query_bigq(project, query_string, query_params=None):
    dataframe = None

    # bind query params
    if query_params:
        template = jinja2.Template(source=query_string)
        query_string = template.render(query_params)

    bqclient = bigquery.Client(project=project)
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return dataframe


def push_to_bigq(df, project, dataset, table, write_disposition=None, schema=None):
    if not write_disposition:
        raise Exception('Explicitly provide the write disposition')

    if df.empty:
        return "Empty DF"

    # credentials from the service_account.json are not used
    # authentication is done automatically through the Google Cloud SDK
    client = bigquery.Client(project=project)

    table_id = project + '.' + dataset + '.' + table
    if schema:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, 
            autodetect=False,
            schema=schema)
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, 
            autodetect=True)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    errors = job.result().error_result
    return errors


def authenticate_googleapis(path_to_cred_folder, client_secret_filename):
    service = None

    if not os.path.exists(path_to_cred_folder):
        raise Exception(f'Credentials folder path: {path_to_cred_folder} does not exist')

    path_to_client_secret_file = os.path.join(path_to_cred_folder, client_secret_filename)
    if not os.path.exists(path_to_client_secret_file):
        raise Exception(f'Google client secret file: {client_secret_filename} does not exist')

    if not os.path.exists(os.path.join(path_to_cred_folder, 'sheets.googleapis.com-python.json')):
        gs_client = pygsheets.authorize(
            client_secret=os.path.join(path_to_cred_folder, 'client_secret.json'),
            credentials_directory=path_to_cred_folder)

        creds = Credentials.from_authorized_user_file(
            os.path.join(path_to_cred_folder,'sheets.googleapis.com-python.json'))
    else:
        creds = Credentials.from_authorized_user_file(
            os.path.join(path_to_cred_folder,'sheets.googleapis.com-python.json'))
    
    service = build('sheets', 'v4', credentials=creds)
    return service


def get_spreadsheet_values(gsheet_url, sheet_ranges):
    values = None
    retry_count = 3

    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    credentials_path = os.path.join(parent_dir, 'credentials')
    client_secret_file = 'client_secret.json'
    service = authenticate_googleapis(
        path_to_cred_folder=credentials_path,
        client_secret_filename=client_secret_file
    )
    if not service:
        raise Exception('Error building googleapis service')

    gsheet_id = gsheet_url.split("/")[5]
    for n in range(0,retry_count): # retry 10 times with exponential backoff
        try:
            # new method: gets only specific sheet. uses sheets api v4
            request = (
                service.spreadsheets().\
                    values().\
                        batchGet(spreadsheetId=gsheet_id, ranges=sheet_ranges)
            )
            response = request.execute()
            values = response['valueRanges'][0]['values']
            break
        except HttpError as ex:
            # apply exponential backoff.
            time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
    return values


def get_credentials(filename, config_name):
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    credentials_path = os.path.join(parent_dir, 'credentials')
    credentials_file = os.path.join(credentials_path, filename)

    parser = ConfigParser()
    credentials = {}

    # get secret variable reference in deployed cloud function
    secret = os.environ.get(config_name)
    if secret:
        parser.read_string(secret)
        if parser.has_section(config_name):
            params = parser.items(config_name)
            for param in params:
                credentials[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the secret environment variable'.format(config_name))
    else:
        # read config file for local execution
        parser.read(credentials_file)
        if parser.has_section(config_name):
            params = parser.items(config_name)
            for param in params:
                credentials[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(config_name, filename))
 
    return credentials

