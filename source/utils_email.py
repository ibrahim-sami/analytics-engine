from io import BytesIO
import smtplib
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from jinja2 import Environment, FileSystemLoader, select_autoescape
import pandas as pd
import os

from utils import get_credentials

# Suppress pandas warning: A value is trying to be set on a copy of a slice from a DataFrame.
# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
# See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
pd.options.mode.chained_assignment = None

CREDS = get_credentials(filename='credentials.ini', config_name='BA_GMAIL_CONFIG')
GMAIL_USER = CREDS['email_address']
GMAIL_PWD = CREDS['password']

P_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
QUERY_DIR = os.path.join(P_DIR, 'queries')
OUTPUT_DIR = os.path.join(P_DIR, 'outputs')

def is_email_valid(email:str):
    # for validating an Email
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(re.fullmatch(regex, email)):
        return True
    else:
        return False


def send_template_email(template:str, recipients:list, CCs:list, subj:str, attachment_file_stream:tuple, **kwargs):
    env = Environment(
        loader=FileSystemLoader('templates'),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template(template)
    body = template.render(**kwargs)
    output = send_email(recipients, CCs, subj, body, attachment_file_stream)
    return output


def send_email(recipients:list, CCs:list, subj:str, body:str, attachment_file_stream:tuple): 
    msg = MIMEMultipart()
    msg['Subject'] = subj  
    msg['From'] = GMAIL_USER
    msg['To'] = ", ".join(recipients)
    msg['Cc'] = ", ".join(CCs)
    msg.add_header('Content-Type','text/html')
    msg.attach(MIMEText(body, 'html'))
    if attachment_file_stream:
        io = attachment_file_stream[0]
        filename = attachment_file_stream[1]
        msg.attach(MIMEApplication(io.getvalue(), Name=filename))

    # NOTE: dataframe for prt_update check exceed limit,
    # instead we use the gsheet links to the exceptions files
    # if dataframes:
    #     for name, df in dataframes.items():
    #         text_stream = StringIO()
    #         df.to_csv(text_stream, index=False)
    #         msg.attach(MIMEApplication(text_stream.getvalue(), Name=name + '.csv'))

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PWD)
        server.send_message(msg)
        server.close()
        return ("Email sent successfully!", None)
    except Exception as ex:
        return ("Something went wrong….", ex)

def convert_dfs_to_iostrean(dfs:dict, filename:str):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    for name, data in  dfs.items():
        data.to_excel(excel_writer=writer, 
                        sheet_name=str(name).removesuffix("_movt"),
                        index=False)

    writer.save()
    return (output, filename)