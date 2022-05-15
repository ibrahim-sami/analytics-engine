import os
from pathlib import Path
from datetime import date, timedelta

from utils import setup_logging, query_bigq
from utils_logic import get_top_projects
from utils_email import send_template_email

PROJECT = 'hub-data-295911'
DATASET = 'onhub_data'

P_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
QUERY_DIR = os.path.join(P_DIR, 'queries')
OUTPUT_DIR = os.path.join(P_DIR, 'outputs')

CCs = ['ibrahim.sami@samasource.org', 'george.kagwe@samasource.org']
IB_EMAIL = ['ibrahim.sami@samasource.org']

LOGGER = setup_logging(name=Path(__file__).stem, project=PROJECT)

def execute(event, context):
    LOGGER.debug('Executing . . .')

    '''
        Query bigQ for project, workflow and agent counts
    '''
    query_file = 'counts.sql'
    LOGGER.debug(f'Getting high level counts as per the query defined in {query_file}')
    with open(os.path.join(QUERY_DIR, query_file), 'r') as f:
        query_string = f.read()
        df_counts = query_bigq(
            project=PROJECT,
            query_string=query_string,
            query_params=None
        )
        num_projects = df_counts['num_projects'][0]
        num_workflows = df_counts['num_workflows'][0]
        num_agents = df_counts['num_agents'][0]
        LOGGER.debug(f"A total of {num_projects} projects with {num_workflows} workflows and {num_agents} agents found") 

    '''
        Query bigQ for project level summaries
    '''
    query_file = 'project_summaries.sql'
    LOGGER.debug(f'Getting project level summaries as per the query defined in {query_file}')
    with open(os.path.join(QUERY_DIR, query_file), 'r') as f:
        query_string = f.read()
        df = query_bigq(
                project=PROJECT,
                query_string=query_string,
                query_params=None
            )
        # replace zeros with nulls in numerical cols
        # numeric_columns = df.select_dtypes(include=['number']).columns
        # df[numeric_columns] = df[numeric_columns].fillna(0.0)

        LOGGER.debug('Calculating movement columns')
        df['num_workflows_movt'] = (df['num_workflows'] - df['num_workflows_prev']) / df['num_workflows_prev']
        df['num_agents_movt'] = (df['num_agents'] - df['num_agents_prev']) / df['num_agents_prev']
        df['num_submissions_movt'] = (df['num_submissions'] - df['num_submissions_prev']) / df['num_submissions_prev']
        df['sumbission_rate_mins_movt'] = (df['sumbission_rate_mins'] - df['sumbission_rate_mins_prev']) / df['sumbission_rate_mins_prev']
        df['avg_first_good_submission_rate_movt'] = (df['avg_first_good_submission_rate'] - df['avg_first_good_submission_rate_prev']) / df['avg_first_good_submission_rate_prev']
        df['avg_rejection_rate_movt'] = (df['avg_rejection_rate'] - df['avg_rejection_rate_prev']) / df['avg_rejection_rate_prev']
        df['avg_quality_score_movt'] = (df['avg_quality_score'] - df['avg_quality_score_prev']) / df['avg_quality_score_prev']
        df['avg_rework_time_movt'] = (df['avg_rework_time'] - df['avg_rework_time_prev']) / df['avg_rework_time_prev']

        # df.iloc[:,:2] # grabs all rows and first 2 columns
        # df.iloc[:,-8:] # grabs all rows and last 8 columns
        # df_movt = pd.concat([df.iloc[:,:2], df.iloc[:,-8:]], axis=1) # puts them together row wise
        
        numerical_cols = list(df.select_dtypes(include=['number']).columns)[1:]
        movt_cols = [col for col in numerical_cols if str(col).endswith('_movt')]
        df['is_missing'] = df[movt_cols].isna().all(axis=1)
        try:
            df.to_csv(os.path.join(OUTPUT_DIR, 'df_original.csv'))
        except Exception:
            ...
        df = df[~df['is_missing']] # all movts are null, no data at all for prev or current weeks
        try:
            df.to_csv(os.path.join(OUTPUT_DIR, 'df_no_nulls.csv'))
        except Exception:
            ...
        
        '''
        Get top project for every movt metric
        '''
        results = get_top_projects(df, movt_cols)
        start_current_week = date.today() - timedelta(days=7)
        end_current_week = start_current_week + timedelta(days=6)
        end_previous_week = start_current_week - timedelta(days=1)
        start_previous_week = end_previous_week - timedelta(days=6)
        output = send_template_email(
            template='mgmt_summary.html', # TODO format HTML table and its entries
            recipients=IB_EMAIL,
            CCs=[],
            subj=f"Project Level Summary as at {date.today().strftime('%d %b %Y')}",
            num_projects=num_projects,
            num_workflows=num_workflows,
            num_agents=num_agents,
            start_current_week=start_current_week.strftime('%d %b %Y'),
            end_current_week=end_current_week.strftime('%d %b %Y'),
            start_previous_week=start_previous_week.strftime('%d %b %Y'),
            end_previous_week=end_previous_week.strftime('%d %b %Y'),
            items=results  
        )
        LOGGER.debug(output)

        return output


if __name__ == "__main__":
    execute(None, None)