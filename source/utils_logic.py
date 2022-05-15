import os
from pathlib import Path
from pandas import DataFrame

from utils import setup_logging


PROJECT = 'hub-data-295911'
LOGGER = setup_logging(name=Path(__file__).stem, project=PROJECT)

METRIC_NAMES = {
    'num_workflows_movt':'Number of workflows',
    'num_agents_movt':'Number of Agents',
    'num_submissions_movt':'Number of Submissions',
    'sumbission_rate_mins_movt':'Submission rate per minute',
    'avg_first_good_submission_rate_movt':'Rate of first good submision %',
    'avg_rejection_rate_movt':'Rejection rate %',
    'avg_quality_score_movt':'Average quality score',
    'avg_rework_time_movt':'Average rework time'
}

def get_top_projects(df_movt:DataFrame, movt_cols:list):
    # TODO check logic, inf%? exclude inf, 100%, 0% et
    results = []
    dfs_to_return = {}

    for col in movt_cols:
        sub_dict = {}
        sub_df = df_movt.copy()
        # remove unwanted cols
        current_col = str(col).replace("_movt", "")
        prev_col = str(col).replace("_movt", "_prev")
        sub_df = sub_df[['project_group_id', 'project_group_name', current_col, prev_col, col]]

        # remove nulls
        sub_df['is_missing'] = sub_df[[col]].isna().all(axis=1)
        sub_df_complete = sub_df.copy()
        sub_df_complete.drop(['is_missing'], inplace=True, axis=1)
        dfs_to_return[METRIC_NAMES[col]] = sub_df_complete
        sub_df = sub_df[~sub_df['is_missing']]

        # highest movt
        idx = sub_df[col].idxmax()
        sub_dict = dict(
            metric=METRIC_NAMES[col],
            data_max=dict(
                project_group_id=sub_df['project_group_id'][idx],
                project_group_name=sub_df['project_group_name'][idx],
                current_week_value=sub_df[current_col][idx],
                previous_week_value=sub_df[prev_col][idx],
                movt=sub_df[col][idx]
            )
        )

        # lowest movt
        idx = sub_df[col].idxmin()
        sub_dict['data_min'] = dict(
                project_group_id=sub_df['project_group_id'][idx],
                project_group_name=sub_df['project_group_name'][idx],
                current_week_value=sub_df[current_col][idx],
                previous_week_value=sub_df[prev_col][idx],
                movt=sub_df[col][idx]
            )
        results.append(sub_dict)

    return results, dfs_to_return