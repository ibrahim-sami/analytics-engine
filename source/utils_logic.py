import os
from pathlib import Path
from pandas import DataFrame

from utils import setup_logging


PROJECT = 'hub-data-295911'
LOGGER = setup_logging(name=Path(__file__).stem, project=PROJECT)

def get_top_projects(df_movt:DataFrame, movt_cols:list):
    # TODO check logic, inf%? exclude inf, 100%, 0% et al
    results = []

    for col in movt_cols:
        sub_dict = {}
        # remove nulls
        sub_df = df_movt.copy()
        sub_df['is_missing'] = sub_df[[col]].isna().all(axis=1)
        sub_df = sub_df[~sub_df['is_missing']]
        # remove unwanted cols
        current_col = str(col).replace("_movt", "")
        prev_col = str(col).replace("_movt", "_prev")
        sub_df = sub_df[['project_group_id', 'project_name', col, current_col, prev_col]]

        # highest movt
        idx = sub_df[col].idxmax()
        sub_dict = dict(
            metric=col,
            project_group_id=sub_df['project_group_id'][idx],
            project_name=sub_df['project_name'][idx],
            current_week_value=sub_df[current_col][idx],
            previous_week_value=sub_df[prev_col][idx],
            movt="{:.0%}".format(sub_df[col][idx])
        )
        results.append(sub_dict)
        # lowest movt
        idx = sub_df[col].idxmin()
        sub_dict = dict(
            metric=col,
            project_group_id=sub_df['project_group_id'][idx],
            project_name=sub_df['project_name'][idx],
            current_week_value=sub_df[current_col][idx],
            previous_week_value=sub_df[prev_col][idx],
            movt="{:.0%}".format(sub_df[col][idx])
        )
        results.append(sub_dict)

    return results