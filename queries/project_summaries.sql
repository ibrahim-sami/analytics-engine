WITH current_week_data AS (
  SELECT project_group_id, project_name, 
  COUNT(DISTINCT project_id) as num_workflows,
  COUNT(DISTINCT user_employee_code) AS num_agents,
  SUM(total_submission_count) AS num_submissions,
  SUM(total_submission_count)/AVG(avg_submission_duration_minutes) AS sumbission_rate_mins,
  AVG(first_good_submission_percentage) AS avg_first_good_submission_rate,
  AVG(total_rejection_count/total_submission_count) AS avg_rejection_rate,
  AVG(avg_score) AS avg_quality_score,
  AVG(avg_rework_submission_duration_minutes) AS avg_rework_time,
  FROM `hub-data-295911.onhub_data.agent_productivity`
  WHERE delivery_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1,2
),
previous_week_data AS (
  SELECT project_group_id, project_name, 
  COUNT(DISTINCT project_id) as num_workflows_prev,
  COUNT(DISTINCT user_employee_code) AS num_agents_prev,
  SUM(total_submission_count) AS num_submissions_prev,
  SUM(total_submission_count)/AVG(avg_submission_duration_minutes) AS sumbission_rate_mins_prev,
  AVG(first_good_submission_percentage) AS avg_first_good_submission_rate_prev,
  AVG(total_rejection_count/total_submission_count) AS avg_rejection_rate_prev,
  AVG(avg_score) AS avg_quality_score_prev,
  AVG(avg_rework_submission_duration_minutes) AS avg_rework_time_prev,
  FROM `hub-data-295911.onhub_data.agent_productivity`
  WHERE delivery_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
  GROUP BY 1,2
)
SELECT a.project_group_id, a.project_name,
a.num_workflows, b.num_workflows_prev,
a.num_agents, b.num_agents_prev,
a.num_submissions, b.num_submissions_prev,
a.sumbission_rate_mins, b.sumbission_rate_mins_prev,
a.avg_first_good_submission_rate, b.avg_first_good_submission_rate_prev,
a.avg_rejection_rate, b.avg_rejection_rate_prev,
a.avg_quality_score, b.avg_quality_score_prev,
a.avg_rework_time, b.avg_rework_time_prev
FROM current_week_data a
LEFT JOIN previous_week_data b
ON a.project_group_id = b.project_group_id
AND a.project_name = b.project_name