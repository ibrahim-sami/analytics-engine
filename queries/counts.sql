SELECT COUNT(DISTINCT project_group_id) as num_projects,
COUNT(DISTINCT project_id) AS num_workflows,
COUNT(DISTINCT user_employee_code) AS num_agents 
FROM `hub-data-295911.onhub_data.agent_productivity`