## Aanalytics Engine

#### Project level summary that shows week on week movements around productivity metrics.
The metrics tracked are:

| metric name                         | description                                                                                                                                      |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| num_workflows_movt                  | change in the number of workflows for a project                                                                                                  |
| num_agents_movt                     | change in the number of agents for a project                                                                                                     |
| num_submissions_movt                | change in the number of submissions for a project                                                                                                |
| submission_rate_mins_movt           | change in the submission_rate (submissions per min) for a project.  calculated as: SUM(total_submission_count)/AVG(avg_submission_duration_mins) |
| avg_first_good_submission_rate_movt | change in the average of the first_good_submission percentage for a project                                                                      |
| avg_rejection_rate_movt             | change in the average rejection rate for a project.   calculated as: AVG(total_rejection_count/total_submission_count)                           |
| avg_quality_score_movt              | change in the average quality score for a project                                                                                                |
| avg_rework_time_movt                | change in the average rework time for a project                                                                                                  |

