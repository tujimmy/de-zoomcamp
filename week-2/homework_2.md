## Question 1 
447770
`13:22:14.946 | INFO    | Task run 'clean-0' - rows: 447770`

## Question 2
0 5 1 * *

`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 5 1 * *" -a`

## Question 3

`13:31:39.636 | INFO    | Task run 'write_bq-0 - writing rows: 7019375`  
`13:32:39.725 | INFO    | Task run 'write_bq-0' - writing rows: 7832545`  
Total: 14,851,920

Q4

`prefect deployment build week-2/etl_web_to_gcp_hmwk.py:etl_web_to_gcs --name week_2_git --tag main -sb github/github-block -a`  
rows: 88605

`rows: 88605
01:41:57 PM
clean-0`

Q5

`prefect deployment build week-2/etl_web_to_gcp_hmwk.py:etl_parent_flow --name week_2_git --tag main -sb github/github-block -a`  
`prefect deployment run etl-parent-flow/week_2_git -p "months=[4]" -p "year=2019" -p "color=green"`  
rows: 514392  
Prefect flow run notification
Flow run etl-web-to-gcs/flying-stork entered state Completed at 2023-02-05T19:10:27.975720+00:00.
Flow ID: 89de6be0-627e-4c51-b3a0-a1dc3870581b
Flow run ID: 2701e8fd-c50f-4795-8235-a343eb749a42
Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/2701e8fd-c50f-4795-8235-a343eb749a42
State message: All states completed.
Prefect Notifications | Today at 2:10 PM
white_check_mark
eyes
raised_hands


2:10
Prefect flow run notification
Flow run etl-parent-flow/inventive-anteater entered state Completed at 2023-02-05T19:10:28.016844+00:00.
Flow ID: 99dc6e32-6925-4241-8663-02d6f0a4d452
Flow run ID: 32cd1149-61a4-4263-9619-ae5ee0f1385a
Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/32cd1149-61a4-4263-9619-ae5ee0f1385a
State message: All states completed.


Q6

8
