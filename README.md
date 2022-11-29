# ADES WPST Communicator


## Overview

ADES WPST Communicator is a service that works as proxy between WPST client and Flex Server. It run as a daemon service and use SQS queues to communicate with client.


## wpst_client_daemon
wpst_client_daemon.py: It is a service that works as proxy between WPST client and Flex Server. It run as a daemon service and use SQS queues to communicate with client.
To configure the service, please see "Configuring SQSConfig" section.

To start the service : python wpst_client_daemon.py start
To stop the service : python wpst_client_daemon.py stop


## SQS Queues


## ADES_WPST_SQS.py


## Configuring SQSConfig

Copy SQSConfig.py.tmpl to SQSConfig.py
Update the following vatiables

- AWS_SQS_QUEUE
  If using aws credential profile to access aws sqs, update the following:
  - aws_credentials_file : The full path to the credential file, such as /home/.aws/credentials
  - aws_credentials_file_profile: The profile name in the credential file
  - region = region name, such as 'us-west-2'

  If not using credentials, update the following variables to communicate with aws:
  - aws_access_key = {{ AWS_ACCESS_KEY }}
  - aws_secret_key = {{ AWS_SECRET_KEY }}
  - aws_session_token = {{ AWS_SESSION_TOKEN }}

  If you are using auto refreshing of the aws access keys, update the following:
  - cred_url =
  - account_number =
  - iam_role_name =
  - api_key =

  For all cases, update the request and reply queue name. reply_queue_name is not required by wpst client daemon, but required by ADES_WPST_SQS
  - request_queue_name = {{ REQUEST_QUEUE_NAME }}
  - reply_queue_name = {{ REPLY_QUEUE_NAME }}


- DAEMON: Only needed for wpst_client_daemon, where you need to specify the paths for pid file, error and output log files:
  - PID_FILE_PATH={{ PID_FILE_PATH }}/daemon_pid.pid
  - DAEMON_OUTPUT_OVERWRITE=False
  - DAEMON_OUTPUT_FILE={{ DAEMON_OUTPUT_FILE_PATH }}/listener_out.log
  - DAEMON_ERROR_FILE={{ DAEMON_OUTPUT_FILE_PATH }}/listener_error.log
  - DAEMON_STDIN=/dev/null

- ADES_WPS-T_SERVER: Only needed for wpst_client_daemon
  - wps_server_url=http://127.0.0.1:5000
