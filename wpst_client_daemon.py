#/usr/bin/env python
"""
a sample daemonization script for the sqs listener
"""
import os
import sys
import json
import boto3
import logging
import configparser
import requests
import argparse
from urllib.parse import urljoin
import time
import traceback
from sqs_client.subscriber import MessagePoller
from sqs_client.contracts import MessageHandler
from sqs_client.factories import SubscriberFactory, PublisherFactory
from sqs_client.daemon import Daemon
from constants import constants as const
from sqs_client.subscriber import MessagePoller
from sqs_client.contracts import MessageHandler
from sqs_client.factories import SubscriberFactory, PublisherFactory


logger = logging.getLogger('sqs_listener')
logger.setLevel(logging.INFO)

sh = logging.FileHandler('wpst_sqs_client.log')
sh.setLevel(logging.INFO)

formatstr = '[%(asctime)s - %(name)s - %(levelname)s]  %(message)s'
formatter = logging.Formatter(formatstr)

sh.setFormatter(formatter)
logger.addHandler(sh)


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


class TestHandler(MessageHandler):
    def submit_request(self, href, request_type, expected_response_code=200, payload_data=None, timeout=None):

        logger.debug("submit_request : href : {} request_type : {}".format(href, request_type))
        headers = {'Content-type': 'application/json'}
        wps_server_url = urljoin(wps_server, href)
        logger.info("wps_server_url : {}".format(wps_server_url))
        if request_type.upper()=="GET":
            logger.debug("calling GET")
            if timeout:
                response = requests.get(wps_server_url, headers=headers, timeout=timeout)
            else:
                response = requests.get(wps_server_url, headers=headers)
            #logger.debug(response.json())
        elif request_type.upper()=="POST":
            if payload_data:
                logger.info("POST DATA : {}".format(wps_server_url))
                headers = {'content-type': 'application/x-www-form-urlencoded'}
                if timeout:
                    response = requests.post(wps_server_url, headers=headers, data={"proc" : payload_data}, timeout=timeout)
                else:
                    response = requests.post(wps_server_url, headers=headers, data={"proc" : payload_data})
            else:
                logger.info("POST: NO PAYLOAD_DATA")
                response = requests.post(wps_server_url, headers=headers)
        elif request_type.upper()=="DELETE":
            response = requests.delete(wps_server_url, headers=headers)
        else:
            raise Exception("Invalid Request Type : {}".format(request_type))
         
        response.raise_for_status()
        logger.info("status code: {}".format(response.status_code))
        logger.info(response)
        # logger.info(json.dumps(response.json(), indent=2))

       
        assert response.status_code == int(expected_response_code)
        print(response.json())
        return json.dumps(response.json())

    def getLandingPage(self):
        logger.debug("getLandingPage")
        href = ""	
        request_type = "GET"
        return self.submit_request(href, request_type)

    def deployProcess(self, payload_data):
        href = "processes"
        request_type = "POST"
        return self.submit_request(href, request_type, 201, payload_data)

    def getProcessDescription(self, process_id):
        href = "processes/{}".format(process_id)
        request_type = "GET"
        return self.submit_request(href, request_type)

    def undeployProcess(self, process_id):
        href = "processes/{}".format(process_id)
        request_type = "DELETE"
        return self.submit_request(href, request_type)

    def getJobList(self, process_id):
        href = "processes/{}/jobs".format(process_id)
        request_type = "GET"
        return self.submit_request(href, request_type)        
 
    def execute(self, process_id, payload_data):
        href = "processes/{}/jobs".format(process_id)
        request_type = "POST"
        wps_server_url = urljoin(wps_server, href)
        headers = {'Content-type': 'application/json'}
        response = requests.post(wps_server_url, headers=headers, data=json.dumps(payload_data))
        response.raise_for_status()
        logger.info("status code: {}".format(response.status_code))
        logger.info(json.dumps(response.json(), indent=2))
        assert response.status_code == 201
        return json.dumps(response.json())

    def getStatus(self, process_id, job_id):
        href = "processes/{}/jobs/{}".format(process_id, job_id)
        request_type = "GET"
        return self.submit_request(href, request_type)

    def dismissJob(self, process_id, job_id):
        href = "processes/{}/jobs/{}".format(process_id, job_id)
        request_type = "DELETE"
        return self.submit_request(href, request_type)

    def getProcesses(self):
        href = "processes"
        request_type = "GET"
        return self.submit_request(href, request_type)

    def getResult(self, process_id, job_id):
        href = "processes/{}/jobs/{}/result".format(process_id, job_id) 
        request_type = "GET"
        return self.submit_request(href, request_type)


    def process_message(self, message):
        try:
            logger.info("Received : {}".format(message))

            message_body = json.loads(message.body)
            job_type = str(message_body["job_type"]).strip()
            
            logger.info("RECEIVED message : {}".format(message_body))
            logger.info(message.attributes)
            print(message.attributes)
            logger.info("Received message of type : {}".format(job_type))
            if job_type == const.GET_LANDING_PAGE:
                logger.info("Calling getLandingPage")
                return self.getLandingPage()
            elif job_type == const.GET_PROCESSES:
                return self.getProcesses()
            elif job_type == const.DEPLOY_PROCESS:
                return self.deployProcess(message_body['payload_data'])
            elif job_type == const.GET_PROCESS_DESCRIPTION:
                return self.getProcessDescription(message_body['process_id'])
            elif job_type == const.UNDEPLOY_PROCESS:
                return self.undeployProcess(message_body['process_id'])
            elif job_type == const.GET_JOB_LIST:
                return self.getJobList(message_body['process_id'])
            elif job_type == const.EXECUTE:
                return self.execute(message_body['process_id'], message_body['payload_data'])
            elif job_type == const.GET_STATUS:
                return self.getStatus(message_body['process_id'], message_body['job_id'])
            elif job_type == const.DISMISS:
                return self.dismissJob(message_body['process_id'], message_body['job_id'])
            elif job_type == const.GET_RESULT:
                return self.getResult(message_body['process_id'], message_body['job_id'])
            
            else:
                return "sorry!! {} is not a supported process".format(job_type)
        except Exception as e:
            logger.error("#" * 20)
            logger.error(str(e))
            logger.error(traceback.format_exc())
            return 'ERROR : {}'.format(str(e))
      

class MyDaemon(Daemon):
    def run(self, config, publisher, subscriber):
        logger.info("Initializing listener")
        poll = MessagePoller(
            handler=TestHandler(),
            subscriber=subscriber,
            publisher=publisher
        )
        poll.start()

        logger.info("listener started")


if __name__ == "__main__":

    config_file = r'sqsconfig.py'

    parser = MyParser()
    parser.add_argument('mode',  type=str)
    parser.add_argument('--verbose', '-v', help="increase output verbosity", required=False,
                    action="store_true")
    parser.add_argument('--config', '-c', default=config_file, required=False,
                        help="config file name with full path")
    args = parser.parse_args()

    if args.verbose:
        logger.debug("Logging level is Debug")
        logger.setLevel(logging.DEBUG)

    logger.debug("args.mode : {} verbose : {} config:{}".format(args.mode, args.verbose, args.config))

    if args.config:
        config_file = args.config

    config = configparser.ConfigParser()
    config.read(config_file)
    logger.info(config.sections())

    default_credential_file = os.path.join(os.path.expanduser('~'), ".aws/credentials")
    default_profile = "maap-hec"
    aws_credentials_file =config["AWS_SQS_QUEUE"].get("aws_credentials_file", default_credential_file)
    aws_credentials_file_profile = config["AWS_SQS_QUEUE"].get('aws_credentials_file_profile', default_profile)
    config2 = configparser.RawConfigParser()
    config2.read(aws_credentials_file)
    AWS_ACCESS_KEY_ID = config2.get(aws_credentials_file_profile, 'aws_access_key_id')
    AWS_SECRET_ACCESS_KEY = config2.get(aws_credentials_file_profile, 'aws_secret_access_key')
    AWS_SESSION_TOKEN = None
    try:
        AWS_SESSION_TOKEN = None #config2.get(aws_credentials_file_profile, 'aws_session_token')
    except:
        pass

    region_name=config["AWS_SQS_QUEUE"]['region_name']
    request_queue_name = config["AWS_SQS_QUEUE"]['request_queue_name']
    # queue_url=config["AWS_SQS_QUEUE"]['queue_url']

    os.environ["AWS_ACCOUNT_ID"] = config["AWS_SQS_QUEUE"]["AWS_ACCOUNT_ID"]
    os.environ["AWS_ACCESS_KEY"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    if AWS_SESSION_TOKEN:
        os.environ["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
    logger.info(os.environ["AWS_ACCOUNT_ID"])
    wps_server = config["ADES_WPS-T_SERVER"]["wps_server_url"]


    if aws_credentials_file_profile:
        boto_session = boto3.Session(profile_name=aws_credentials_file_profile)
        sqs_client = boto_session.client('sqs', region_name=region_name)
    else:
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=region_name
        )

    queue_url = sqs_client.get_queue_url(QueueName=request_queue_name)['QueueUrl']

    subscriber = SubscriberFactory(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        session_token = AWS_SESSION_TOKEN,
        region_name=region_name,
        credentials_file_profile=aws_credentials_file_profile,
        queue_url=queue_url
    ).build()

    publisher = PublisherFactory(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        session_token = AWS_SESSION_TOKEN,
        region_name=region_name,
        credentials_file_profile=aws_credentials_file_profile
    ).build()


    sqs_config={}
    for key in config["AWS_SQS_QUEUE"]:
        sqs_config[key] = config["AWS_SQS_QUEUE"][key]
    if config["AWS_SQS_QUEUE"][key].isnumeric():
        sqs_config[key] = int(config["AWS_SQS_QUEUE"][key])
    
    daemon_config = config['DAEMON']
    pid_path = daemon_config.get('PID_FILE_PATH')
    output_log = daemon_config.get('DAEMON_OUTPUT_FILE')
    error_log = daemon_config.get('DAEMON_ERROR_FILE')
    std_in = daemon_config.get('DAEMON_STDIN', '/dev/null')
    overwrite = daemon_config.get('DAEMON_OUTPUT_OVERWRITE', False)  
  
    daemon = MyDaemon(pidfile=pid_path, overwrite=overwrite, stdout=output_log, stderr=error_log, sqs_config=sqs_config, publisher=publisher, subscriber=subscriber)
    
    if 'start' == args.mode.lower():
        logger.info("Starting listener daemon")
        daemon.start()
    elif 'stop' == args.mode.lower():
       logger.info("Attempting to stop the daemon")
       daemon.stop()
    elif 'restart' == args.mode.lower():
        daemon.restart()
    else:
        logger.info("usage: %s start|stop|restart --verbose -c <config_file>" % sys.argv[0])
        sys.exit(2)
