import logging 
import typer
import logging
import configparser
import json
import os
import urllib
import uuid
import requests
import backoff
import botocore
import boto3

from traceback import print_exc

from sqs_client.factories import ReplyQueueFactory, PublisherFactory
from sqs_client.message import RequestMessage
from sqs_client.exceptions import ReplyTimeout
from constants import constants as const

logger = logging.getLogger('soamc_submitter')
logger.setLevel(logging.INFO)


CONFIG_FILE_PATH = r'sqsconfig.py'

config = configparser.ConfigParser()
config.read(CONFIG_FILE_PATH)

request_queue_name = config["AWS_SQS_QUEUE"].get('request_queue_name', None)
reply_queue_name = config["AWS_SQS_QUEUE"].get('reply_queue_name', 'reply_queue_{}'.format(request_queue_name))
api_key = config["AWS_SQS_QUEUE"].get("api_key")
cred_url = config["AWS_SQS_QUEUE"].get("cred_url")
account_number = config["AWS_SQS_QUEUE"].get("account_number")
iam_role_name = config["AWS_SQS_QUEUE"].get("iam_role_name")

os.environ["AWS_ACCOUNT_ID"] = config["AWS_SQS_QUEUE"]["AWS_ACCOUNT_ID"]
os.environ["AWS_ACCESS_KEY"] = config["AWS_SQS_QUEUE"]["aws_access_key"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SQS_QUEUE"]["aws_secret_key"]
os.environ["AWS_SESSION_TOKEN"] = config["AWS_SQS_QUEUE"]["aws_session_token"]

wps_server = config["ADES_WPS-T_SERVER"]["wps_server_url"]
default_queue_url = config["AWS_SQS_QUEUE"].get('queue_url', None)
reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("reply_timeout_sec", 20))
execute_reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("execute_reply_timeout_sec", 600))
deploy_process_timeout_sec = int(config["AWS_SQS_QUEUE"].get("deploy_process_timeout_sec", 900))

reply_queue = ReplyQueueFactory(
    name=reply_queue_name,
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()


publisher = PublisherFactory(
    access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
    secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
    session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
    region_name=config["AWS_SQS_QUEUE"]['region_name']
).build()

cred_url = "https://login.mcp.nasa.gov/api/v3/temporary-credentials"

reply_queue_dict = {}
reply_queue_dict[os.path.basename(default_queue_url)] = reply_queue


'''
sh = logging.FileHandler('mylog.log')
sh.setLevel(logging.INFO)

formatstr = '[%(asctime)s - %(name)s - %(levelname)s]  %(message)s'
formatter = logging.Formatter(formatstr)

sh.setFormatter(formatter)
logger.addHandler(sh)


logging.basicConfig(level=logging.INFO)
'''

app = typer.Typer()

def get_sqs_client():
    return boto3.client(
        'sqs',
        aws_access_key_id=config["AWS_SQS_QUEUE"]["aws_access_key"],
        aws_secret_access_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
        aws_session_token=config["AWS_SQS_QUEUE"]["aws_session_token"],
        region_name=config["AWS_SQS_QUEUE"]["region_name"]
    )

def get_reply_queue(queue_url):

    global reply_queue_dict

    queue_name = os.path.basename(queue_url)
    
    if queue_name in reply_queue_dict.keys():
        print("reply queue found")
        return reply_queue_dict[queue_name]
    
    reply_queue = ReplyQueueFactory(
        name=reply_queue_name,
        access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
        secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
        session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
        region_name=config["AWS_SQS_QUEUE"]['region_name']
    ).build()
    reply_queue_dict[queue_name] = reply_queue

    return reply_queue


def update_aws_credentials():

    global publisher
    global  reply_queue_dict  
    global config 
    global reply_queue
    
    # auth_key = api_key.encode("ascii", "ignore")
    headers =  { 'accept': 'application/json', "Content-Type":"application/json", "Authorization": f"Bearer {api_key}"}
    print(json.dumps(headers, indent=2))
    payload = {
        "account_number": str(account_number),
        "iam_role_name": str(iam_role_name)
    }
    print(cred_url)
    response = requests.post(cred_url, data=json.dumps(payload), headers=headers).json()
    print(response)
    status = response['status']
    data = response['data']

    config["AWS_SQS_QUEUE"]["aws_access_key"] = data['access_key']
    config["AWS_SQS_QUEUE"]["aws_secret_key"] = data['secret_access_key']
    config["AWS_SQS_QUEUE"]["aws_session_token"] = data['session_token']

    with open(CONFIG_FILE_PATH, 'w') as configfile:    # save
        config.write(configfile)

    os.environ["AWS_ACCESS_KEY"] = data['access_key']
    os.environ["AWS_SECRET_ACCESS_KEY"] = data['secret_access_key']
    os.environ["AWS_SESSION_TOKEN"] = data['session_token']

    publisher = PublisherFactory(
        access_key=data['access_key'],
        secret_key=data['secret_access_key'],
        session_token = data['session_token'],
        region_name=config["AWS_SQS_QUEUE"]['region_name']
    ).build()


    reply_queue = ReplyQueueFactory(
        name=reply_queue_name,
        access_key=data['access_key'],
        secret_key=data['secret_access_key'],
        session_token = data['session_token'],
        region_name=config["AWS_SQS_QUEUE"]['region_name']
    ).build()


@backoff.on_exception(backoff.expo,
                      botocore.exceptions.ClientError,
                      max_time=10)
def submit_message(data, request_queue_url=default_queue_url, timeout=reply_timeout_sec, reply_queue_name=reply_queue_name):
    
    try:
        sqs = get_sqs_client()
        res = sqs.get_queue_url(QueueName=reply_queue_name)
        print(res)
    except Exception as e:
        print(e.__class__.__name__)
        print(str(e))
        if "ExpiredToken" in str(e):
            update_aws_credentials()

    print("Updating Config Done")

    request_queue_name = os.path.basename(request_queue_url)
    queue_url = get_sqs_client().get_queue_url(QueueName=request_queue_name)['QueueUrl']

    message = RequestMessage(
        body= json.dumps(data),
        queue_url= queue_url,
        reply_queue=reply_queue
        #group_id="SOAMC_DEFAULT_GROUP"
    )

    if queue_url.lower().endswith("fifo"):
        print("FIFO Queue: {}".format(queue_url.lower()))
        try:
            group_id=config["AWS_SQS_QUEUE"]['fifo_group_id']
        except:
            group_id = "SOAMC_DEFAULT_GROUP"

        data["uuid"] = uuid.uuid4().hex
        message = RequestMessage(
            body= json.dumps(data),
            queue_url= queue_url,
            reply_queue=reply_queue,
            group_id=group_id
    )

    print("submit_message : queue_url : {} reply_queue : {} data : {}".format(queue_url, reply_queue.get_name(), json.dumps(data)))
    try:
        publisher.send_message(message)
        print("submit_message : sent")
    except Exception as e:
        print(e.__class__.__name__)
        print(str(e))
        if "ExpiredToken" in str(e):
            #respone = get_response_json_object(cred_url, api_key)
            update_aws_credentials(queue_url)
            raise e


    try:
        response = message.get_response(timeout=20)
        #print(response.body)
        return json.loads(response.body)
    except ReplyTimeout:
        return {"Error:": "Timeout"}
    except Exception as e:
        return {"Error": str(e)}
    finally:
        try:
            reply_queue.remove_queue()
        except Exception:
            pass

@app.command()
def getLandingPage(queue_url:str=default_queue_url):
    data = {'job_type': const.GET_LANDING_PAGE}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getProcesses(queue_url:str=default_queue_url):
    data = {'job_type': const.GET_PROCESSES}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def deployProcess(payload:str, queue_url:str=default_queue_url):
    data = {'job_type': const.DEPLOY_PROCESS, 'payload_data' : payload}
    response = submit_message(data, queue_url, timeout=deploy_process_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getProcessDescription(process_id: str, queue_url:str=default_queue_url):
    print(process_id)
    data = {'job_type': const.GET_PROCESS_DESCRIPTION, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def undeployProcess(process_id: str, queue_url:str=default_queue_url):
    print(process_id)
    data = {'job_type': const.UNDEPLOY_PROCESS, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getJobList(process_id: str, queue_url:str=default_queue_url):
    data = {'job_type': const.GET_JOB_LIST, 'process_id' : process_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def execute(process_id: str, payload_data: str, queue_url:str=default_queue_url):
    print(process_id)
    if os.path.exists(payload_data):
        with open(payload_data, 'r') as f:
            payload = json.load(f)
    else:
        payload =  payload_data
    data = {'job_type': const.EXECUTE, 'process_id' : process_id, 'payload_data' : payload}
    response = submit_message(data, queue_url, timeout=execute_reply_timeout_sec)
    print(json.dumps(response, indent=2))

@app.command()
def getStatus(process_id: str, job_id:str, queue_url:str=default_queue_url):
    print(process_id)
    data = {'job_type': const.GET_STATUS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def dismiss(process_id: str, job_id:str, queue_url:str=default_queue_url):
    print(process_id)
    data = {'job_type': const.DISMISS, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

@app.command()
def getResult(process_id: str, job_id:str, queue_url:str=default_queue_url):
    print(process_id)
    data = {'job_type': const.GET_RESULT, 'process_id' : process_id, 'job_id': job_id}
    response = submit_message(data, queue_url)
    print(json.dumps(response, indent=2))

if __name__=="__main__":
    sqs_config={}
    for key in config["AWS_SQS_QUEUE"]:
        sqs_config[key] = config["AWS_SQS_QUEUE"][key]
    if config["AWS_SQS_QUEUE"][key].isnumeric():
        sqs_config[key] = int(config["AWS_SQS_QUEUE"][key])

    app()
