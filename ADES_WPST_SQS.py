import logging 
import configparser
import json
import os
import uuid

from sqs_client.factories import ReplyQueueFactory, PublisherFactory
from sqs_client.message import RequestMessage
from sqs_client.exceptions import ReplyTimeout
from constants import constants as const

logger = logging.getLogger('ADES_WPST_SQS')
logger.setLevel(logging.INFO)


CONFIG_FILE_PATH = r'sqsconfig.py'


'''
sh = logging.FileHandler('mylog.log')
sh.setLevel(logging.INFO)

formatstr = '[%(asctime)s - %(name)s - %(levelname)s]  %(message)s'
formatter = logging.Formatter(formatstr)

sh.setFormatter(formatter)
logger.addHandler(sh)


logging.basicConfig(level=logging.INFO)
'''

class ADES_WPST_SQS():
    
    def __init__(self, queue_url=None, reply_queue_name="", config_file="./sqsconfig.py"):
        
        config = configparser.ConfigParser()
        config.read(config_file)
        
        self.queue_url = queue_url
        self.reply_queue_name = reply_queue_name
        
        if not self.queue_url:
            self.queue_url = config["AWS_SQS_QUEUE"]["queue_url"]
        
        if reply_queue_name == "":
            self.reply_queue_name = "reply_queue_{}".format(os.path.basename(self.queue_url))
        self.reply_queue = ReplyQueueFactory(
            name=self.reply_queue_name,
            access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
            secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
            session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
            region_name=config["AWS_SQS_QUEUE"]['region_name']
        ).build()
        

        default_queue_url = config["AWS_SQS_QUEUE"].get('queue_url', None)
        self.reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("reply_timeout_sec", 20))
        self.execute_reply_timeout_sec = int(config["AWS_SQS_QUEUE"].get("execute_reply_timeout_sec", 600))
        self.deploy_process_timeout_sec = int(config["AWS_SQS_QUEUE"].get("deploy_process_timeout_sec", 900))

        self.publisher = PublisherFactory(
            access_key=config["AWS_SQS_QUEUE"]["aws_access_key"],
            secret_key=config["AWS_SQS_QUEUE"]["aws_secret_key"],
            session_token = config["AWS_SQS_QUEUE"]["aws_session_token"],
            region_name=config["AWS_SQS_QUEUE"]['region_name']
        ).build()

    def submit_message(self, data, timeout=None):

        print("\n")
        #reply_queue = get_reply_queue(queue_url)
        #print(data)
        if not timeout:
            timeout = self.reply_timeout_sec
        message = RequestMessage(
            body= json.dumps(data),
            queue_url= self.queue_url,
            reply_queue=self.reply_queue
        
        )
        
        #print("submit_message : queue_url :  data : {}".format(self.queue_url,  json.dumps(data)))
        self.publisher.send_message(message)
        # print("submit_message : sent")

        try:
            response = message.get_response(timeout=20)
            #print(response.body)
            return json.loads(response.body)
        except ReplyTimeout:
            return {"Error:": "Timeout"}
        except Exception as e:
            return {"Error": str(e)}

    def cleanup(self):
        self.reply_queue.remove_queue()

    def getLandingPage(self):
        data = {'job_type': const.GET_LANDING_PAGE}
        print(data)
        response = self.submit_message(data)
        return response

    def getProcesses(self):
        #print("\nGET LIST of ALL PROCESSES")
        
        data = {'job_type': const.GET_PROCESSES}
        response = self.submit_message(data)
        proc_list = []
        #data = json.loads(response)
        processes = response["processes"]
        for d in processes:
            proc_list.append(d["id"])
        print("\n\nProcesses: {}".format(proc_list))
        # print(json.dumps(response, indent=2))
        return proc_list
  
    def deployProcess(self, payload:str):
        data = {'job_type': const.DEPLOY_PROCESS, 'payload_data' : payload}
        response = self.submit_message(data, timeout=self.deploy_process_timeout_sec)
        return response

    
    def getProcessDescription(self, process_id: str):
        #print(process_id)
        data = {'job_type': const.GET_PROCESS_DESCRIPTION, 'process_id' : process_id}
        response = self.submit_message(data)
        return response


    def undeployProcess(self, process_id: str):
        #print(process_id)
        data = {'job_type': const.UNDEPLOY_PROCESS, 'process_id' : process_id}
        response = self.submit_message(data)
        return response

    def getJobList(self, process_id: str):
        print("\nGET ALL JOBS for PROCESS : {}".format(process_id))
        data = {'job_type': const.GET_JOB_LIST, 'process_id' : process_id}
        response = self.submit_message(data)
        job_list = []
        #data = json.loads(response)
        jobs = response["jobs"]
        for j in jobs:
            job_list.append(j["jobID"])
        print("\n\nJobs for process {} : {}".format(process_id, job_list))
        #print(json.dumps(response, indent=2))
        return job_list

    def execute(self, process_id: str, payload_data: str):
        print(process_id)
        try:
            json.loads(json.dumps(payload_data))
            payload =  payload_data
        except Exception as err:
            if os.path.isfile(payload_data) and os.path.exists(payload_data):
                with open(payload_data, 'r') as f:
                    payload = json.load(f)
            else:
                raise Exception("Payload should be a valid json object or json file")
        print(json.dumps(payload, indent=2))    
        data = {'job_type': const.EXECUTE, 'process_id' : process_id, 'payload_data' : payload}
        response = self.submit_message(data, timeout=self.execute_reply_timeout_sec)
        print(json.dumps(response, indent=2))
        return response["jobID"]

    def getStatus(self, process_id: str, job_id:str):
        print("\nGET STATUS of PROCESS : {} JOB : {}".format(process_id, job_id))
        data = {'job_type': const.GET_STATUS, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data)
        return response

    
    def dismiss(self, process_id: str, job_id:str):
        print(process_id)
        data = {'job_type': const.DISMISS, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data)
        return response


    def getResult(self, process_id: str, job_id:str):
        print(process_id)
        data = {'job_type': const.GET_RESULT, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data)
        return response

    def fullResult(self):
        processes = self.getProcesses()
        jobs = {}
        for p in processes:
            jobs = self.getJobList(p)
            for j in jobs:
                response = self.getStatus(p, j)
                print(json.dumps(response, indent=2))
 