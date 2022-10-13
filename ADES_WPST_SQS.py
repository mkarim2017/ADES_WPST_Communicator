import logging 
import configparser
import json
import os
import uuid
import boto3
import requests


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
    
    def __init__(self, request_queue_name=None, reply_queue_name="", config_file="./sqsconfig.py"):
        
        self._config_file = config_file
        config = configparser.ConfigParser()
        config.read(config_file)

        self._config = config
        default_credential_file = os.path.join(os.path.expanduser('~'), ".aws/credentials")
        default_profile = "maap-hec"

        self._credentials_file =config["AWS_SQS_QUEUE"].get("aws_credentials_file", None)
        self._credentials_file_profile = None

        if self._credentials_file:
            self._credentials_file_profile = self._config["AWS_SQS_QUEUE"].get('aws_credentials_file_profile', default_profile)
            config2 = configparser.RawConfigParser()
            config2.read(self._credentials_file)
            self._access_key = config2.get(self._credentials_file_profile, 'aws_access_key_id')
            self._secret_key = config2.get(self._credentials_file_profile, 'aws_secret_access_key')
            self._session_token = None
            self._region_name = 'us-west-2' #config2.get(self._credentials_file_profile, {}).get('aws_secret_access_key', 'us-west-2')
        else:
            self._access_key=self._config["AWS_SQS_QUEUE"]["aws_access_key"]
            self._secret_key=self._config["AWS_SQS_QUEUE"]["aws_secret_key"]
            self._session_token = self._config["AWS_SQS_QUEUE"]["aws_session_token"]
            self._region_name = self._config["AWS_SQS_QUEUE"].get('region_name', 'us-west-2')
        self._reply_queue_dict = {}
        
        self._request_queue_name = request_queue_name
        self._reply_queue_name = reply_queue_name

        if not self._request_queue_name:
            self._request_queue_name = config["AWS_SQS_QUEUE"]["request_queue_name"].strip()
        if not self._reply_queue_name:
            self._reply_queue_name = config["AWS_SQS_QUEUE"].get("reply_queue_name", "reply_queue_{}".format(os.path.basename(self._request_queue_name))).strip()
       
        print("self._request_queue_name : {}".format(self._request_queue_name))
        print("self._reply_queue_name : {}".format(self._reply_queue_name))

        self._reply_queue = None
        self._publisher = None
        self.set_reply_queue(access_key=self._access_key, secret_key=self._secret_key, session_token=self._session_token, region_name=self._region_name, credentials_file_profile=self._credentials_file_profile)
        self.set_publisher(access_key=self._access_key, secret_key=self._secret_key, session_token=self._session_token, region_name=self._region_name, credentials_file_profile=self._credentials_file_profile)   

    
    def get_sqs_client(self):
        if self._credentials_file_profile:
            boto_session = boto3.Session(profile_name=self._credentials_file_profile)
            return boto_session.client('sqs', region_name=self._region_name)
        else:
            return boto3.client(
                'sqs',
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                aws_session_token=self._session_token,
                region_name=self._region_name
        )

    
    def set_publisher(self, access_key, secret_key, session_token, region_name, credentials_file_profile=None):
        self.reply_timeout_sec = int(self._config["AWS_SQS_QUEUE"].get("reply_timeout_sec", 20))
        self.execute_reply_timeout_sec = int(self._config["AWS_SQS_QUEUE"].get("execute_reply_timeout_sec", 600))
        self.deploy_process_timeout_sec = int(self._config["AWS_SQS_QUEUE"].get("deploy_process_timeout_sec", 900))

        self._publisher = PublisherFactory(
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name,
            credentials_file_profile=credentials_file_profile
        ).build()


    def get_publisher(self):
        return self._publisher


    def set_reply_queue(self, access_key, secret_key, session_token, region_name, credentials_file_profile=None):
        self._reply_queue = ReplyQueueFactory(
            name=self._reply_queue_name,
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name,
            credentials_file_profile=credentials_file_profile
        ).build()

        queue_name = os.path.basename(self._request_queue_name)
        self._reply_queue_dict[queue_name] = self._reply_queue


    def get_reply_queue(self):
        return self._reply_queue


    def update_env(self, access_key, secret_key, session_token):
        self._access_key = access_key
        self._secret_key = secret_key
        self._session_token = session_token

        os.environ["AWS_ACCESS_KEY"] = access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
        os.environ["AWS_SESSION_TOKEN"] = session_token

        self.set_reply_queue(access_key=self._access_key, secret_key=self._secret_key, session_token=self._session_token, region_name=self._region_name, credentials_file_profile=self._credentials_file_profile) 
        self.set_publisher(access_key=self._access_key, secret_key=self._secret_key, session_token=self._session_token, region_name=self._region_name, credentials_file_profile=self._credentials_file_profile)

        self._config["AWS_SQS_QUEUE"]["aws_access_key"] = access_key
        self._config["AWS_SQS_QUEUE"]["aws_secret_key"] = secret_key
        self._config["AWS_SQS_QUEUE"]["aws_session_token"] = session_token

        with open(self._config_file, 'w') as configfile:    # save
            self._config.write(configfile)

    
    def refresh_aws_credentials(self):
        api_key = self._config["AWS_SQS_QUEUE"].get("api_key")
        cred_url = self._config["AWS_SQS_QUEUE"].get("cred_url")
        account_number = self._config["AWS_SQS_QUEUE"].get("account_number")
        iam_role_name = self._config["AWS_SQS_QUEUE"].get("iam_role_name")

        headers = {}
        #headers =  { 'accept': 'application/json', "Content-Type":"application/json", "Authorization": f"Bearer {api_key}"}
        print(json.dumps(headers, indent=2))
        payload = {
            "account_number": str(account_number),
            "iam_role_name": str(iam_role_name)
        }
        response = requests.post(cred_url, data=json.dumps(payload), headers=headers).json()
        print(response)
        status = response['status']
        data = response['data']

        access_key = data['access_key']
        secret_key = data['secret_access_key']
        session_token = data['session_token']

        self.update_env(access_key, secret_key, session_token)


    def submit_message(self, data, request_queue_name=None, timeout=None):
        try:
            sqs = self.get_sqs_client()
            res = sqs.get_queue_url(QueueName=self._reply_queue_name)
            print(res)
        except Exception as e:
            print(e.__class__.__name__)
            print(str(e))
            if "ExpiredToken" in str(e) and not self._credentials_file_profile:
                self.refresh_aws_credentials()

        if not request_queue_name:
            request_queue_name = self._request_queue_name

        request_queue_name = os.path.basename(request_queue_name)
        # print(request_queue_name)
        self._request_queue_url = self.get_sqs_client().get_queue_url(QueueName=request_queue_name)['QueueUrl']

        if not timeout:
            timeout = self.reply_timeout_sec
        message = RequestMessage(
            body= json.dumps(data),
            queue_url= self._request_queue_url,
            reply_queue=self._reply_queue
        
        )

        try:
            self.get_publisher().send_message(message)
        except Exception as e:
            self.refresh_aws_credentials()
            raise e

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
        self._reply_queue.remove_queue()


    def getLandingPage(self):
        data = {'job_type': const.GET_LANDING_PAGE}
        print(data)
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response

    def getProcesses(self):
        #print("\nGET LIST of ALL PROCESSES")
        
        data = {'job_type': const.GET_PROCESSES}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
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
        response = self.submit_message(data, request_queue_name=self._request_queue_name,timeout=self.deploy_process_timeout_sec)
        return response

    
    def getProcessDescription(self, process_id: str):
        #print(process_id)
        data = {'job_type': const.GET_PROCESS_DESCRIPTION, 'process_id' : process_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response


    def undeployProcess(self, process_id: str):
        #print(process_id)
        data = {'job_type': const.UNDEPLOY_PROCESS, 'process_id' : process_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response


    def getJobList(self, process_id: str):
        print("\nGET ALL JOBS for PROCESS : {}".format(process_id))
        data = {'job_type': const.GET_JOB_LIST, 'process_id' : process_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
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
        response = self.submit_message(data, request_queue_name=self._request_queue_name, timeout=self.execute_reply_timeout_sec)
        print(json.dumps(response, indent=2))
        return response["jobID"]


    def getStatus(self, process_id: str, job_id:str):
        print("\nGET STATUS of PROCESS : {} JOB : {}".format(process_id, job_id))
        data = {'job_type': const.GET_STATUS, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response

    
    def dismiss(self, process_id: str, job_id:str):
        print(process_id)
        data = {'job_type': const.DISMISS, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response


    def getResult(self, process_id: str, job_id:str):
        print(process_id)
        data = {'job_type': const.GET_RESULT, 'process_id' : process_id, 'job_id': job_id}
        response = self.submit_message(data, request_queue_name=self._request_queue_name)
        return response


    def fullResult(self):
        processes = self.getProcesses()
        jobs = {}
        for p in processes:
            jobs = self.getJobList(p)
            for j in jobs:
                response = self.getStatus(p, j)
                print(json.dumps(response, indent=2))


def main():
    wpst = ADES_WPST_SQS() 
    print(json.dumps(wpst.getLandingPage(), indent=2))


if __name__ == "__main__":
  main()

