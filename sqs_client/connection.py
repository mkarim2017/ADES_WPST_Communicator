import boto3

from sqs_client.contracts import SqsConnection as SqsConnectionBase

class SqsConnection(SqsConnectionBase):

    def __init__(self, region_name: str, access_key: str=None, secret_key: str=None, session_token: str=None, credentials_file_profile=None):
        self._access_key = access_key 
        self._secret_key = secret_key
        self._session_token = session_token
        self._credentials_file_profile = credentials_file_profile
        self._region_name = region_name 
        self._queue_url = None
        self._load_resource()
        self._load_client()

    def set_queue(self, queue_url: str):
        self._queue_url = queue_url
    
    def get_queue_resource(self, queue_url: str=None):
        self._set_queue(queue_url)
        return self.resource.Queue(self._queue_url)

    def _set_queue(self, queue_url: str=None):
        self._queue_url = queue_url if queue_url else self._queue_url
        if not self._queue_url:
            raise Exception("Queue is not defined.")

    def _load_resource(self):
        print(self._credentials_file_profile)
        if self._credentials_file_profile:
            session = boto3.Session(profile_name=self._credentials_file_profile)
        else:
            if self._session_token:
                session = boto3.Session(
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    aws_session_token=self._session_token
                )
            else:
                session = boto3.Session(
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key
                )
        self.resource = session.resource('sqs', region_name=self._region_name)

    def _load_client(self):
        if self._credentials_file_profile:
            session = boto3.Session(profile_name=self._credentials_file_profile)
            self.client = session.client('sqs')
        else:
            if self._session_token:
                self.client = boto3.client(
                    'sqs',
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    aws_session_token=self._session_token,
                    region_name=self._region_name
                )
            else:
                self.client = boto3.client(
                    'sqs',
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    region_name=self._region_name
                )
