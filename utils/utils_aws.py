import json
import boto3
import base64
from typing import Any
from botocore.exceptions import ClientError

class AWSUtils:
    def __init__(self):
        """
        AWSUtils handles utility functions that interact with AWS services
        """
        pass

    @staticmethod
    def get_secret(secret_name: str, region_name: str) -> Any:
        """
        This static method retrieves secret values from AWS Secret Manager

        :param secret_name: Secret name as it appears in Secrets Manager
        :type secret_name: str
        :param region_name: AWS Region where the secret is stored
        :type region_name: str
        :return: Secret values as dictionary
        """
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        try:
            response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in response:
                try:
                    secret = json.loads(response["SecretString"])
                except Exception as e:
                    secret = response["SecretString"]
            else:
                secret = base64.b64decode(response["SecretBinary"])

        return secret