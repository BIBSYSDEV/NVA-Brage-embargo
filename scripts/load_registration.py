import boto3
import requests
import uuid

USER = 'Dataporten_2b544d30-3aa4-4572-aaf8-4031c676f2b5'
ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/CognitoUserPoolId',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/CognitoUserPoolAppClientId',
                              WithDecryption=False)['Parameter']['Value']

get_registration_endpoint = 'https://api.{}.nva.aws.unit.no/publication/{}'
STAGE = 'e2e'

headers = {
    'Authorization': '',
    'accept': 'application/json'
}


def login(username):
    client = boto3.client('cognito-idp')
    password = f'P_{str(uuid.uuid4())}'
    client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=username,
        Permanent=True,
    )
    trying = True
    count = 0
    while trying:
        try:
            response = client.initiate_auth(
                AuthFlow='USER_PASSWORD_AUTH',
                ClientId=CLIENT_ID,
                AuthParameters={
                    'USERNAME': username,
                    'PASSWORD': password
                }
            )
            return response['AuthenticationResult']['AccessToken']
        except:
            count+=1
            if count == 3: trying = False
    return ''

def load_registration(id, environment):
    auth_token = login(USER)
    headers['Authorization'] = f'Bearer {auth_token}'
    url = get_registration_endpoint.format(environment, id)
    response = requests.get(headers=headers, url=url)
    return response.json()

if __name__ == '__main__':
    load_registration()