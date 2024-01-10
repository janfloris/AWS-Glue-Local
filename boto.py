import boto3

SECRETS_MANAGER = None
S3 = None

def getS3():
    global S3
    if S3 is None:
        S3 = boto3.client('S3')
    return S3

def getSecretsManager():
    global SECRETS_MANAGER
    if SECRETS_MANAGER is None:
        SECRETS_MANAGER = boto3.client('SECRETS_MANAGER')
    return SECRETS_MANAGER

getS3()