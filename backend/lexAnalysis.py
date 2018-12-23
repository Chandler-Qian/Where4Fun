from __future__ import print_function
from botocore.vendored import requests
import json
import boto3


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

# ----------------Logic Begin---------------- #
    



def getSentiment(sentiment):
    data = {
        'text': sentiment
        }

    response = requests.post('http://text-processing.com/api/sentiment/',data=data)
    response = json.loads(response.text)
    label = response["label"]
    prob = response["probability"][label]
    
    return str(prob * 5)

def getid():
    import random
    return random.randint(1,65535)


def submitSample():
    emr = boto3.client("emr")
    # clusters = emr.list_clusters()
    # clusters = [c["Id"] for c in clusters["Clusters"] if c["Status"]["State"] in ["RUNNING","WAITTING"]]
    cluster_id = 'j-37LMBVYQ3AO2W'
    CODE_DIR = '/home/hadoop/code/'
    step_args = ["/usr/bin/spark-submit", CODE_DIR + "sample.py" , event["args"]]
    response = emr.add_job_flow_steps(
        JobFlowId = cluster_id,
        Steps = [
            {
              'Name':'run sample.py',# + str(datetime.datetime.now()),
              'ActionOnFailure' : 'CONTINUE',
              'HadoopJarStep' : {
                  'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                  'Args' : step_args
              }
            },
        ]
    )

def getFromLex(intent_request):
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'Review':
        place = get_slots(intent_request)["place"]
        sentiment = get_slots(intent_request)["sentiment"]
        return (place,getSentiment(sentiment))
    elif intent_name == 'ForFun':
        # submitSample()
        pass

def StoreInDb(tup):
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UserRatings')
    
    userid = getid()
    place = tup[0]
    rating = tup[1]
    
    table.put_item(
       Item={
            'userId' : userid,
            'place' : place,
            'rating' : rating 
        }
    )

def responseToLex(intent_request,content):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': content})



def lambda_handler(event, context):
    # TODO implement
    tup = getFromLex(event)
    print(tup)
    
    
    intent_name = event['currentIntent']['name']
    if intent_name == "ForFun":
    
        return responseToLex(event,"Suggesting......Please drink a cup of tea and keep waiting")
    else:
        t = StoreInDb(tup)
        return responseToLex(event,"Great! We know you better now!")
