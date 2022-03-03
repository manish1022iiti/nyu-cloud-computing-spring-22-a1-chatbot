import json
import boto3


def push_query_to_queue(data):
    # sqs url
    SQS_URL = "https://sqs.us-east-1.amazonaws.com/104588357780/TestQueue"

    # Create SQS client
    sqs = boto3.client('sqs')

    # preparing message attributes
    msg_attr = dict()
    msg_attr["location"] = {"DataType": "String", "StringValue": str(data["location"])}
    msg_attr["cuisine"] = {"DataType": "String", "StringValue": str(data["cuisine"])}
    msg_attr["dining_time"] = {"DataType": "String", "StringValue": str(data["dining_time"])}
    msg_attr["dining_date"] = {"DataType": "String", "StringValue": str(data["dining_date"])}
    msg_attr["party_size"] = {"DataType": "String", "StringValue": str(data["party_size"])}
    msg_attr["phone_num"] = {"DataType": "String", "StringValue": str(data["phone_num"])}

    # sending message to SQS and getting back response
    response = sqs.send_message(QueueUrl=SQS_URL, DelaySeconds=10, MessageAttributes=msg_attr,
                                MessageBody=("Some user is asking for dining suggestions!"))

    # response = sqs.send_message(QueueUrl=SQS_URL, DelaySeconds=10,
    #                             MessageAttributes={
    #                                 'Title': {
    #                                     'DataType': 'String',
    #                                     'StringValue': 'The Whistler'
    #                                 },
    #                                 'Author': {
    #                                     'DataType': 'String',
    #                                     'StringValue': 'John Grisham'
    #                                 },
    #                                 'WeeksOn': {
    #                                     'DataType': 'Number',
    #                                     'StringValue': '6'
    #                                 }
    #                                 },
    #                                 MessageBody=(
    #                                     'Information about current NY Times fiction bestseller for '
    #                                     'week of 12/11/2016.'
    #                                 )
    #                             )

    print(f"Message ID of the pushed SQS message: {response['MessageId']}")


def get_confirmation_msg(location, cuisine, dining_time, dining_date, party_size, phone_num):
    return f"So you are looking for a {cuisine} food restaurant for {party_size} people in {location} at {dining_time}, {dining_date}. Well, I am gonna send you my findings at your number\
    {phone_num} shortly"


def error_response():
    return "Geez, I don't know what to say!"


def lambda_handler(event, context):
    # creating response object
    response = {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText"
            }
        }
    }
    print(event)

    # extracting currentIntent details
    location = event["currentIntent"]["slotDetails"]["LocationType"]["originalValue"]
    cuisine = event["currentIntent"]["slotDetails"]["CuisineType"]["originalValue"]
    dining_time = event["currentIntent"]["slotDetails"]["DiningTime"]["resolutions"][0]["value"]
    dining_date = event["currentIntent"]["slotDetails"]["DiningDate"]["resolutions"][0]["value"]
    party_size = event["currentIntent"]["slotDetails"]["NumPeople"]["resolutions"][0]["value"]
    phone_num = event["currentIntent"]["slotDetails"]["PhoneNum"]["resolutions"][0]["value"]

    # getting confirmation message to be sent to the user of chatbot
    response["dialogAction"]["message"]["content"] = get_confirmation_msg(location, cuisine, dining_time, dining_date,
                                                                          party_size, phone_num)

    # pushing info to the SQS queue
    data = dict()
    data["location"] = location
    data["cuisine"] = cuisine
    data["dining_time"] = dining_time
    data["dining_date"] = dining_date
    data["party_size"] = party_size
    data["phone_num"] = phone_num

    push_query_to_queue(data)

    return response
