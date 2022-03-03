import json
import boto3


def get_response_msg(inp_msg):
    # Lex calls will go here
    client = boto3.client('lex-runtime')
    data = inp_msg

    response = client.post_text(botName="NYResto", botAlias="Prod", userId='id', inputText=data)
    print(f"Chatbot response: {response}")

    ret_msg = response["message"]
    print(f"return message to the user: {ret_msg}")

    return ret_msg
    # return "I am still under development!"


def lambda_handler(event, context):
    print(f"event: {event}")

    # responseObject={}
    # responseObject['statusCode']=200
    # responseObject['body']='Still under maintenance'
    # return responseObject
    # message="Still under maintenance"
    # return {
    #     'headers': {
    #         'Access-Control-Allow-Headers': 'Content-Type',
    #         'Access-Control-Allow-Origin': 'http://uitesting-assignment1.s3-website-us-east-1.amazonaws.com',
    #         'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    #     },
    #     "statusCode":200,
    #     "body":json.dumps("still under maintenance")

    # }

    inp_msg = ". ".join([msg_struct["unstructured"]["text"] for msg_struct in event["messages"]])
    print(f"user input message: {inp_msg}")

    ret_msg = get_response_msg(inp_msg)
    print(f"response message to the user: {ret_msg}")

    ret_obj = dict()
    ret_obj["messages"] = list()
    ret_obj["messages"].append({
        "type": "unstructured",
        "unstructured": {"text": ret_msg}
    })
    print(f"final return object: {ret_obj}")

    # return{
    #     'messages': [{'type': 'unstructured', 'unstructured': {'text': "I’m still under development. Please come back later."}}]
    # }
    return ret_obj

