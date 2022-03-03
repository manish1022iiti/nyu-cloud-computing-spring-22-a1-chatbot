import json


def get_greeting_msg(name):
    name = name if name else ""
    return f"hello {name}, what can I do for you today?"


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
    name = event["currentIntent"]["slotDetails"]["Name"]["originalValue"]

    # adding return info into the response object
    response["dialogAction"]["message"]["content"] = get_greeting_msg(name)

    return response
