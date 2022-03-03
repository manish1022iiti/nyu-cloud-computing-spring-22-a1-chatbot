import json


def get_welcome_msg():
    return f"You are welcome! Have a good day."


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

    # adding return info into the response object
    response["dialogAction"]["message"]["content"] = get_welcome_msg()

    return response
