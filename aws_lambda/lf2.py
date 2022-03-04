import json
import random
import boto3
import datetime
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


def send_text_message(msg):
    sns_wrapper = boto3.resource('sns')
    topic_name = f"demo-basics-topic"

    # print(f"Creating topic {topic_name}.")
    # topic = sns_wrapper.create_topic(Name=topic_name)

    phone_number1 = "+19174785785"
    phone_number2 = "+19172936556"

    print(f"Sending an SMS message directly from SNS to {phone_number1}: {msg}")
    response = sns_wrapper.meta.client.publish(PhoneNumber=phone_number1, Message=msg)
    message_id = response['MessageId']
    print(f"Published message to {phone_number1}.")

    print(f"Sending an SMS message directly from SNS to {phone_number2}: {msg}")
    response = sns_wrapper.meta.client.publish(PhoneNumber=phone_number2, Message=msg)
    message_id = response['MessageId']
    print(f"Published message to {phone_number2}.")


def send_email(msg):
    SENDER = "Manish Singh Saini <mss9238@nyu.edu>"
    RECIPIENT1 = "manish1022iiti@gmail.com"
    RECIPIENT2 = "ag8177@nyu.edu"

    # Specify a configuration set. If you do not want to use a configuration
    # set, comment the following variable, and the
    # ConfigurationSetName=CONFIGURATION_SET argument below.
    # CONFIGURATION_SET = "ConfigSet"

    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "Your Restaurant Recommendation"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = (f"Your Restaurant Recommendation: {msg} \r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )

    # The HTML body of the email.
    BODY_HTML = f"""<html>
    <head></head>
    <body>
      <h1>Your Restaurant Recommendation</h1>
      <p>This email was sent with
        <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
        <a href='https://aws.amazon.com/sdk-for-python/'>
          AWS SDK for Python (Boto)</a>.</p>
        <p>Here is your recommendation: {msg}</p>
    </body>
    </html>
                """

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT1,
                    RECIPIENT2
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # # If you are not using a configuration set, comment or delete the
            # # following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print(f"Email sent! Message ID: {response['MessageId']}"),


def get_restaurant_ids_from_opensearch(cuisine):
    host = "search-domain2-pogtt2zz253ds6potcveo4gi2a.us-east-1.es.amazonaws.com"
    region = "us-east-1"

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    index_name = "restaurants"

    search = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Search for the document.
    q = 'indian'
    query = {
        'query': {
            'match': {
                'cuisine': cuisine,
            }
        }
    }

    response = search.search(
        body=query,
        index=index_name
    )
    print(f'Response from OpenSearch: {response}')

    tot_suggestions = len(response["hits"]["hits"])
    print(f"total number of restaurant suggestions: {tot_suggestions}")

    # picking at max. 5 restaurant recommendations
    rids = set()
    for i in range(min(5, tot_suggestions)):
        idx = random.randint(0, tot_suggestions - 1)
        rids.add(response["hits"]["hits"][idx]["_id"])
    rids = list(rids)

    # rid = response["hits"]["hits"][0]["_id"]
    print(f"restaurant_ids (picked): {rids}")
    return rids


def get_restaurant_data_from_dynamodb(rids):
    session = boto3.Session()
    ddb = session.client("dynamodb")
    table = "yelp-restaurants"

    # getting data for restaurants for various ids
    ret_data = list()
    for rid in rids:
        ddb_response = ddb.get_item(TableName=table, Key={"id": {"S": rid}})
        print(f"Response from DYnamo db for id: {rid}: {ddb_response}")

        data = dict()
        data["name"] = ddb_response["Item"]["name"]["S"]
        data["address"] = ", ".join([part["S"] for part in ddb_response["Item"]["address"]["L"]])
        data["zipcode"] = ddb_response["Item"]["zip_code"]["S"]
        data["rating"] = ddb_response["Item"]["rating"]["N"]
        data["reviews"] = ddb_response["Item"]["reviews"]["N"]
        ret_data.append(data)

    print(f"restaurant_data(picked): {ret_data}")
    return ret_data


def put_user_recommendation_into_dynamodb(msg, user_data):
    session = boto3.Session()
    ddb = session.client("dynamodb")
    table = "NYRestoUsers"
    item = dict()
    item["phoneNum"] = {"S": user_data["phone_num"]}
    item["insertTs"] = {"S": datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")}
    item["recommendation"] = {"S": msg}
    response = ddb.put_item(TableName=table, Item=item)
    print(f"response from dynamodb after putting userdata: {response}")


def get_restaurant_recommendation_msg(user_attr):
    cuisine = user_attr["cuisine"]
    rids = get_restaurant_ids_from_opensearch(cuisine)
    data = get_restaurant_data_from_dynamodb(rids)

    # preparing message to return to the user
    msg = list()
    msg.append(
        f"Hello there, I am recommending you following {cuisine} restaurants for your recent request to dine in. Enjoy your meal!\n")
    for i, d in enumerate(data):
        msg.append(
            f"[{i + 1}] Restaurant Name: {d['name']}; Address: {d['address']}, {d['zipcode']}; Rating: {d['rating']}; Number of Reviews: {d['reviews']}.\n")

    msg = "\n".join(msg)
    print(f"restaurant recommendation message: {msg}")

    return msg


def pull_query_param_from_queue():
    # sqs url
    SQS_URL = "https://sqs.us-east-1.amazonaws.com/104588357780/TestQueue"

    # Create SQS client
    sqs = boto3.client('sqs')

    # receiving message from SQS
    response = sqs.receive_message(QueueUrl=SQS_URL, MaxNumberOfMessages=1, WaitTimeSeconds=20,
                                   MessageAttributeNames=['All'])
    # print(response)

    msgs = response.get("Messages", list())
    if msgs:
        print(f"number of messages received from SQS: {len(msgs)}")
        print(f"msgs: {msgs}")

        msg_id = msgs[0]["MessageId"]
        msg_body = msgs[0]["Body"]
        msg_attr = dict()
        msg_attr["cuisine"] = msgs[0]["MessageAttributes"]["cuisine"]["StringValue"]
        msg_attr["phone_num"] = msgs[0]["MessageAttributes"]["phone_num"]["StringValue"]
        receipt_handle = msgs[0]["ReceiptHandle"]

        # deleteing message from queue
        response = sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
        print(f"Deleting request's response from SQS: {response}")
        print(f"message attributes (Extracted from SQS message): {msg_attr}")
        return msg_attr
    else:
        return {"cuisine": "lebanese", "phone_num": ""}


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
    # print(event)

    user_data = pull_query_param_from_queue()
    msg = get_restaurant_recommendation_msg(user_data)
    try:
        put_user_recommendation_into_dynamodb(msg, user_data)
    except Exception as e:
        print(f"error while putting user recommendation into dynamodb: {e}")
    send_text_message(msg)
    send_email(msg)

    return response
