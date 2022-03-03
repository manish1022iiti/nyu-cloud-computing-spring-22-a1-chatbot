import datetime
import json
import boto3

def get_raw_json_items():
    rfile = "yelp/parsed_data/resto_data_manhattan_part3.json"
    with open(rfile, "r") as fp:
        data = json.load(fp)
    return data


def json_to_dynamodb_json(data):
    ndata = list()
    for _, cuisine_data in data.items():
        for _, idata in cuisine_data.items():
            item = dict()
            item["id"] = {"S": idata["id"]}
            item["name"] = {"S": idata["name"]}
            item["address"] = {"L": [{"S": i} for i in idata["location"]["display_address"]]}
            item["coordinates"] = {"M": {"latitude": {"N": str(idata["coordinates"]["latitude"])},
                                         "longitude": {"N": str(idata["coordinates"]["longitude"])}}}
            item["reviews"] = {"N": str(idata["review_count"])}
            item["rating"] = {"N": str(idata["rating"])}
            item["zip_code"] = {"S": idata["location"]["zip_code"]}
            item["insert_ts"] = {"N": datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")}
            ndata.append(item)
    return ndata


def main(run=0):
    if run == 1:
        session = boto3.Session()
        ddb = session.client("dynamodb")
        table = "yelp-restaurants"

        items = get_raw_json_items()
        dditems = json_to_dynamodb_json(data=items)

        for i, dditem in enumerate(dditems):
            print(f"Inserting item: {i}/{len(dditems)}")
            ddb.put_item(TableName=table, Item=dditem)


main(1)
