import json
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import smtplib
from email.message import EmailMessage
import numpy
from flightsql import FlightSQLClient

email_address = "loracatfeeder@gmail.com"
email_password = "vfeidwkizljtcckl"

def get_cat_appeared_msg(weight, last_weight):
    # create email
    msg = EmailMessage()
    msg['Subject'] = "Your cat came to eat!"
    msg['From'] = email_address
    msg['To'] = "ddatsko.work@gmail.com"
    
    # msg.set_content("The weight in the bowl of the CatFeeder MK1 changed from " + str(lastweight) + " to " + str(weight) + " so a cat has most likely eaten some of the food!")
    msg.set_content(f"Weight change from {last_weight} to {weight}")
    return msg

def get_feeding_error_msg():
    # create email
    msg = EmailMessage()
    msg['Subject'] = "Food container is out of food!"
    msg['From'] = email_address
    msg['To'] = "ddatsko.work@gmail.com"
    
    # msg.set_content("Oh no! I tried to dispense food and nothing happened! Most likely cause: container is out of food! You can order some at https://www.svetkocicek.cz/krmivo-pro-kocky/")
    msg.set_content("Error in feeding")
    return msg

def send_msg_as_mail(msg):
    # send email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(email_address, email_password)
    server.sendmail(email_address, msg["To"], msg.get_content())
    server.close()
    
    
g_prev_weight = 0

def post_weight(weight: int):
    token = "bae4X-1qTsyIKHXGNvFnXKhTkjdkL-pR-9I_PP5WluOPm-u5tavqIPErjLUI3JRBLvKjNLUoyPPsCMHRwZmUdw=="
    org = "test"
    url = "https://us-east-1-1.aws.cloud2.influxdata.com"
    write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    bucket = "bowl"

    # Define the write api
    write_api = write_client.write_api(write_options=SYNCHRONOUS)

    point = Point("weight").field('grams', weight)
    write_api.write(bucket=bucket, org=org, record=point)


def check_weight_change(current_weight):
    query = """SELECT * from weight
WHERE
time >= now() - interval '1 minute'
ORDER by time ASC LIMIT 1"""
    token = "bae4X-1qTsyIKHXGNvFnXKhTkjdkL-pR-9I_PP5WluOPm-u5tavqIPErjLUI3JRBLvKjNLUoyPPsCMHRwZmUdw=="
    query_client = FlightSQLClient(host="us-east-1-1.aws.cloud2.influxdata.com", token=token, metadata={"bucket-name": "bowl"})
    info = query_client.execute(query)
    reader = query_client.do_get(info.endpoints[0].ticket)
    data = reader.read_all()
    print("ff")
    if len(data['grams']) != 0:
        last_weight = int(data['grams'][0].as_py())
        print(last_weight, current_weight)
        if last_weight - current_weight > 50:
            send_msg_as_mail(get_cat_appeared_msg(current_weight, last_weight))
    
        
def check_weight_change_no_db(current_weight, last_weight):
    print("Here")
    if last_weight - current_weight > 50:
        print("Senging")
        send_msg_as_mail(get_cat_appeared_msg(current_weight, last_weight))
    
        

def lambda_handler(event, context):
    body = json.loads(event['body'])
    post_weight(body['uplink_message']['decoded_payload']['bowl_weight'])
    status_code = body['uplink_message']['decoded_payload']['status_code']
    global g_prev_weight

    weight = int(body['uplink_message']['decoded_payload']['bowl_weight'])
    print(weight, g_prev_weight)
    check_weight_change_no_db(weight, g_prev_weight)
    g_prev_weight = weight

    if status_code == 1:
        send_msg_as_mail(get_feeding_error_msg())
        pass
    
    return {
            'statusCode': 200,
        }

