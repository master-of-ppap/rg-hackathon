import json
import sys
import time
import threading
from pprint import pprint
from uuid import uuid4

import boto3
from awscrt import auth, io, mqtt
from awscrt.exceptions import AwsCrtError
from awsiot import mqtt_connection_builder
from flask import Flask, app, Response, jsonify, render_template, stream_with_context

app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates"
)

latest_message = None

refresh_token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.bzBsNc_xZotEaKiVvRuKuBgf4-Y1imOicaAclK1VI78m5QrS1i5KAU1wxpmM0L0iSR59VsX844ThS2rUyrq7isXzFO0X4pi-IksihbwASmE3h15iHKgTf0YJEMfG8xRm65R5E5GmCiXp2vXDJ_K_LN0HXWrc_igfDVZCXCGVdHRZ_wsYKDmtXpgvLTug05omz0KIvQGVMukntWmk5GGvLd98CitHTXCJ_QhB5s5eN4jcrzg0UrVOejoVEQ1qYfuA8cVU532CXvRDuyC8BfjzG20qo7CGrCjNXsQwe6wCrbIcO_RzPxqAYUynnuap9LhvpRhm-k63U0pcMWTLQ1-klA.Wt0k-2VjKDK_vXZS.m04d3Js4_0vXaCmyROUQZ7FfVFB9hKBUrqe2CHRjWjLb3VAdVuvLSzQELsipBpyBtoRkYwFpI6QFjAhR-YXh5aEvWZ_IYciIGuHBd8_mTTvRCQDY40BSDIVWqUPq4NLbAHDTvMaWpuPdPO76XTPiu80B5dvs9xCSTOKs1_7_NdMncevFAKpGDEoooUH26bX8hr7eE1GoWf4dDpOhtpHe2ME_W_0hHETQmiTamUVK8mBEDm-CNBvkVrYNl7qmgabAUTDnaF8RHuWAlCbsSzSJ6riBIrX2lJh4uxFywkGeurL4u8XNA-aXzPb81bXO11MW76zzfECA9NEKL-wWZ5r4z2QBnMhH0R7cNMDned39Ynrm9IVA6WcIuXp3oCfEEcCfvQxrL5uY1FIta7hvjoGrtkpZkO72WA0or2JlLzzUyRH1BzRflCWDIkbYb1l4AtrLlD6VurWLwm5PtOeHRVpMPVoGfj0CPqWvTRPjS5641qqjZlp6eXPevbJrHFt282orXA8NskrUiET4nTLdzTfRIogtyliH55TaoewlxRXF5xbaiIoN3qdmyvuvVIVpi48WdaIxYSW6AM1EvggrQK6P2DRrpavgM1_9bJl6S6voqEKnIEofpXf8jqKAz07NNzBmngE1T5DozSEaXp9BrPaFXpQwb4JDSnN5kbw0PVDW_FFuU_AtwjPpuw9dYgH2GGu9Ec8hOzaimRw2-yRgYJDRexsmcpI0l66LYybVNV-6U_stR1LkoRFBKUwOJWaDJ26eA2oQmD-I5A_3_-2MbFXV8guz5wBsMS1SGJ9WreXdMk1x6Mb4S9C2audDNo7zeJM8ziVfIQWk2r1iJzdXonztW_V_Io0yCUnUy7-V9ln_Zpj9fcrKxRmpO7S8I5bkvIJfo2PbW24F3Rnxfy0wSzM_gfnw4pdncaZi7iJNMStAqBQsEgp3opVyigaghzuFLkn4hUw6h0fVt0DrIZt-x1uduEuemBPV2KAWmQYwiot-wYKMTSJGOAznQnfxPqA7P0wjiofW9sOGVi9FXfwONGlPdLRtkvHGt4oKopmzMBa0N1r0-fArvp8678OBk_i15Dpoa2PB0lqgvcW73yN-8HXFLVvFrg7nKa-nAR7u2JeaO0Q8DH64RXxCfURNn1KKNfUYZ1cd9zGGkA48cz9yyC9iw0aZlN0tb__j5snEtmE2MnqmVDPSCMqWbU4R-sBkev8dMouPdDjtjJ4z-Io5ZVCXA7p73npwXQv3NNMV-cmpOresWx0VpH3jvEwt4kqwVUc1XTmS_vhHPsZjbGQEXwi1CT72jSaNTWJ-aYzheUW-uImxzFXBeAGfIkihcpA2qYarJYQS27J8AMEzng.XVct4qy8IJwy-VtsNNeRPQ"
region = "ap-northeast-1"
user_pool_id = "ap-northeast-1_kRWuig6oV"
user_pool_client_id = "2jl8m0q968eudj7lubpdkuvq9v"
identity_pool_id = "ap-northeast-1:7e24baf3-0e4b-4c3a-bacf-ca1e9b7f4650"
endpoint = "ak6s01k4r928v-ats.iot.ap-northeast-1.amazonaws.com"
message_topic = "signal/0/north-south"
client_id = "sample-" + str(uuid4())


def fetch_id_token(refresh_token: str,
                  user_pool_client_id: str,
                  region: str = "ap-northeast-1") -> str:
    client = boto3.client("cognito-idp", region_name=region)
    response: dict = client.initiate_auth(
        AuthFlow='REFRESH_TOKEN_AUTH',
        AuthParameters={'REFRESH_TOKEN': refresh_token},
        ClientId=user_pool_client_id
    )
    return response['AuthenticationResult']['IdToken']


def fetch_identity_id(id_token: str,
                      user_pool_id: str,
                      identity_pool_id: str,
                      region: str = "ap-northeast-1") -> str:
    client = boto3.client("cognito-identity", region_name=region)
    response = client.get_id(
        IdentityPoolId=identity_pool_id,
        Logins={f"cognito-idp.{region}.amazonaws.com/{user_pool_id}": id_token}
    )
    return response['IdentityId']


def on_connection_interrupted(connection: mqtt.Connection,
                              error: AwsCrtError,
                              **kwargs) -> None:
    print(f"Connection interrupted. error: {error}")


def on_connection_resumed(connection: mqtt.Connection,
                          return_code: mqtt.ConnectReturnCode,
                          session_present: bool,
                          **kwargs) -> None:
    print("Connection resumed. "
          f"return_code: {return_code} session_present: {session_present}")


def on_message_received(topic: str,
                        payload: bytes,
                        dup: bool,
                        qos: mqtt.QoS,
                        retain: bool,
                        **kwargs) -> None:
    global latest_message
    print(f"Received message from topic '{topic}'")
    decoded_payload = json.loads(payload)
    pprint(decoded_payload)
    latest_message = decoded_payload


def connect_and_subscribe():
    id_token = fetch_id_token(
        refresh_token=refresh_token,
        user_pool_client_id=user_pool_client_id,
        region=region,
    )

    global identity_id
    if identity_id is None:
        identity_id = fetch_identity_id(
            id_token=id_token,
            user_pool_id=user_pool_id,
            identity_pool_id=identity_pool_id,
            region=region,
        )

    credentials_provider = auth.AwsCredentialsProvider.new_cognito(
        endpoint=f"cognito-identity.{region}.amazonaws.com",
        identity=identity_id,
        tls_ctx=io.ClientTlsContext(io.TlsContextOptions()),
        logins=[
            (f"cognito-idp.{region}.amazonaws.com/{user_pool_id}",
                id_token),
        ]
    )

    mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
        endpoint=endpoint,
        client_id=client_id,
        region=region,
        credentials_provider=credentials_provider,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        clean_session=False,
        reconnect_min_timeout_secs=1,
        keep_alive_secs=30,
    )

    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected!")

    print(f"Subscribing to topic '{message_topic}'...")
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_MOST_ONCE,
        callback=on_message_received)
    subscribe_future.result()
    print("Subscribed!")

    try:
        while True:
            time.sleep(1)
    except Exception as e:
        print(e, file=sys.stderr)
    finally:
        print("Disconnecting...")
        mqtt_connection.disconnect().result()
        print("Disconnected!")


def mqtt_thread():
    global identity_id
    identity_id = None
    backoff_time = 1
    while True:
        try:
            connect_and_subscribe()
        except Exception as e:
            print(e, file=sys.stderr)
        time.sleep(backoff_time)
        backoff_time = min(backoff_time * 2, 600)

# Start MQTT listener in background
mqtt_listener = threading.Thread(target=mqtt_thread, daemon=True)
mqtt_listener.start()

@app.route('/')
def index():
    # simply renders templates/index.html
    return render_template('index.html')

@app.route('/message')
def get_message():
    global latest_message
    if latest_message:
        return jsonify(latest_message)
    else:
        return jsonify({'error': 'No data yet'}), 204
@app.route('/stream')
def stream():
    def event_stream():
        last = None
        while True:
            if latest_message is not None and latest_message != last:
                # push the full JSON payload
                yield f"data: {json.dumps(latest_message)}\n\n"
                last = latest_message
            time.sleep(0.2)  # check 5×/sec
    return Response(
        stream_with_context(event_stream()),
        mimetype='text/event-stream'
    )

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5500)