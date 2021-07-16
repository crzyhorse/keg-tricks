import serial
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
from uuid import uuid4
import json
import time
import configparser

topic = "topic_1"
client_id = "test-" + str(uuid4())
port = None
proxy_options = None


# instantiate
config = configparser.ConfigParser()

# parse config file
# expects a [AWS] section with the path to your certs and your device endpoint as below.
config.read('config.ini')
root_ca = config.get('AWS', 'root_ca')
cert = config.get('AWS', 'cert')
key = config.get('AWS', 'key')
endpoint= config.get('AWS', 'endpoint')

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))


if __name__ == '__main__':


    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            port=port,
            cert_filepath=cert,
            pri_key_filepath=key,
            client_bootstrap=client_bootstrap,
            ca_filepath=root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=6,
            http_proxy_options=proxy_options)

    print("Connecting to {} with client ID '{}'...".format(
        endpoint, client_id))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Subscribe
    #print("Subscribing to topic '{}'...".format(args.topic))
    #subscribe_future, packet_id = mqtt_connection.subscribe(
    #    topic=topic,
    #    qos=mqtt.QoS.AT_LEAST_ONCE,
    #    callback=on_message_received)

    #subscribe_result = subscribe_future.result()
    #print("Subscribed with {}".format(str(subscribe_result['qos'])))

    
    publish_count = 1
    while publish_count <= 5:
        message = "{} [{}]".format("Test message from PI4", publish_count)
        print("Publishing message to topic '{}': {}".format(topic, message))
        message_json = json.dumps(message)
        mqtt_connection.publish(
            topic=topic,
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(1)
        publish_count += 1

        ser = serial.Serial('/dev/serial0',115200, timeout=1)
        ser.flush()
    
    #while True:

        if ser.in_waiting > 0:
            line = ser.readline().decode('utf-8').rstrip()
            print(line)
