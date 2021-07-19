import serial
import json
import time
import configparser
import sys
import threading
import traceback
from uuid import uuid4

from awscrt import io, mqtt
from awsiot import iotshadow
from awsiot import mqtt_connection_builder
from concurrent.futures import Future

# Shadow JSON schema:
#
# {
#   "state": {
#       "desired":{
#           "pouring":<BOOL VALUE>,
#           "capturing_image":<BOOL VALUE>,
#           "pulsesperunit" : <INT VALUE>,
#           "units" : <STRING VALUE>
#           "stealth" : <BOOL VALUE>,
#           "pour_id" : <STRING VALUE>,
#           "pour_amount" : <INT VALUE>,
#           "keg_temp" : <INT VALUE>           
#       }
#   }
# }

mqtt_connection = None
shadow_client = None

# get current device values
# first instantiate ini parser
config = configparser.ConfigParser()

# next parse the config file
# expects a file called config.ini
config.read('config.ini')

# there MUST be an [AWS] section with the path to your certs, your device endpoint, and port as below.
root_ca = config.get('AWS', 'root_ca') #path to your AWS Root_CA certificate.
cert = config.get('AWS', 'cert') #path your device (thing) certificate
key = config.get('AWS', 'key') #path to your device (thing) private key
endpoint= config.get('AWS', 'endpoint') #your device (thing) endpoint
port = config.getint('AWS', "port",fallback=8883) #port to use. supports 443 and 8883

# there can be a [Keg] section with your device defaults. if they don't exist, it will use defaults.
stealth = config.getboolean('Keg', 'stealth', fallback = False) #puts kegerator in stealth mode. default false.
units = config.get('Keg', 'units', fallback = "metric") #metric or imperial (ounces or millilitres)
pulsesperlitre = config.getint('Keg','pulsesperlitre', fallback=5600)#default for a swissflow 800 is 5600 pulses per litre for water.

#with the exception of the thing name and the device UUID. these can't have defaults. 
#use the name of the thing you created in AWS for thing name.
#Use UUID4() to generate a device_uuid if needed.
thing_name = config.get('Keg','thing_name')
device_uuid = config.get('Keg','device_uuid')

#set device current state after reading config file.
#pouring and capturing image will always be false unless input comes in from the meter
#pour_id will be empty string until pouring starts, and pour_amount will be 0 until pour completes
shadow_properties = {
                        "pulsesperlitre" : pulsesperlitre,
                        "units" : units,
                        "stealth" : stealth,
                        "pouring": False,
                        "capturing_image": False,
                        "pour_id" : "",
                        "pour_amount" : 0 
                    }

client_id='keg-'+device_uuid

# helper function for logging to std.err
def pStdErr(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class LockedData:
    def __init__(self):
        self.lock = threading.Lock()
        self.shadow_value = None
        self.disconnect_called = False
        self.request_tokens = set()

locked_data = LockedData()
"""
def exit(msg_or_exception):
    if isinstance(msg_or_exception, Exception):
        stdErr("Exiting sample due to exception.")
        traceback.print_exception(msg_or_exception.__class__, msg_or_exception, sys.exc_info()[2])
    else:
        print("Exiting sample:", msg_or_exception)

    with locked_data.lock:
        if not locked_data.disconnect_called:
            print("Disconnecting...")
            locked_data.disconnect_called = True
            future = mqtt_connection.disconnect()
            future.add_done_callback(on_disconnected)

def on_disconnected(disconnect_future):
    # type: (Future) -> None
    print("Disconnected.")

    # Signal that sample is finished
    is_sample_done.set()
"""
 
def on_get_shadow_accepted(response):
    # type: (iotshadow.GetShadowResponse) -> None
    try:
        with locked_data.lock:
            # check that this is a response to a request from this session
            try:
                locked_data.request_tokens.remove(response.client_token)
            except KeyError:
                pStdErr("Ignoring get_shadow_accepted message due to unexpected token.")
                return

            pStdErr("Finished getting initial shadow state.")
            if locked_data.shadow_value is not None:
                pStdErr("Ignoring initial query because a delta event has already been received.")
                return

        if response.state:
            if response.state.delta:
                value = response.state.delta.get(response.state)
                if value:
                    pStdErr("  Shadow contains delta value '{}'.".format(value))
                    change_shadow_value(value)
                    return

            if response.state.reported:
                value = response.state.reported.get(response.state)
                if value:
                    pStdErr("  Shadow contains reported value '{}'.".format(value))
                    set_local_value_due_to_initial_query(response.state.reported)
                    return

        print("  Shadow document lacks '{}' property. Setting defaults...".format(shadow_properties))
        change_shadow_value(shadow_properties)
        return

    except Exception as e:
        pStdErr(e)

def on_get_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(error.client_token)
            except KeyError:
                print("Ignoring get_shadow_rejected message due to unexpected token.")
                return

        if error.code == 404:
            print("Thing has no shadow document. Creating with defaults...")
            change_shadow_value(shadow_properties)
        else:
            exit("Get request was rejected. code:{} message:'{}'".format(
                error.code, error.message))

    except Exception as e:
        exit(e)

def on_shadow_delta_updated(delta):
    # type: (iotshadow.ShadowDeltaUpdatedEvent) -> None
    try:
        print("Received shadow delta event.")
        print("recieved delta state is {}".format(delta.state))
        if delta.state:
            for key in delta.state.keys():
                if key in shadow_properties.keys():
                    shadow_properties[key] = delta.state[key]
                else:
                    shadow_properties[key] = {}
                    print("Unknown device value {}...removing".format(key))

            change_shadow_value(shadow_properties)
            return
               
        else:
            print("  Delta did not report a change in '{}'".format(shadow_properties))

    except Exception as e:
        exit(e)

def on_publish_update_shadow(future):
    #type: (Future) -> None
    try:
        future.result()
        print("Update request published.")
    except Exception as e:
        print("Failed to publish update request.")
        exit(e)

def on_update_shadow_accepted(response):
    # type: (iotshadow.UpdateShadowResponse) -> None
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(response.client_token)
            except KeyError:
                print("Ignoring update_shadow_accepted message due to unexpected token.")
                return

        try:
            print("Finished updating reported shadow value to '{}'.".format(response.state.reported)) # type: ignore
        except:
            exit("Updated shadow is missing the target property.")

    except Exception as e:
        exit(e)

def on_update_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(error.client_token)
            except KeyError:
                print("Ignoring update_shadow_rejected message due to unexpected token.")
                return

        exit("Update request was rejected. code:{} message:'{}'".format(
            error.code, error.message))

    except Exception as e:
        exit(e)

def set_local_value_due_to_initial_query(reported_value):
    with locked_data.lock:
        locked_data.shadow_value = reported_value

def change_shadow_value(value):
    with locked_data.lock:
        print(value)
        if locked_data.shadow_value == value:
            print("Local value is already '{}'.".format(value))
            return

        print("Changed local shadow value to '{}'.".format(value))
        locked_data.shadow_value = value

        print("Updating reported shadow value to '{}'...".format(value))

        # use a unique token so we can correlate this "request" message to
        # any "response" messages received on the /accepted and /rejected topics
        token = str(uuid4())

        request = iotshadow.UpdateShadowRequest(
            thing_name=thing_name,
            state=iotshadow.ShadowState(
                reported=shadow_properties,
                desired=shadow_properties,
            ),
            client_token=token,
        )
        future = shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)

        locked_data.request_tokens.add(token)

        future.add_done_callback(on_publish_update_shadow)

send_pour_topic = "send_pour"
config_topic = "config_device"
proxy_options = None
received_count = 0


# updates our device config if any updates come in from the UX (UX will be Alexa)
def updateConfig():
    global config
    with open(config,"w") as filehandle:
        config.write(filehandle)

# update units if any updates come in from the UX (Alexa)
def changeUnits(_units):
    global shadow_properties
    global units
    units = _units
    shadow_properties['units'] = units
    updateConfig()

# update pulses per litre if any updates come in from the UX (Alexa)
def changePulses(_pulsesperlitre):
    global shadow_properties
    global pulsesperlitre
    pulsesperlitre = _pulsesperlitre
    shadow_properties['pulsesperlitre'] = pulsesperlitre
    updateConfig()

# update setting per stealth mode if any updates come in from the UX (Alexa)
def setStealth(_stealth):
    global stealth
    global shadow_properties
    stealth = _stealth
    shadow_properties['stealth']=stealth
    updateConfig()

if __name__ == '__main__':
    print(endpoint)
    print(cert)
    print(key)
    print(root_ca)
    print(client_id)
    print(thing_name)
    print("starting")

    # set up serial port.
    ser = serial.Serial('/dev/serial0',115200, timeout=1)
    ser.flush()
    
    # start AWS IOT logging to stderr
    # options are NoLogs, Fatal, Error, Warn, Info, Debug, Trace
    io.init_logging(io.LogLevel.Error, 'stderr')

    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=cert,
        pri_key_filepath=key,
        client_bootstrap=client_bootstrap,
        ca_filepath=root_ca,
        client_id=client_id,
        clean_session=True,
        keep_alive_secs=6,
        http_proxy_options=None)

    pStdErr("Connecting to {} with client ID '{}'...".format(
        endpoint, client_id))

    connected_future = mqtt_connection.connect()

    shadow_client = iotshadow.IotShadowClient(mqtt_connection)

    # Wait for connection to be fully established.
    connected_future.result()
    pStdErr("Connected to {}!".format(endpoint))

    try:
        # Subscribe to necessary topics.
        # Note that is **is** important to wait for "accepted/rejected" subscriptions
        # to succeed before publishing the corresponding "request".
        pStdErr("Subscribing to {} Shadow Update responses...".format(thing_name))
        update_accepted_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_accepted(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_accepted)

        update_rejected_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_rejected(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_rejected)

        # Wait for subscriptions to succeed
        update_accepted_subscribed_future.result()
        update_rejected_subscribed_future.result()

        pStdErr("Subscribing to {} Shadow Get responses...".format(thing_name))
        get_accepted_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_accepted(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_accepted)

        get_rejected_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_rejected(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_rejected)

        # Wait for subscriptions to succeed
        get_accepted_subscribed_future.result()
        get_rejected_subscribed_future.result()

        pStdErr("Subscribing to {} Shadow Delta events...".format(thing_name))
        delta_subscribed_future, _ = shadow_client.subscribe_to_shadow_delta_updated_events(
            request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_shadow_delta_updated)

        # Wait for subscription to succeed
        delta_subscribed_future.result()


        # Issue request for shadow's current state.
        # The response will be received by the on_get_accepted() callback
        pStdErr("Requesting current {} shadow state...".format(thing_name))

        with locked_data.lock:
            # use a unique token so we can correlate this "request" message to
            # any "response" messages received on the /accepted and /rejected topics
            token = str(uuid4())

            publish_get_future = shadow_client.publish_get_shadow(
                request=iotshadow.GetShadowRequest(thing_name=thing_name, client_token=token),
                qos=mqtt.QoS.AT_LEAST_ONCE)

            locked_data.request_tokens.add(token)

        # Ensure that publish succeeds
        publish_get_future.result()

        #start our monitor loops
        while True:
        
            # get input from serial0. this will signal if a pour starts, and how much was poured.
            serial_input = ''
            if ser.in_waiting > 0:
                serial_input = ser.readline().decode('utf-8').rstrip()
                print(serial_input)
            
            time.sleep(1)

    except Exception as e:
        pStdErr(e.msg)
