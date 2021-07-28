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
        self.shadow_properties = shadow_properties
        self.disconnect_called = False
        self.request_tokens = set()

locked_data = LockedData()

is_prog_done = threading.Event()
 
def on_get_shadow_accepted(response):
    # type: (iotshadow.GetShadowResponse) -> None
    try:
        pStdErr("Finished getting initial shadow state.")
        
        if response.state:
            print("***")
            if response.state.delta:
                print("Delta is {}".format(response.state.delta))
                values = response.state.delta
                print("Values for delta are {}".format(values))
                if values:
                    pStdErr("  Shadow contains delta value '{}'.".format(values))
                    change_shadow_values(values)
                    return

            if response.state.reported:
                print("Reported is {}".format(response.state.reported))
                values = response.state.reported
                print("Values for reported are {}".format(values))
                
                if values:
                    pStdErr("  Shadow contains reported value '{}'.".format(values))
                    set_local_value_due_to_initial_query(values)
                    return
        else:
            print("  Shadow document lacks '{}' properties. Setting defaults...".format(locked_data.shadow_properties))
            change_shadow_values(locked_data.shadow_properties)
            return

    except Exception as e:
        pStdErr(e)

def on_get_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
    try:
        if error.code == 404:
            print("Thing has no shadow document. Creating with defaults...")
            change_shadow_values(locked_data.shadow_properties)
        else:
            exit("Get request was rejected. code:{} message:'{}'".format(
                error.code, error.message))

    except Exception as e:
        pStdErr("Error {} in on_get_shadow_rejected".format(e))

def on_shadow_delta_updated(delta):
    # type: (iotshadow.ShadowDeltaUpdatedEvent) -> None
    try:
        print("Received shadow delta event.")
        print("recieved delta state is {}".format(delta.state))
        properties = {}
        if delta.state:
            for key in delta.state.keys():
                if key in locked_data.shadow_properties.keys():
                    locked_data.shadow_properties[key] = delta.state[key]
                    properties[key] = delta.state[key]
                else:
                    locked_data.shadow_properties[key] = {}
                    properties[key] = {}
                    print("Unknown device value {}...removing".format(key))

            change_shadow_values(properties)
            return
               
        else:
            print("  Delta did not report a change in '{}'".format(shadow_properties))

    except Exception as e:
        pStdErr("Error {} in on_shadow_delta_updated".format(e))

def on_publish_update_shadow(future):
    #type: (Future) -> None
    try:
        future.result()
        print("Update request published.")
    except Exception as e:
        print("Failed to publish update request.")

def on_update_shadow_accepted(response):
    # type: (iotshadow.UpdateShadowResponse) -> None
    try:
        try:
            print("Finished updating reported shadow value to '{}'.".format(response.state.reported)) # type: ignore
        except:
            exit("Updated shadow is missing the target propertie(s).")

    except Exception as e:
        pStdErr("Error {} in on_update_shadow_accepted".format(e))

def on_update_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
        pStdErr("Update request was rejected. code:{} message:'{}'".format(
            error.code, error.message))

def set_local_value_due_to_initial_query(reported_values):
    pStdErr("Setting initial values in set_local_value_due_to_initial_query")
    with locked_data.lock:
        values = {}
        for key in reported_values:
            if locked_data.shadow_properties[key] != reported_values[key]:
                values[key] = reported_values[key]
        if values:
            change_shadow_values(values)

def change_shadow_values(values):
    with locked_data.lock:
        newValues = {}
        for key in values:
            if locked_data.shadow_properties[key] == values[key]:
                print("Local value is already '{}'.".format(values[key]))
            else:
                print("Changed local shadow value to '{}'.".format(values[key]))
                newValues[key] = values[key]
                locked_data.shadow_properties[key] = values[key]

        if not newValues:
            return

        print("Updating reported shadow value to '{}'...".format(values))

        # use a unique token so we can correlate this "request" message to
        # any "response" messages received on the /accepted and /rejected topics
        token = str(uuid4())

        request = iotshadow.UpdateShadowRequest(
            thing_name=thing_name,
            state=iotshadow.ShadowState(
                reported=newValues,
                desired=newValues
            ),
            client_token=token,
        )
        future = shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)

        locked_data.request_tokens.add(token)

        future.add_done_callback(on_publish_update_shadow)
    #updateSettings()

def updateSettings():
    with open(config,"w") as filehandle:
        config.write(filehandle)

send_pour_topic = "send_pour"
config_topic = "config_device"
proxy_options = None
received_count = 0

def flowmeter_input_thread_fn():
    # set up serial port.
    ser = serial.Serial('/dev/serial0',115200, timeout=1)
    ser.flush()

    while True:
        try:
            # get input from serial0. this will signal if a pour starts, and how much was poured.
            serial_input = ''
            new_values = {}
            if ser.in_waiting > 0:
                serial_input = ser.readline().decode('utf-8').rstrip()
            if serial_input:
                if serial_input.startswith("PourTotal:"):
                    pStdErr(serial_input)
                    pourAmount = int(serial_input.replace("PourTotal:",""))
                    new_values = {
                        'pouring' : False,
                        'pour_amount' : pourAmount, 
                        'datetime_of_pour' : time.time() 
                    }
                    print(pourAmount)
                elif serial_input == 'PourStart':
                    new_values = {
                        'pouring' : True,
                        'capturing_image' : True,
                        'pour_id' : str(uuid4())
                    }
                    #callStartCapture()    
                elif serial_input == 'capture_completed':
                    new_values = {
                        'capturing_image' : False,
                    }
                    pass # send image

                    
                change_shadow_values(new_values)
            
            time.sleep(1)
            
        except Exception as e:
            print("Exception on input thread. {}".format(e))
            
if __name__ == '__main__':
    print(endpoint)
    print(cert)
    print(key)
    print(root_ca)
    print(client_id)
    print(thing_name)
    print("starting")
    
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
        # Note that it **is** important to wait for "accepted/rejected" subscriptions
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
        print("Launching thread to flowmeter input...")
        flowmeter_input_thread = threading.Thread(target=flowmeter_input_thread_fn, name='flowmeter_input_thread')
        flowmeter_input_thread.daemon = True
        flowmeter_input_thread.start()
        
        
    except Exception as e:
        pStdErr(e)
    
    is_prog_done.wait()
