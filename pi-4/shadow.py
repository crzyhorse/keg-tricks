# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import argparse
from awscrt import auth, io, mqtt, http
from awsiot import iotshadow
from awsiot import mqtt_connection_builder
from concurrent.futures import Future
import sys
import threading
import traceback
from uuid import uuid4

# - Overview -
# This sample uses the AWS IoT Device Shadow Service to keep a property in
# sync between device and server. Imagine a light whose color may be changed
# through an app, or set by a local user.
#
# - Instructions -
# Once connected, type a value in the terminal and press Enter to update
# the property's "reported" value. The sample also responds when the "desired"
# value changes on the server. To observe this, edit the Shadow document in
# the AWS Console and set a new "desired" value.
#
# - Detail -
# On startup, the sample requests the shadow document to learn the property's
# initial state. The sample also subscribes to "delta" events from the server,
# which are sent when a property's "desired" value differs from its "reported"
# value. When the sample learns of a new desired value, that value is changed
# on the device and an update is sent to the server with the new "reported"
# value.
class LockedData():
        def __init__(self):
            self.lock = threading.Lock()
            self.shadow_value = None
            self.disconnect_called = False
            self.request_tokens = set()

class deviceShadow():
    is_sample_done = threading.Event()
    mqtt_connection = None
    shadow_client = None
    thing_name = ""
    shadow_property = ""
    SHADOW_VALUE_DEFAULT = "off"


    locked_data = LockedData()

    # Function for gracefully quitting this sample
    def exit(self,msg_or_exception):
        if isinstance(msg_or_exception, Exception):
            print("Exiting sample due to exception.")
            traceback.print_exception(msg_or_exception.__class__, msg_or_exception, sys.exc_info()[2])
        else:
            print("Exiting sample:", msg_or_exception)

        with self.locked_data.lock:
            if not self.locked_data.disconnect_called:
                print("Disconnecting...")
                self.locked_data.disconnect_called = True
                future = self.mqtt_connection.disconnect()
                future.add_done_callback(self.on_disconnected)

    def on_disconnected(self,disconnect_future):
        # type: (Future) -> None
        print("Disconnected.")

        # Signal that sample is finished
        self.is_sample_done.set()


    def on_get_shadow_accepted(self,response):
        # type: (iotshadow.GetShadowResponse) -> None
        try:
            with self.locked_data.lock:
                # check that this is a response to a request from this session
                try:
                    self.locked_data.request_tokens.remove(response.client_token)
                except KeyError:
                    print("Ignoring get_shadow_accepted message due to unexpected token.")
                    return

                print("Finished getting initial shadow state.")
                if self.locked_data.shadow_value is not None:
                    print("  Ignoring initial query because a delta event has already been received.")
                    return

            if response.state:
                if response.state.delta:
                    value = response.state.delta.get(self.shadow_property)
                    if value:
                        print("  Shadow contains delta value '{}'.".format(value))
                        self.change_shadow_value(value)
                        return

                if response.state.reported:
                    value = response.state.reported.get(self.shadow_property)
                    if value:
                        print("  Shadow contains reported value '{}'.".format(value))
                        self.set_local_value_due_to_initial_query(response.state.reported[self.shadow_property])
                        return

            print("  Shadow document lacks '{}' property. Setting defaults...".format(self.shadow_property))
            self.change_shadow_value(self.SHADOW_VALUE_DEFAULT)
            return

        except Exception as e:
            exit(e)

    def on_get_shadow_rejected(self,error):
        # type: (iotshadow.ErrorResponse) -> None
        try:
            # check that this is a response to a request from this session
            with self.locked_data.lock:
                try:
                    self.locked_data.request_tokens.remove(error.client_token)
                except KeyError:
                    print("Ignoring get_shadow_rejected message due to unexpected token.")
                    return

            if error.code == 404:
                print("Thing has no shadow document. Creating with defaults...")
                self.change_shadow_value(self.SHADOW_VALUE_DEFAULT)
            else:
                exit("Get request was rejected. code:{} message:'{}'".format(
                    error.code, error.message))

        except Exception as e:
            exit(e)

    def on_shadow_delta_updated(self,delta):
        # type: (iotshadow.ShadowDeltaUpdatedEvent) -> None
        try:
            print("Received shadow delta event.")
            if delta.state and (self.shadow_property in delta.state):
                value = delta.state[self.shadow_property]
                if value is None:
                    print("  Delta reports that '{}' was deleted. Resetting defaults...".format(self.shadow_property))
                    self.change_shadow_value(self.SHADOW_VALUE_DEFAULT)
                    return
                else:
                    print("  Delta reports that desired value is '{}'. Changing local value...".format(value))
                    self.change_shadow_value(value)
            else:
                print("  Delta did not report a change in '{}'".format(self.shadow_property))

        except Exception as e:
            exit(e)

    def on_publish_update_shadow(self,future):
        #type: (Future) -> None
        try:
            future.result()
            print("Update request published.")
        except Exception as e:
            print("Failed to publish update request.")
            exit(e)

    def on_update_shadow_accepted(self,response):
        # type: (iotshadow.UpdateShadowResponse) -> None
        try:
            # check that this is a response to a request from this session
            with self.locked_data.lock:
                try:
                    self.locked_data.request_tokens.remove(response.client_token)
                except KeyError:
                    print("Ignoring update_shadow_accepted message due to unexpected token.")
                    return

            try:
                print("Finished updating reported shadow value to '{}'.".format(response.state.reported[self.shadow_property])) # type: ignore
                print("Enter desired value: ") # remind user they can input new values
            except:
                exit("Updated shadow is missing the target property.")

        except Exception as e:
            exit(e)

    def on_update_shadow_rejected(self,error):
        # type: (iotshadow.ErrorResponse) -> None
        try:
            # check that this is a response to a request from this session
            with self.locked_data.lock:
                try:
                    self.locked_data.request_tokens.remove(error.client_token)
                except KeyError:
                    print("Ignoring update_shadow_rejected message due to unexpected token.")
                    return

            exit("Update request was rejected. code:{} message:'{}'".format(
                error.code, error.message))

        except Exception as e:
            exit(e)

    def set_local_value_due_to_initial_query(self,reported_value):
        with self.locked_data.lock:
            self.locked_data.shadow_value = reported_value
        print("Enter desired value: ") # remind user they can input new values

    def change_shadow_value(self,value):
        with self.ocked_data.lock:
            if self.locked_data.shadow_value == value:
                print("Local value is already '{}'.".format(value))
                print("Enter desired value: ") # remind user they can input new values
                return

            print("Changed local shadow value to '{}'.".format(value))
            self.locked_data.shadow_value = value

            print("Updating reported shadow value to '{}'...".format(value))

            # use a unique token so we can correlate this "request" message to
            # any "response" messages received on the /accepted and /rejected topics
            token = str(uuid4())

            request = iotshadow.UpdateShadowRequest(
                thing_name=self.thing_name,
                state=iotshadow.ShadowState(
                    reported={ self.shadow_property: value },
                    desired={ self.shadow_property: value },
                ),
                client_token=token,
            )
            future = self.shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)

            self.locked_data.request_tokens.add(token)

            future.add_done_callback(self.on_publish_update_shadow)

    
    def __init__(self,thing_name,shadow_property,log_verbosity,endpoint,root_ca,cert,key,client_id):
        # Process input args
        self.thing_name = thing_name
        self.shadow_property = shadow_property
        io.init_logging(getattr(io.LogLevel, log_verbosity), 'stderr')

        # Spin up resources
        self.event_loop_group = io.EventLoopGroup(1)
        self.host_resolver = io.DefaultHostResolver(self.event_loop_group)
        self.client_bootstrap = io.ClientBootstrap(self.event_loop_group, self.host_resolver)

        self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            cert_filepath=cert,
            pri_key_filepath=key,
            client_bootstrap=self.client_bootstrap,
            ca_filepath=root_ca,
            client_id=client_id,
            clean_session=True,
            keep_alive_secs=6,
            http_proxy_options=None)

        print("Connecting to {} with client ID '{}'...".format(
            endpoint, client_id))

        connected_future = self.mqtt_connection.connect()

        shadow_client = iotshadow.IotShadowClient(self.mqtt_connection)

        # Wait for connection to be fully established.
        # Note that it's not necessary to wait, commands issued to the
        # mqtt_connection before its fully connected will simply be queued.
        # But this sample waits here so it's obvious when a connection
        # fails or succeeds.
        connected_future.result()
        print("Connected!")

        try:
            # Subscribe to necessary topics.
            # Note that is **is** important to wait for "accepted/rejected" subscriptions
            # to succeed before publishing the corresponding "request".
            print("Subscribing to Update responses...")
            update_accepted_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_accepted(
                request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_update_shadow_accepted)

            update_rejected_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_rejected(
                request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_update_shadow_rejected)

            # Wait for subscriptions to succeed
            update_accepted_subscribed_future.result()
            update_rejected_subscribed_future.result()

            print("Subscribing to Get responses...")
            get_accepted_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_accepted(
                request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_get_shadow_accepted)

            get_rejected_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_rejected(
                request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_get_shadow_rejected)

            # Wait for subscriptions to succeed
            get_accepted_subscribed_future.result()
            get_rejected_subscribed_future.result()

            print("Subscribing to Delta events...")
            delta_subscribed_future, _ = shadow_client.subscribe_to_shadow_delta_updated_events(
                request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(thing_name=thing_name),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_shadow_delta_updated)

            # Wait for subscription to succeed
            delta_subscribed_future.result()

        except Exception as e:
            exit(e)

    def getShadowCurrentState(self):
        print("Requesting current shadow state...")

        with self.locked_data.lock:
            # use a unique token so we can correlate this "request" message to
            # any "response" messages received on the /accepted and /rejected topics
            token = str(uuid4())

            publish_get_future = self.shadow_client.publish_get_shadow(
                request=iotshadow.GetShadowRequest(thing_name=self.thing_name, client_token=token),
                qos=mqtt.QoS.AT_LEAST_ONCE)

            self.locked_data.request_tokens.add(token)

        # Ensure that publish succeeds
        publish_get_future.result()


def user_input_thread_fn(deviceShadow):
        while True:
            try:
                # Read user input
                new_value = input()

                # If user wants to quit sample, then quit.
                # Otherwise change the shadow value.
                if new_value in ['exit', 'quit']:
                    exit("User has quit")
                    break
                else:
                    deviceShadow.change_shadow_value(new_value)

            except Exception as e:
                print("Exception on input thread.")
                exit(e)
                break



if __name__ == '__main__':
    
        # The rest of the sample runs asynchronously.

        # Issue request for shadow's current state.
        # The response will be received by the on_get_accepted() callback
        print("Requesting current shadow state...")
        shadow = deviceShadow(thing_name,shadow_property,log_verbosity,endpoint,root_ca,cert,key,client_id) 
        
        # Launch thread to handle user input.
        # A "daemon" thread won't prevent the program from shutting down.
        print("Launching thread to read user input...")
        user_input_thread = threading.Thread(target=user_input_thread_fn, name='user_input_thread')
        user_input_thread.daemon = True
        user_input_thread.start()

        # Wait for the sample to finish (user types 'quit', or an error occurs)
        shadow.is_sample_done.wait()
