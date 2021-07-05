from machine import Pin
import utime

#define hardware
led = Pin(25, Pin.OUT)
swissflow = Pin(2,Pin.IN)

#define variables
pouring = False
curPulseCount = 0
lastPulseCount = 0
pulseCount = 0


# Our interuppt handler. we will increment our pulse counter and blink our led on detection of flow
def irq_handler(pin):
    global pulseCount
    global pouring
    led.toggle()
    pulseCount += 1
    pouring = True


#set up hardware state and define interrupts
led.low()
swissflow.irq(trigger=Pin.IRQ_RISING,handler=irq_handler)


# our main loop.

while True:
    
    # turn off interupt and get the current pulsecount
    swissflow.irq(handler=None)
    curPulseCount = pulseCount
    swissflow.irq(handler=irq_handler)
    
    # if pulse count has changed AND been stopped for longer than loop time 
    if (curPulseCount !=0) and (lastPulseCount == curPulseCount):
        # if the LED is on, let's turn it off.
        if led.value() == 1:
            led.low()
        
        # transmit the data. right now just printing it.
        print(curPulseCount)
        
        # turn off interupts and clear the counter
        swissflow.irq(handler=None)
        pulseCount = 0
        pouring = False
        swissflow.irq(handler=irq_handler)
        lastPulseCount= 0
        curPulseCount = 0
        
    lastPulseCount = curPulseCount
    utime.sleep(1)