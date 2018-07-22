
// Someone set us up the SSD1306 OLED display clone
#include <SSD1306AsciiWire.h>
#include <SSD1306Ascii.h>
#define OLED_I2C_ADDRESS 0x3C
SSD1306AsciiWire display;


#define SERIAL_BAUD 9600
#define LED_PIN 3
#define PULSE_PIN 2

volatile unsigned int pulseCount;
volatile byte ledState = LOW;
volatile bool pouring = false;
unsigned int curPulseCount = 0;
unsigned int lastPulseCount = 0;

void setupDisplay()
{
	display.begin(&Adafruit128x32, OLED_I2C_ADDRESS);
	display.set400kHz();
	display.setFont(Adafruit5x7);
	display.clear();
	display.setCursor(0, 0);
	display.println("Waiting for someone");
	display.println("to drink beer :(");
}

void setup()
{
	Serial.begin(SERIAL_BAUD);
  pinMode(LED_PIN, OUTPUT);
  
  // attach an interrupt to our SF800 connected pin that triggers on FALLING state
  pinMode(PULSE_PIN, INPUT);
  digitalWrite(PULSE_PIN,HIGH);
  attachInterrupt(digitalPinToInterrupt(PULSE_PIN),incFlow,FALLING);

  // configure our OLED
  delay(1000);
  setupDisplay();
}

void incFlow() {
  // increment count from SF800
  pulseCount++;
  // set state for LED...it pulses so fast that we'll just keep flipping it while
  // the flow is occuring.
  ledState=!ledState;
  digitalWrite(LED_PIN, ledState);
  pouring = true;
}

void sendPulseCount(unsigned int pulses)
// currently just prints to the OLED screen. but eventually we will transmit to 
// raspberry PI for more complicated stuff (we'll be a Raspberry Hat)
{
    display.clear();
    display.println("Transmitting Pour");
    display.println("information.");
    float ounces = ((pulses/5.600)/29.57); // I understand that doing floating point math on 
                                           // this Atmel is not a good idea. 
                                           // Will transmit pulses only in the release
                                           // version. This is just for placeholder.
    display.println("Transmitted " + String(pulses) + " = " + String(ounces));
}

void loop() {
  // turn off interrupts to safely get the current counter.
  noInterrupts();
  curPulseCount = pulseCount;
  interrupts();
 
  // if pulse count has changed AND been stopped for longer than loop time 
  if ((curPulseCount !=0) && (lastPulseCount == curPulseCount)) {
    // if the LED is on, let's turn it off.
    if (ledState==HIGH) {
      digitalWrite(LED_PIN, LOW);
    }
    // transmit the data.
    sendPulseCount(curPulseCount);

    // turn off interrupts to safely clear the counter
    noInterrupts();
    pulseCount = 0;
    pouring = false;
    interrupts();
    lastPulseCount=0;
    curPulseCount = 0;
    
  } else {
    // TO DO: clean this up so we only clear and redraw if something changed.
    // Probably also want to see the amount poured for a while.
    if (pouring) {
      display.clear();
      display.print("Pouring...Yay Beer!");
    } else {
      display.clear();
      display.println("Waiting for someone");
      display.println("to drink beer :(");
    }
  }
  lastPulseCount = curPulseCount;
  delay(1500);
} 


