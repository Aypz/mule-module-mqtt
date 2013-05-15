# Mule Sample: Integrating with MQTT

## Purpose

In this sample, we'll learn how to publish data to an MQTT topic and how to subscribe to this topic.
More precisely, we're going to broadcast our Mule's JVM memory information to a topic named `/mule/info/memory/${server.host}`,
where `${server.host}` will be Mule's host name.
We will subscribe to all topics below `/mule/info/memory` so we'll be able to get memory information from all the Mules running this sample.

## Prerequisites

In order to follow this sample walk-through you'll need:

- A basic understanding of MQTT. Refer this (protocol's documentation)[http://mqtt.org/documentation] if need be.
- A working installation of a recent version of (Mule Studio)[http://www.mulesoft.org/download-mule-esb-community-edition].

## Building the sample application

### Getting Mule Studio Ready

The MQTT Connector doesn't come bundled with Mule Studio so we have to install it first. For this, we have to do the following:

1. Open Mule Studio and from "Help" menu select "Install New Software...". The installation dialog - shown below - opens.
2. From "Work with" drop down, select "MuleStudio Cloud Connectors Update Site". The list of available connectors will be shown to you.
3. Find and select the MQTT connector in the list of available connectors, the tree structure that is shown.
A faster way to find a specific connector is to filter the list by typing the name of the connector in the input box above the list.
You can choose more than one connector to be installed at once.
4. When you are done selecting the MQTT connector, click on "Next" button.
Installation details are shown on the next page.
Click on "Next" button again and accept the terms of the license agreement.
5. Click on "Finish" button. The MQTT connector is downloaded and installed onto Studio.
You'll need to restart the Studio for the installation to be completed.

![](images/studio-install-connector.png)

### Setting up the project

TBD
