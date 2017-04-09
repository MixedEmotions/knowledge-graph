# MixedEmotions' knowledge-graph

## Description

Creates knowledge graph from information processed by "Entity Extraction and Linking" module, and "Emotion Recognition from Text" module

This MixedEmotions Orchestratoknowledge-graph was developed by NUIG.

The code of this orchestrator will let users have an starting point on how to interact with the MixedEmotion Toolbox modules. It is written in scala and can interact with RESTservices and DockerServices deployed in Mesos with a Mesos-DNS as a discovery service. The orchestrator will execute input documents in a pipeline with the defined modules.

## REQUIREMENTS

This package requires python3.5.
Python libraries:
elasticsearch
Flask

DBpedia dumps from http://wiki.dbpedia.org/downloads-2016-04
Minimal list of required files:
infobox_properties_en.ttl
instance_types_en.ttl
persondata_en.ttl


## USAGE

Type  http://0.0.0.0:5000/text=give your sentence here 

in your browser, where text can be a sentence or a tweet. 
The service should return 'suggestion' or 'non-suggestion'

## CREDITS (citations if available)

The neural network based classifier has been implemented using the deep learning library KERAS [site](https://keras.io).


## ACKNOWLEDGEMENT

This development has been partially funded by the European Union through the MixedEmotions Project (project number H2020 655632), as part of the `RIA ICT 15 Big data and Open Data Innovation and take-up` programme.

![MixedEmotions](https://raw.githubusercontent.com/MixedEmotions/MixedEmotions/master/img/me.png) 

![EU](https://raw.githubusercontent.com/MixedEmotions/MixedEmotions/master/img/H2020-Web.png)

http://ec.europa.eu/research/participants/portal/desktop/en/opportunities/index.html
