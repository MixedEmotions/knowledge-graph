# MixedEmotions' knowledge-graph

## Description

Creates Knowledge Graph from information processed by  ["Entity Extraction and Linking"](https://github.com/MixedEmotions/entity-linking), and "Emotion Recognition from Text" module

This MixedEmotions Knowledge Graph was developed by NUIG.

Knowledge Graph provides insight into relations between recognized entities using semantic knowledge from DBpedia. KG module uses entities that are recognised by  ["Entity Extraction and Linking"](https://github.com/MixedEmotions/entity-linking) module, and extracts relations between the entities from DBpedia. Once the relations are extracted and filtered, they are stored in Elasticsearch database, where using Kibi they are visualized.

## REQUIREMENTS

This package requires python3.5.

Python libraries:
* elasticsearch
* Flask

DBpedia dumps from http://wiki.dbpedia.org/downloads-2016-04

Minimal list of required files:
* infobox_properties_en.ttl
* instance_types_en.ttl
* persondata_en.ttl


Elasticsearch: 2.4.1

Kibi: [kibi-enterprise-standard-4.6.3-2](https://siren.solutions/kibi/enterprise/)




## USAGE


Type  http://localhost:5000/

| Description | API call |
| ------------- |:-------------:|
| Check default configuration | GET	 /configuration |
| Modify the configuration | POST	 /configuration |
| Reset back to default configuration | GET	 /reset | 
| Get status of the module | GET	/status | 
| Create the Knowledge Graph | GET 	/start |




## Usage example

Kibi already has to contain  the source index pattern. In our example **trump_tweets**.

trump_tweets: index on elasticsearch that contains data processed by ["Entity Extraction and Linking"](https://github.com/MixedEmotions/entity-linking), ["Emotion Recognition from Text"] module, and has a field ***text*** (that contains original text which was processed). 

### Submit credentials

http://localhost:5000/configuration PUT

```JSON
{
"credentials": {
"elasticHost": "localhost",
"elasticPassword": "changeme",
"elasticPort": 9220,
"elasticUsername": "elastic"
},
  "variables": {
"inputIndexName": "trump_tweets",
"inputIndexType": "text_review"
}

}
```


### Start Creation of graph

http://localhost:5000/start GET

### Monitor progress 
http://localhost:5000/status GET

### View graph in Kibi
http://localhost:5606/app/kibana#/dashboard/Graph


## ACKNOWLEDGEMENT

This development has been partially funded by the European Union through the MixedEmotions Project (project number H2020 655632), as part of the `RIA ICT 15 Big data and Open Data Innovation and take-up` programme.

![MixedEmotions](https://raw.githubusercontent.com/MixedEmotions/MixedEmotions/master/img/me.png) 

![EU](https://raw.githubusercontent.com/MixedEmotions/MixedEmotions/master/img/H2020-Web.png)

http://ec.europa.eu/research/participants/portal/desktop/en/opportunities/index.html
