from datetime import datetime
from elasticsearch import Elasticsearch
import os
import subprocess
from collections import Counter
from elasticsearch.helpers import bulk
import itertools
import operator
import json
import logging
import copy

from flask import Flask, request, jsonify,make_response
from threading import Thread

def getSourceMapping(typeName='text_review',indexName='trump_tweets', elasticPort=9220, elasticHost="localhost"):
    es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}])
    mapping = es.indices.get_mapping(index=indexName,doc_type=typeName)
    return mapping[indexName]["mappings"][typeName] 

def setMapping(mapping,typeName='text_review',indexName='tweets', elasticPort=9220, elasticHost="localhost"):
    es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}])
    result = es.indices.create(index=indexName, ignore=400, body={})
    result = es.indices.put_mapping(index=indexName, doc_type=typeName, body=mapping)
    return result 

def getDatasets( typeName='text_review',indexName='trump_tweets', elasticPort=9220, elasticHost="localhost"):
	es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
	results=[]
	global loging
	loging.append("Getting datasets") 
	esSearchResult = es.search(
				index=indexName,
				search_type='scan',
				doc_type=typeName,
				scroll = '2m',
				size=1000,
				request_timeout=1060,
				body={"fields" : ["entity_linking.URI", "entity_linking.EntityType"],
				'query': {
					'filtered': {
					  'query': {
						'match_all': {}
					  }
					}
				  }
				}
			)
	sid = esSearchResult['_scroll_id']
	scroll_size = esSearchResult['hits']['total']
  
  # Start scrolling
	while (scroll_size > 0):
		page = es.scroll(scroll_id = sid, scroll = '2m')
		# Update the scroll ID
		sid = page['_scroll_id']
		# Get the number of results that we returned in the last scroll
		scroll_size = len(page['hits']['hits'])
	#	print(page["hits"]["hits"])
		results.extend([(dataset["fields"]["entity_linking.EntityType"][0],dataset["fields"]["entity_linking.URI"][0].replace("dbpedia.org/page","dbpedia.org/resource")) for dataset in page["hits"]["hits"] if "fields" in dataset])
	return set(results)

def cloneTweets(indexName, typeName="text_review", elasticPort=9220, elasticHost="localhost"):
	# delete old tweet index index 
	deleteIndex( ["tweets"], elasticPort=elasticPort, elasticHost=elasticHost)
	loging.append("deleted index "+"tweets")
	loging.append("filtering tweets  to index "+"tweets")
	mapping=getSourceMapping(typeName=typeName, indexName=indexName, elasticPort=elasticPort, elasticHost=elasticHost)
	setMapping(mapping,typeName='text_review',indexName='tweets', elasticPort=elasticPort, elasticHost=elasticHost)
	es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}])
	temRecordES=[]
	
	# search for tweets that are NOT retweets
	esSearchResult = es.search(
				index=indexName,
				search_type='scan',
				doc_type=typeName,
				scroll = '2m',
				size=1000,
				request_timeout=1060,
				body={'query': {
					'filtered': {
					  "filter" : {
							"missing" : { "field" : "retweeted_status.text" }
						 }
					}
				  }
                }
            )
	sid = esSearchResult['_scroll_id']
	scroll_size = esSearchResult['hits']['total']
  
  # Start scrolling
	while (scroll_size > 0):
		page = es.scroll(scroll_id = sid, scroll = '2m')
		# Update the scroll ID
		sid = page['_scroll_id']
		# Get the number of results that we returned in the last scroll
		scroll_size = len(page['hits']['hits'])
#        print "scroll size: " + str(scroll_size)
		# Do something with the obtained page
		#print(page.get('hits').get('hits'))
		for terms in page.get('hits').get('hits'):
			tmp = terms.get('_source')
		 #   del tmp['raw']
			#print (terms)
			if tmp:
				temRecordES.append(tmp)
			if len(temRecordES) > 10000:
				write2EStweets(temRecordES, elasticPort=elasticPort, elasticHost=elasticHost)
				temRecordES=[]
	write2EStweets(temRecordES, elasticPort=elasticPort, elasticHost=elasticHost)
	loging.append("Finished filtering tweets to index "+"tweets")

def write2EStweets(AllData, indexName="tweets", typeName="text_review",elasticPort=9220, elasticHost="localhost"): 
	es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}])
	messages = []
	#print(AllData)
	for record in AllData:
		#print(record)
		tmpMap = {"_op_type": "index", "_index": indexName, "_type": typeName}
		tmpMap.update(record)
		messages.append(tmpMap)
	#print (len(messages))
	result = bulk(es, messages)
	return result



def preSetES(indexName, typeName , elasticPort=9220, elasticHost="localhost"):
		logging.info('Running preset for ', indexName)
		 
		print("Running preset for ", indexName)
		global loging
		loging.append("Running preset for "+ indexName)
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))  # 8018
		result = es.indices.create(index=indexName, ignore=400, body={
				"settings": {
			"analysis":{ "filter": {
				"my_length": {
					"type":       "length",
					"min": 3 
				},
				"my_stop":{"type":"stop",
				"stopwords" : "_english_"}
			}, "tokenizer":{
			"my_tokenizer":{"type": "pattern",
			 "pattern": "\\s+"}
			} ,
						"analyzer" : {
						"my_analyzer" : {
						"type": "custom",
						 "tokenizer": "my_tokenizer",
												"filter" : ["my_length","my_stop"],
												"char_filter" : "html_strip"    }
												 }          }}})
		logging.debug("Creating index ",result)
		dynamicMapping = {typeName: {"date_detection": "true"}}
		customMapping = {
				typeName: {
						"date_detection": "false",
						"dynamic_templates": [
								{"source": {
											"match": "source",
											"match_mapping_type": "string",
											"mapping": {
													"type": "string",
													"index": "not_analyzed"
											}
								}},{"target": {
											"match": "target",
											"match_mapping_type": "string",
											"mapping": {
													"type": "string",
													"index": "not_analyzed"
													
											}}},
									{"connection": {
											"match": "connection",
											"match_mapping_type": "string",
											"mapping": {
													"type": "string",
													"index": "not_analyzed"
													
											}}},
											{"entity": {
											"match": "entity",
											"match_mapping_type": "string",
											"mapping": {
													"type": "string",
													"index": "not_analyzed"
													
											}}},
								{"rest": {
											"match": "*",
											"match_mapping_type": "string",
											"mapping": {
													"type": "string",
													"index": "not_analyzed"
													
											}
								}}
						]}
				}
		result = es.indices.put_mapping(index=indexName, doc_type=typeName, body=customMapping)
		logging.debug("Creating mapping ",result)

def write2ES(AllData, indexName, typeName , elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		messages = []
		logging.debug('Running preset')
		for record in AllData:
			#	print(record)
				tmpMap = {"_op_type": "index", "_index": indexName, "_type": typeName }
				if len(record)>2:
					tmpMap.update({"label":record[0].replace("http://dbpedia.org/resource/",""), "entity_linking":{"URI":record[0].replace("/resource/","/page/"),"connection":record[1] ,"target":record[2].replace("/resource/","/page/")}})
				else:
					tmpMap.update({"label":record[0].replace("http://dbpedia.org/resource/",""), "entity_linking":{"URI":record[0].replace("/resource/","/page/"),"target":record[1]}})
				messages.append(tmpMap)
		#print messages
		result = bulk(es, messages)
		return result

def getKibiRelationConfig(indexName=".kibi", typeName="config" , elasticPort=9220, elasticHost="localhost"):
	es =  Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
	mapping = es.search(
				index=indexName,
				doc_type=typeName,
				size=1000,
				request_timeout=1060,
				body={
				'query': {
					'filtered': {
					  'query': {
						'match_all': {}
					  }
					}
				  }
				}
			)
	return mapping['hits']['hits'][0]
	#return mapping[SourceIndexName]["mappings"][SourceTypeName] 

def createVisualizations(nameList, elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		tweetsIndex =nameList[len(nameList)-1] #"trump_tweets" # index containing tweets, emotions.emotion field and   entity_linking.URI
		messages = []
		print("creating visualizations ")
		# Emotion Distribution visualisation 
		emotionDistribution ="EmotionDistribution"
		tmpMap = { "_index": ".kibi", "_type": "visualization", "_id": emotionDistribution, "_score": 1}
		tmpMap.update( {
		  "title": emotionDistribution,
		  "visState": "{\"title\":\""+emotionDistribution+"\",\"type\":\"pie\",\"params\":{\"addLegend\":true,\"addTooltip\":true,\"isDonut\":false,\"shareYAxis\":true},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"type\":\"terms\",\"schema\":\"segment\",\"params\":{\"field\":\"emotions.emotion\",\"size\":10,\"order\":\"desc\",\"orderBy\":\"1\"}}],\"listeners\":{}}",
		  "uiStateJSON": "{}",
		  "description": "",
		  "version": 1,
		  "kibanaSavedObjectMeta": { "searchSourceJSON": "{\"index\":\""+tweetsIndex+"\",\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[]}"  }
		 })
		messages.append(tmpMap)
		#PercentageEmotions visualization
		percentageEmotions = "PercentageEmotions"
		tmpMap ={  "_index": ".kibi", "_type": "visualization", "_id": percentageEmotions, "_score": 1}
		tmpMap.update( {
		  "description": "",
		  "kibanaSavedObjectMeta": { "searchSourceJSON": "{\"index\":\""+tweetsIndex+"\",\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}},\"filter\":[]}" },
		  "title": percentageEmotions,
		  "uiStateJSON": "{}",
		  "version": 1,
		  "visState": "{\"title\":\""+percentageEmotions+"\",\"type\":\"histogram\",\"params\":{\"shareYAxis\":true,\"addTooltip\":true,\"addLegend\":true,\"scale\":\"linear\",\"mode\":\"percentage\",\"times\":[],\"addTimeMarker\":false,\"defaultYExtents\":false,\"setYExtents\":false,\"yAxis\":{}},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"created_at\",\"interval\":\"d\",\"customInterval\":\"2h\",\"min_doc_count\":1,\"extended_bounds\":{}}},{\"id\":\"3\",\"type\":\"terms\",\"schema\":\"group\",\"params\":{\"field\":\"emotions.emotion\",\"size\":10,\"order\":\"desc\",\"orderBy\":\"1\"}}],\"listeners\":{}}"
		})
		messages.append(tmpMap)
		#create Location Clouds
		for name in nameList[:-1]:
			cloudName = name[:name.find("_")]+"Cloud"
			tmpMap ={ "_index": ".kibi", "_type": "visualization", "_id": cloudName, "_score": 1}
			tmpMap.update({
			  "description": "",
			  "kibanaSavedObjectMeta": { "searchSourceJSON": "{\"index\":\""+name+"\",\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}},\"filter\":[]}"},
			  "title": cloudName,
			  "uiStateJSON": "{}",
			  "version": 1,
			  "visState": "{\"title\":\""+cloudName+"\",\"type\":\"kibi_wordcloud\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"entity_linking.URI\",\"size\":10,\"order\":\"desc\",\"orderBy\":\"1\"}}],\"listeners\":{}}"
			})
			messages.append(tmpMap)

		#create graph visualisation 
		tmpMap ={  "_index": ".kibi", "_type": "visualization", "_id": "Graph", "_score": 1}
		tmpMap.update( {
		  "visState": "{\"title\":\"Graph\",\"type\":\"kibi_graph_browser\",\"params\":{\"vertices\":[{\"indexPattern\":\"person_unique\",\"indexPatternType\":\"person_type\",\"iconType\":\"fontawesome\",\"labelType\":\"docField\",\"fwicon\":\"fa-user\",\"sourceField\":\"label\"},{\"indexPattern\":\"organization_unique\",\"indexPatternType\":\"organization_type\",\"iconType\":\"fontawesome\",\"labelType\":\"docField\",\"fwicon\":\"fa-university\",\"sourceField\":\"label\"},{\"indexPattern\":\"location_unique\",\"indexPatternType\":\"location_type\",\"iconType\":\"fontawesome\",\"labelType\":\"docField\",\"fwicon\":\"fa-map-marker\",\"sourceField\":\"label\"},{\"indexPattern\":\"tweets\",\"indexPatternType\":\"text_review\",\"iconType\":\"fontawesome\",\"labelType\":\"docField\",\"fwicon\":\"fa-comment-o\",\"sourceField\":\"emotions.emotion\"}],\"queryOption\":{\"queryId\":\"Kibi-Graph-Query\",\"datasourceId\":\"Kibi-Gremlin-Server\"},\"contextualScripts\":[],\"onUpdateScripts\":[],\"expansionScript\":\"Default-Expansion-Policy\",\"addTooltip\":false},\"aggs\":[],\"listeners\":{},\"version\":2}",
		  "uiStateJSON": "{}",
		  "description": "",
		  "title": "Graph",
		  "kibanaSavedObjectMeta": { "searchSourceJSON": "{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[]}" }
		})
		messages.append(tmpMap)

		# create Graph dashboard
		tmpMap ={  "_index": ".kibi", "_type": "dashboard", "_id": "Graph", "_score": 1}
		tmpMap.update( {
		  "title": "Graph",
		  "hits": 0,
		  "description": "",
		  "panelsJSON": "[{\"id\":\"Graph\",\"type\":\"visualization\",\"panelIndex\":1,\"size_x\":12,\"size_y\":8,\"col\":1,\"row\":1}]",
		  "optionsJSON": "{\"darkTheme\":false}",
		  "uiStateJSON": "{}",
		  "version": 1,
		  "timeRestore": "false",
		  "savedSearchId": "",
		  "kibanaSavedObjectMeta": {"searchSourceJSON": "{\"filter\":[{\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}}}]}" }
		})
		messages.append(tmpMap)
		print (messages)
		result = bulk(es, messages)
		return result

def writeSearch2ES(name, indexName=".kibi", typeName="search" , elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		messages = []
		logging.debug('Writing Search')
		print('Writing Search')
		tmpMap = {"_op_type": "index", "_index": indexName, "_type": typeName, "_id":name }
		tmpMap.update({"sort": ["_score", "desc" ], "version": 1,"description": "", "hits": 0})
		if name.find("tweets")>= 0:
			tmpMap.update({ "title":name, "columns":["emotions.emotion", "detected_entities", "text"]})
		elif name.find("_unique")>= 0:
			tmpMap.update({ "title":name, "columns":["label", "entity_linking.target"]})
		else:
			tmpMap.update({ "title":name, "columns":["label","entity_linking.connection" ,"entity_linking.target"]})
		tmpMap.update({"kibanaSavedObjectMeta": { "searchSourceJSON": "{\"index\":\""+name+"\",\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[],\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"require_field_match\":false,\"fragment_size\":2147483647}}"}})
		
				
		messages.append(tmpMap)
		print (messages)
		result = bulk(es, messages)
		return result

def writeDashboard2ES(name, indexName=".kibi", typeName="dashboard" , elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		messages = []
		PercentageEmotions = "PercentageEmotions"
		logging.debug('Writing dashboard')
		print('Writing dashboard')
		PercentageEmotions = "PercentageEmotions"
		EmotionDistribution = "EmotionDistribution"
		cloudVisualization= name[:name.find("_")]+"Cloud"
		nameFull= name[:name.find("_")]

		tmpMap = {"_op_type": "index", "_index": indexName, "_type": typeName, "_id":name }
		tmpMap.update({"defaultIndex": "reviews", "kibi:relationalPanel": "true"})
		tmpMap.update({"savedSearchId": name})
		tmpMap.update({"sort": ["_score", "desc" ], "version": 1,"description": "", "hits": 0, "optionsJSON": "{\"darkTheme\":false}", "uiStateJSON": "{}", "timeRestore": "false"})
		tmpMap.update({"kibanaSavedObjectMeta": { "searchSourceJSON": "{\"filter\":[{\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}}}]}"}})
		if name.find("tweets")>= 0:
			tmpMap.update({ "title":name, "panelsJSON": "[{\"id\":\""+name+"\",\"type\":\"search\",\"panelIndex\":3,\"size_x\":7,\"size_y\":6,\"col\":1,\"row\":1,\"columns\":[\"emotions.emotion\",\"detected_entities\",\"text\"],\"sort\":[\"entity_linking.URI\",\"desc\"]},{\"id\":\""+EmotionDistribution+"\",\"type\":\"visualization\",\"panelIndex\":4,\"size_x\":5,\"size_y\":4,\"col\":8,\"row\":3},{\"id\":\""+PercentageEmotions+"\",\"type\":\"visualization\",\"panelIndex\":5,\"size_x\":5,\"size_y\":2,\"col\":8,\"row\":1}]"})
		else:
		#	tmpMap.update({ "title":name,"panelsJSON": "[{\"id\":\""+name+"\",\"type\":\"search\",\"panelIndex\":1,\"size_x\":6,\"size_y\":6,\"col\":1,\"row\":1,\"columns\":[\"label\",\"entity_linking.target\"],\"sort\":[\"_score\",\"desc\"]},{\"id\":\"entityEmotion\",\"type\":\"visualization\",\"panelIndex\":3,\"size_x\":6,\"size_y\":4,\"col\":7,\"row\":6},{\"id\":\"Location\",\"type\":\"search\",\"panelIndex\":2,\"size_x\":6,\"size_y\":2,\"col\":1,\"row\":7,\"columns\":[\"label\",\"entity_linking.connection\",\"entity_linking.target\"],\"sort\":[\"connection\",\"asc\"]},{\"id\":\"PercentageEmotions\",\"type\":\"visualization\",\"panelIndex\":4,\"size_x\":6,\"size_y\":5,\"col\":7,\"row\":1}]"})
			tmpMap.update({ "title":name,"panelsJSON": "[{\"col\":1,\"columns\":[\"label\",\"entity_linking.target\"],\"id\":\""+name+"\",\"panelIndex\":1,\"row\":1,\"size_x\":6,\"size_y\":2,\"sort\":[\"_score\",\"desc\"],\"type\":\"search\"},{\"col\":7,\"id\":\""+PercentageEmotions+"\",\"panelIndex\":4,\"row\":1,\"size_x\":6,\"size_y\":3,\"type\":\"visualization\"},{\"col\":1,\"columns\":[\"label\",\"entity_linking.connection\",\"entity_linking.target\"],\"id\":\""+nameFull+"\",\"panelIndex\":2,\"row\":3,\"size_x\":6,\"size_y\":2,\"sort\":[\"connection\",\"asc\"],\"type\":\"search\"},{\"id\":\""+cloudVisualization+"\",\"type\":\"visualization\",\"panelIndex\":5,\"size_x\":3,\"size_y\":2,\"col\":1,\"row\":5},{\"id\":\""+EmotionDistribution+"\",\"type\":\"visualization\",\"panelIndex\":6,\"size_x\":6,\"size_y\":3,\"col\":7,\"row\":4}]"})
			 #"[{\"id\":\""+name+"\",\"type\":\"search\",\"panelIndex\":1,\"size_x\":12,\"size_y\":6,\"col\":1,\"row\":1,\"columns\":[\"label\", \"entity_linking.connection\", \"entity_linking.target\"],\"sort\":[\"_score\",\"desc\"]}]"})
		messages.append(tmpMap)
		print (messages)
		result = bulk(es, messages)
		return result

def writeRelations2ES(nameList, indexName=".kibi", typeName="config" , elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		print(elasticPort, elasticHost)
		messages = []
		logging.debug('Writing Relations')
		print('Writing Relations')
		relationList = set()
		for i in range(len(nameList)-1):
			for j in range(len(nameList)-1):
				relationList.add((nameList[i],nameList[j]))
		originalConfig = getKibiRelationConfig( elasticPort=elasticPort, elasticHost=elasticHost)
		source = originalConfig["_source"]
		del originalConfig["_source"]
		print(source)
		try:
			kibi_relations = json.loads(source["kibi:relations"])
			del source["kibi:relations"]
		except:
			kibi_relations ={}
		#print (relationList)
		tmpMap = originalConfig
		#tmpMap = {"_op_type": "index", "_index": indexName, "_type": typeName, "_id":"4.5.3" }
		tmpMap.update(source)
		tmpMap.update({"kibi:relationalPanel": "true"})
		tmpMap.update({"timepicker:timeDefaults": "{\n  \"from\": \"now-90d\",\n  \"to\": \"now\",\n  \"mode\": \"quick\"\n}"}) # set default timeframe for 90 days
		relationsIndices =[]
		for name in relationList:
			fromName=name[0]
			toName=name[1]
			indices = {"indices": [
									{
									  "indexPatternType": "",
									  "indexPatternId": fromName,
									  "path": "entity_linking.target"
									},
									{
									  "indexPatternType": "",
									  "indexPatternId": toName,
									  "path": "entity_linking.URI"
									}
								  ],
								"label": "connected",
								"id": fromName+"//entity_linking.URI/"+toName+"//entity_linking.target"
					 } 
			relationsIndices.append(indices)
		for nameId in range(len(nameList)-1): #adding links to emotions/ tweets
			fromName=nameList[nameId]
			toName=nameList[len(nameList)-1]
			indices = {"indices": [
									{
									  "indexPatternType": "",
									  "indexPatternId": fromName,
									  "path": "entity_linking.URI"
									},
									{
									  "indexPatternType": "",
									  "indexPatternId": toName,
									  "path": "entity_linking.URI"
									}
								  ],
								"label": "emotion",
								"id": fromName+"//entity_linking.URI/"+toName+"//entity_linking.target"
					 } 
			relationsIndices.append(indices)

		kibi_relations.update({"relationsIndices":relationsIndices})
		tmpMap.update({"kibi:relations":json.dumps(kibi_relations)})

		 
		messages.append(tmpMap)
		#print (messages)
		result = bulk(es, messages)
		return result

def createGrepFile(entities):
	tmpGrepFile = entities[0]
	#print(entities)
	with open (tmpGrepFile,"w") as fails:
		for entity in entities[1]:
			fails.write("<"+entity+">\n")
	return tmpGrepFile

def GrepDBpedia(filename):
	tripleFile=filename+".nt"
	art2catDbpediaFile = "DBpedia_uncompressed/*"

	
	arguments = ['LC_ALL=C;','/bin/grep','-Fhwf',filename,art2catDbpediaFile ,'>',tripleFile]
	try:
		p1 =subprocess.check_output(" ".join(arguments), shell=True)
	
	except subprocess.CalledProcessError as e:
		print (RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)))
	return tripleFile

def createIndexes(entityList):
	entities =list(entityList)
	entities.sort(key=operator.itemgetter(0))
	groupedEntities = [(key.lower(),[elem for _, elem in group])  for key, group in itertools.groupby(entities, lambda pair: pair[0])]
	#print(groupedEntities)
	global loging
	
	allOriginalEntities=[]
	for t in groupedEntities:
		allOriginalEntities.extend(t[1])

	for groupedEntity in groupedEntities:
		entityType =groupedEntity[0]
		grepFile = createGrepFile(groupedEntity)
		tripleFile = GrepDBpedia (grepFile)
		subgraph = filterResults(tripleFile,groupedEntity,allOriginalEntities)
		preSetES(entityType, entityType+"_type" , elasticPort=elasticPort, elasticHost=elasticHost )  
		write2ES(subgraph, entityType, entityType+"_type", elasticPort=elasticPort, elasticHost=elasticHost	)
		uniqueEntities = uniquSubjects(subgraph)
		preSetES(entityType+"_unique", entityType+"_type" , elasticPort=elasticPort, elasticHost=elasticHost)  
		write2ES(uniqueEntities, entityType+"_unique", entityType+"_type", elasticPort=elasticPort, elasticHost=elasticHost )	
		#preSetES(entityType, entityType+"_type" , elasticPort=9200, elasticHost="136.243.53.83")  # pilot 2 servers
		#write2ES(subgraph, entityType, entityType+"_type", elasticPort=9200, elasticHost="136.243.53.83") # pilot 2 servers

def addDefaultIndexes(indexName=".kibi", typeName="config" , elasticPort=9220, elasticHost="localhost"):
		es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
		messages = []
		logging.debug('add default indexes')
		print('add default indexes')
		messages = [ {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "location_unique",
		"_score": 1,
		  "title": "location_unique",
		  "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
		  },
		  {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "person_unique",
		"_score": 1,
		  "title": "person_unique",
		  "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
		  },
		  {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "organization_unique",
		"_score": 1,
		  "title": "organization_unique",
		  "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
		 },
		 {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "location",
		"_score": 1,
		  "title": "location",
		   "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.connection\":[\"entity_linking\",\"connection\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.connection\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
		  },
		  {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "person",
		"_score": 1,
		  "title": "person",
		   "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.connection\":[\"entity_linking\",\"connection\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.connection\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
			},
		  {
		"_index": ".kibi",
		"_type": "index-pattern",
		"_id": "organization",
		"_score": 1,
		  "title": "organization",
		   "paths": "{\"entity_linking.URI\":[\"entity_linking\",\"URI\"],\"entity_linking.connection\":[\"entity_linking\",\"connection\"],\"entity_linking.target\":[\"entity_linking\",\"target\"],\"label\":[\"label\"]}",
		  "fields": "[{\"name\":\"_index\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"entity_linking.target\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.connection\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"entity_linking.URI\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"label\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":true,\"analyzed\":false,\"doc_values\":true},{\"name\":\"_source\",\"type\":\"_source\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_id\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_type\",\"type\":\"string\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false},{\"name\":\"_score\",\"type\":\"number\",\"count\":0,\"scripted\":false,\"indexed\":false,\"analyzed\":false,\"doc_values\":false}]"
		 }
		 ]

		result = bulk(es, messages)
		return result

def uniquSubjects(subgraph ):
	subjects ={}
	for record in subgraph:
		if record[0] in subjects:
			tmp = subjects[record[0]]
			tmp.append(record[2].replace("/resource/","/page/"))
			subjects[record[0]] = tmp
		else:
			subjects[record[0]] = [record[2].replace("/resource/","/page/")]
	
	return tuple(subjects.items())

def filterResults(tripleFile, oneTypeEntities,allOriginalEntities ):
	filters4Types = {"location": ["birthPlace","location","foundation","type","founder","predecessor","party","country","based","channel","relations","branch","placeOfBirth","spouse","network","chancellor","parent","parents","primeminister","artist","athletics","title","successor","state"],
					 "organization": ["type", "successor", "distributor", "birthPlace", "vicepresident", "largestcity", "residence", "spouse", "predecessor", "state", "party", "children", "parents", "education", "almaMater", "president", "leaderName", "largestCity", "officialLanguages", "leaderTitle", "capital", "governmentType", "influences", "influenced"],
					 "person":["leaderName", "leaderTitle", "type", "largestCity", "governmentType", "officialLanguages", "capital", "network", "firstAired", "channel", "headquarters", "sisterNames", "broadcastArea", "country", "director","creator"]}
	name = tripleFile.replace(".nt","")
	tupleSet=set()
	entityType =oneTypeEntities[0]
	with open (tripleFile) as fails:
		for line in fails:
			s,p,*o = line.replace("<","").split("> ")
		#	print (s,p,o[0])
		#
			if s in oneTypeEntities[1] and (o[0] in allOriginalEntities or o[0].find("http://dbpedia.org/ontology/")>=0):
				if p[p.rfind("/")+1:].replace("22-rdf-syntax-ns#","") not in filters4Types[entityType]: # filters out manualy selected types 
					tupleSet.add((s,p[p.rfind("/")+1:].replace("22-rdf-syntax-ns#",""),o[0].replace("\n","")))

	return tupleSet

def mergeConfiguarion(config):
	global newConfig
	#print (newConfig)
	#print(config)
	origCred = newConfig["credentials"]
	origVal = newConfig["variables"]
	if "credentials" in config:
		newCred = config["credentials"]
	else:
		newCred= {}
	if "variables" in config:
		newVal =  config["variables"]
	else:
		newVal = {}
	origCred.update(newCred)
	origVal.update(newVal)
	newConfig = {"credentials":origCred, "variables":origVal }

def setVariables(config):
	global elasticPort
	global elasticHost
	global elasticUsername
	global elasticPassword
	global inputIndexName
	global inputIndexType
	global indexList
	global filters
	error =[]
	try:
		elasticPort = config['credentials']['elasticPort']
	except:
		error.append("elasticPort")
	try:
		elasticHost = config['credentials']['elasticHost']
	except:
		error.append("elasticHost")
	try:
		elasticUsername = config['credentials']['elasticUsername']
	except:
		error.append("elasticUsernam")
	try:
		elasticPassword = config['credentials']['elasticPassword']
	except:
		error.append("elasticPasswor")
	try:
		inputIndexName = config['variables']['inputIndexName']
	except:
		error.append("inputIndexName")
	try:
		inputIndexType = config['variables']['inputIndexType']
	except:
		error.append("inputIndexType")
	try:
		indexList = config['variables']['indexList']
	except:
		error.append("indexList")
	try:
		filters = config['variables']['filters']
	except:
		error.append("filters")
	if error:
		return {"error": "Configuration file error", "bad config variables":error}

def deleteIndex( indexList,  elasticPort=9220, elasticHost="localhost"):
	global loging

	es = Elasticsearch([{'host': elasticHost, 'port': elasticPort}], http_auth=(elasticUsername, elasticPassword))
	for index in indexList:
		result =es.indices.delete(index=index, ignore=[400, 404])
		loging.append("deleting index "+index+" "+str(result))
	return result

#-------------------------------------------------------------------------
# Rest interface

app = Flask(__name__)

with open('originalConfig.json') as json_data:
	originalConfig = json.load(json_data)
newConfig =  copy.deepcopy(originalConfig)

#defining global Variables
#----------------------------------------
elasticPort=0
elasticHost=""
elasticUsername=""
elasticPassword=""
inputIndexName=""
inputIndexType=""
indexList=[]
filters={}
#--------------------------------------------
loging = [] # used for logging
startTime ="" 

def create():
	"""
	Start Creating Retrieve the Graph
	"""
	# request.method == 'GET'
	global loging
	global startTime
	loging =[]
	
	startTime = datetime.now()
	try:
		cloneTweets(typeName=inputIndexType,indexName=inputIndexName, elasticPort=elasticPort, elasticHost=elasticHost)
	except:
		loging.append("error: problem cloning tweets from index"+inputIndexName+" docType: "+inputIndexType)
		return make_response(jsonify({"error":"problem cloning tweets from "}),500)
	try:
		deleteIndex( indexList, elasticPort=elasticPort, elasticHost=elasticHost)
	except:
		loging.append("error: problems deleting index")
		return make_response(jsonify({"error":"problems deleting index"}),500)
	try:
		entities = getDatasets(indexName="tweets", typeName="text_review", elasticPort=elasticPort, elasticHost=elasticHost) 
		if not entities:
			return make_response(jsonify({"error":"no results from index: "+inputIndexName+" docType: "+inputIndexType+"elasticHost: "+elasticHost+" elasticPort: "+elasticPort}),500)
		loging.append("got entities")
	except:
		loging.append("error: problems reading input index")
		return make_response(jsonify({"error":"problems reading input index"}),500)
	print("creating indexes")
	try:
		createIndexes(entities)
		loging.append("created indexes successfully")
	except:
		loging.append("error: creating indexes")
		return make_response(jsonify({"error":"creating indexes"}),500)

	try:
		output = addDefaultIndexes(indexName=".kibi", typeName="config" , elasticPort=elasticPort, elasticHost=elasticHost)
		loging.append("adding default indexes "+str(output))
	except:
		loging.append("error: adding default indexes")
		return make_response(jsonify({"error":"adding default indexes"}),500)
	fullIndex =indexList+["tweet"]#inputIndexName
	#
	for index in fullIndex+["location", "organization", "person"]:
		try:
			output =writeSearch2ES(index, indexName=".kibi", typeName="search",  elasticPort=elasticPort, elasticHost=elasticHost)
			loging.append("Creating search "+index+" "+str(output))
		except:
			loging.append("error: creating search for "+index)
			return make_response(jsonify({"error":"creating search for "+index}),500)

	try:
		output = createVisualizations(fullIndex, elasticPort=elasticPort, elasticHost=elasticHost)
		loging.append("Creating visualization "+str(output))
	except:
		loging.append("error: creating visuaizations")
		return make_response(jsonify({"error":"creating visuaizations"}),500)

	for index in fullIndex:
		try:
			output=writeDashboard2ES(index, indexName=".kibi", typeName="dashboard" ,elasticPort=elasticPort, elasticHost=elasticHost)
			loging.append("Creating dashboards "+index+" "+str(output))
		except:
			loging.append("error: creating dashboard for "+index)
			return make_response(jsonify({"error":"creating dashboard for "+index}),500)
	try:
		output = writeRelations2ES(fullIndex, elasticPort=elasticPort, elasticHost=elasticHost)
		loging.append("Creating relations "+str(output))
	except:
		loging.append("error: creating relationships")
		return make_response(jsonify({"error":"creating relationships"}),500)
	loging.append("Finished: "+str(datetime.now()))
	
	#return jsonify({"status":"done", "started":startTime, "finished":datetime.now(), "log":loging})
	return "done"

#strating REST calls
#-------------------------------------------------

@app.route("/reset", methods=['GET'])
def resetConfiguration():
	global newConfig
	print(originalConfig)
	newConfig = copy.deepcopy(originalConfig)
	return jsonify(newConfig)

@app.route("/configuration", methods=['GET', 'POST'])
def configuration():
	"""
	Provides existing configuration (GET)
	accepts new configuration (POST)
	"""
	if request.method == 'POST':
		if not request.json:
			return make_response(jsonify({'error': 'Not JSON format'}), 400)
		if not 'credentials' in request.json:
			return make_response(jsonify({'error': 'no elasticsearch credentials provided'}), 400)
		configFile = request.json 	
		try:
			mergeConfiguarion(configFile)
			return jsonify({"status":"Updated"})
		except:
			return make_response(jsonify({"error":"Failed updating configuration"}),500)
	# Provides existing configuration (GET)
	return jsonify(newConfig)

@app.route("/status", methods=['GET'])
def status():
	"""
	Provides existing configuration (GET)
	accepts new configuration (POST)
	"""
	if len(loging)>1:
		if loging[len(loging)-1].find("Finished:")>=0:
			return jsonify({"status":"finished","started":startTime, "log":loging})
		if loging[len(loging)-1].find("error:")>=0:
			return jsonify({"status":"errors","started":startTime, "log":loging})
		return jsonify({"status":"running","started":startTime, "log":loging})
	return jsonify({"status":"not started"})

@app.route("/start", methods=['GET'])
def start():
	result = setVariables(newConfig)
	if result:
		return make_response(jsonify(result),500)

	if elasticPort==0 or elasticHost=="elasticIP":
		return make_response(jsonify({'error': 'provide correct elasticsearch credentials '}), 400)

	background_thread = Thread(target=create, args=())
	background_thread.start()
	return jsonify({"output":"check status on /status"})

if __name__ == "__main__":
	app.run( debug=False, port=8012, host="140.203.155.226",threaded = True)
