#!/usr/bin/python
# coding=utf-8

import json
import re
import MySQLdb

# TODO: v logu se muzou nachazet multirequest a multiresponsy!

with open('config') as f:
        config = f.readline()

# parsuje config
config = config.split('\n')[0].split(',')

# pripojeni k db
db = MySQLdb.connect(host=config[0],
                     port=int(config[1]),
                     user=config[2],
                     passwd=config[3],
                     db=config[4])

# # you must create a Cursor object. It will let
# #  you execute all the queries you need
cur = db.cursor()

# with open('theSmallestDataset.txt') as f:
# with open('smallDataset.txt') as f:
with open('normalDataset.txt') as f:
	lines = f.readlines()

def processesPrefix(str):
	# odreze bordel na zacatku
	str = str[15:]
	# odreze vsechno az po company
	str = str.split('[', 2)[2]
	# ulozi a odreze company
	str = str.split(']', 1)
	company = str[0]
	str = str[1]
	# rozdeli line na info a zbytek
	str = str.split('{', 1)

	info = None
	if (str[0].find("response")>0):
		info = "resp" 
	elif (str[0].find("request")>0):
		info = "req"
	str = "{" + str[1]
	return str, company, info

def processesSufix(str):
	# odreze vsechno az po '}'
	return re.sub('} .*$', '}', str)

def validateJson(str, info):
	if (info=="req"):
		# prohodi uvozovky za apostrofy a opacne
		str = str.replace('\"', 'X@X@X@X').replace("'", '"').replace('": u"', '": "')
		str = str.replace("\\x", "").replace('X@X@X@X', '\'')
	return str

def getJson(str):
	str, company, info = processesPrefix(str)
	if info in ["resp", "req"]:
		str = processesSufix(str)
		str = validateJson(str, info)
		return str, company, info
	return str, company, None

# ziska vitezne bids z db
def getWinningBids():
	query = ("SELECT dsp_bid.original_id, dsp_imp.win, dsp_imp.bidfloor, dsp_imp.cpt, dsp_imp.auction_price" 
	+ " FROM dsp_bid INNER JOIN dsp_imp ON (dsp_imp.bid_id=dsp_bid.id) WHERE dsp_bid.original_id IN (")
	responses = {}
	winningBids = {}

	for line in lines:
		jsonStr, company, info = getJson(line)

		if info and info in ["resp"]:
			data = json.loads(jsonStr)
			# ziska id responsu
			query += str("\"") + data['id'] + str("\",")

			if len(data['seatbid'])!=1:
				print len(data['seatbid'])
				# tady je potreba pracovat s multireguestama apod.

			# data = data['seatbid'][0]['bid'][0]['price']

			
			# ['seatbid'][0]


			# ['bid']['price']
			# naplni responses
			# responses[data['id']]={
			# 	'price':,
			# 	'cur':data['cur']
			# }


	query += "\"\");"

	cur.execute(query)
	for row in cur.fetchall():
		# klicem je original_id
		winningBids[row[0]]={
			'win':row[1],
			'bidfloor':row[2],
			'cpt':row[3],
			'auction_price':row[4]
		}
		
	return winningBids, responses

def getImport(winningBids):
	requestsNr = responsesNr = 0
	
	for line in lines:
		jsonStr, company, info = getJson(line)

		if info and info in ["req", "resp"]:
			data = json.loads(jsonStr)
			if info=="req":
				requestsNr += 1
				# prohodiint "INSERT INTO dostam12_ks.logs(value1,...) VALUES (value1,...);"
			if info=="resp":
				responsesNr += 1

	return requestsNr, responsesNr

def main():
	winningBids, responses = getWinningBids()
	requestsNr, responsesNr = getImport(winningBids)

	# print winningBids
	print "responsesNr = " + str(responsesNr)
	print "requestsNr  = " + str(requestsNr)

if __name__ == "__main__":
    main()