#!/usr/bin/python
# coding=utf-8

import json
import re
import MySQLdb

# TODO: multirequest: "ea5d2905-0a33-4177-b5bd-43790c118da0" (ma vice vyher)
# TODO: dat nacitani DB do fce

with open('config') as f:
        config = f.readline()

# parsuje config
config = config.split('\n')[0].split(',')

# pripojeni k db
db = MySQLdb.connect(host   = config[0],
                     port   = int(config[1]),
                     user   = config[2],
                     passwd = config[3],
                     db     = config[4])

cur = db.cursor()

# with open('theSmallestDataset.txt') as f:
# with open('smallDataset.txt') as f:
with open('normalDataset.txt') as f:
	lines = f.readlines()

def processesPrefix(str):
	# ziska a odreze datum
	m = re.search(r"[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", str)
	datetime = m.group()
	str = re.sub('^.*[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', '', str)
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
	return str, company, info, datetime

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
	str, company, info, datetime = processesPrefix(str)
	if info:
		str = processesSufix(str)
		str = validateJson(str, info)
		return str, company, info, datetime
	return str, company, None, datetime

# ziska vsechny bids (plus info, zda jsou vitezne)
def getBids():
	query = ("SELECT dsp_bid.original_id, dsp_imp.win, dsp_imp.bidfloor, dsp_imp.cpt, dsp_imp.auction_price" 
	+ " FROM dsp_bid INNER JOIN dsp_imp ON (dsp_imp.bid_id=dsp_bid.id) WHERE dsp_bid.original_id IN (")
	responses = {}
	winningBids = {}

	for line in lines:
		jsonStr, company, info, datetime = getJson(line)

		if info and info in ["resp"]:
			data = json.loads(jsonStr)

			# multiresponsy zatim nezpracovavam
			if (len(data['seatbid'])>1 or len(data['seatbid'][0]['bid'])>1):
				continue

			# ziska id responsu
			query += str("\"") + data['id'] + str("\",")

			responses[data['id']]={
				'price'         : data['seatbid'][0]['bid'][0]['price'],
				'cur'           : data['cur'],
				'win'           : None,
				'bidfloor'      : None,
				'cpt'           : None,
				'auction_price' : None,
				'datetime_resp' : datetime
			}

	query += "\"\");"

	cur.execute(query)
	for row in cur.fetchall():
		# klicem je original_id
		responses[row[0]]['win']           = row[1]
		responses[row[0]]['bidfloor']      = row[2]
		responses[row[0]]['cpt']           = row[3]
		responses[row[0]]['auction_price'] = row[4]
		
	return responses

def getImport(responses):
	requestsNr = responsesNr = 0

	i = 0

	multirequestsNr = multiresponsesNr = 0
	
	for line in lines:
		jsonStr, company, info, datetime = getJson(line)

		if info and info in ["req", "resp"]:
			data = json.loads(jsonStr)
			if info=="req":
				requestsNr += 1
				if len(data['imp'])>1:
					multirequestsNr += 1
					# multirequesty zatim nezpracovavam
					continue

				# jestlize existujes response (do budoucna chci vsechny)
				if data['id'] in responses:
					# data pro Cassandru
					d = {}
					d['id']      = data['id']
					d['imp_id']  = data['imp'][0]['id']
					d['company'] = company
					d['website'] = data['site']['page'] if 'site' in data and 'page' in data['site'] else None
					d['app'] = data['app']['name'] if 'app' in data and 'name' in data['app'] else None
					# data z response
					d['price']         = responses[data['id']]['price']
					d['cur']           = responses[data['id']]['cur']
					d['win']           = responses[data['id']]['win']
					d['bidfloor']      = responses[data['id']]['bidfloor']
					d['cpt']           = responses[data['id']]['cpt']
					d['auction_price'] = responses[data['id']]['auction_price']
					d['datetime_req']  = datetime
					d['datetime_resp'] = responses[data['id']]['datetime_resp']
					i += 1

					print ("INSERT INTO dostam12_ks.logs(idimp_id,company,website,app,price,cur,win,bidfloor,cpt,auction_price)" + 
						" VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\");" 
						% (
							d['id'],
							d['imp_id'],
							d['company'],
							d['website'],
							d['app'],
							d['price'],
							d['cur'],
							d['win'],
							d['bidfloor'],
							d['cpt'],
							d['auction_price'],
							d['datetime_req'],
							d['datetime_resp']
						)
					)

			if info=="resp":
				responsesNr += 1
				if (len(data['seatbid'])>1 or len(data['seatbid'][0]['bid'])>1):
					multiresponsesNr += 1

	print "multirequestsNr  = " + str(multirequestsNr)
	print "multiresponsesNr = " + str(multiresponsesNr)
	print "i = " + str(i) 
	return requestsNr, responsesNr

def main():
	responses = getBids()
	requestsNr, responsesNr = getImport(responses)

	# print winningBids
	print "responsesNr = " + str(responsesNr)
	print "requestsNr  = " + str(requestsNr)

if __name__ == "__main__":
	main()