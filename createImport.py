#!/usr/bin/python
# coding=utf-8

import json
import re
import MySQLdb
from datetime import datetime

def loadFile():
	# with open('./dataset/theSmallestDataset.txt') as f:
	# with open('./dataset/smallDataset.txt') as f:
	# with open('./dataset/normalDataset.txt') as f:
	with open('./dataset/bigDataset.txt') as f:
	# with open('./dataset/test.txt') as f:
		return f.readlines()

def getCqlDatetimeFormat(dtStr):
	dtObj = datetime.strptime(dtStr, '%Y/%m/%d %I:%M:%S')
	dtStr = str(dtObj.strftime('%Y-%m-%d %I:%M:%S'))
	return dtStr

def getCqlStr(str):
	if str is None:
		return 'NULL'
	else:
		return ("\'" + str + "\'")

def getCqlNum(str):
	if str is None:
		return 'NULL'
	else:
		return str

def initDb():
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
	return db.cursor()

def processesPrefix(str):
	# ziska a odreze datum
	m = re.search(r"[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", str)
	if m:
		dt = getCqlDatetimeFormat(m.group())
	else:
		return str, None, "ERROR", None
	dt = getCqlDatetimeFormat(m.group())
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
	return str, company, info, dt

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
	str, company, info, dt = processesPrefix(str)
	if info:
		str = processesSufix(str)
		str = validateJson(str, info)
		return str, company, info, dt
	return str, None, None, dt

# ziska vsechny bids (plus info, zda jsou vitezne)
def getBids(cursor, lines):
	query = ("SELECT dsp_bid.original_id, dsp_imp.win, dsp_imp.bidfloor, dsp_imp.cpt, dsp_imp.auction_price" 
	+ " FROM dsp_bid INNER JOIN dsp_imp ON (dsp_imp.bid_id=dsp_bid.id) WHERE dsp_bid.original_id IN (")
	responses = {}
	winningBids = {}

	for line in lines:
		jsonStr, company, info, dt = getJson(line)

		if info and info in ["resp"]:
			try:
				data = json.loads(jsonStr)
			except ValueError:
				# pravdepodobne response err
				continue
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
				'auction_price' : None
			}

	query += "\"\");"

	cursor.execute(query)
	for row in cursor.fetchall():
		# klicem je original_id
		responses[row[0]]['win']           = row[1]
		responses[row[0]]['bidfloor']      = row[2]
		responses[row[0]]['cpt']           = row[3]
		responses[row[0]]['auction_price'] = row[4]
		
	return responses

def loadDataForInsert(data, responses, company, dt):
	d = {}
	d['id']            = data['id']
	d['imp_id']        = data['imp'][0]['id']
	d['company']       = company
	d['website']       = data['site']['page'].replace('\n', '') if 'site' in data and 'page' in data['site'] else None
	d['app']           = data['app']['name'] if 'app' in data and 'name' in data['app'] else None
	# data z response
	d['price']         = responses[data['id']]['price']
	d['cur']           = responses[data['id']]['cur']
	d['win']           = responses[data['id']]['win']
	d['bidfloor']      = responses[data['id']]['bidfloor']
	d['cpt']           = responses[data['id']]['cpt']
	d['auction_price'] = responses[data['id']]['auction_price']
	d['datetime']      = dt
	return d

def writeToFile(data, file, mode = 'a'):
	with open(file, mode) as f:
    		f.write(data)

def getImport(lines, responses):
	requestsNr = responsesNr = 0
	multirequestsNr = multiresponsesNr = 0
	i = 0

	# clear output file
	writeToFile('', 'insert.cql', 'w')
	
	for line in lines:
		jsonStr, company, info, dt = getJson(line)

		if info and info in ["req", "resp"]:
			data = json.loads(jsonStr)
			if info=="req" and 'id' in data and 'imp' in data:
				requestsNr += 1
				if len(data['imp'])>1:
					multirequestsNr += 1
					# multirequesty zatim nezpracovavam
					continue
				# jestlize existujes response (do budoucna chci vsechny)
				if data['id'] in responses:
					d = loadDataForInsert(data, responses, company, dt)
					i += 1
					query = ("INSERT INTO dostam12_ks.logs("
						+ "id,imp_id,company,website,app,price,cur,win,bidfloor,cpt,auction_price,datetime)"
						+ " VALUES ("
						+ "%s"  % (getCqlStr(d['id']))
						+ ",%s" % getCqlNum(d['imp_id'])
						+ ",%s" % getCqlStr(d['company'])
						+ ",%s" % getCqlStr(d['website'])
						+ ",%s" % getCqlStr(d['app'])
						+ ",%s" % getCqlNum(d['price'])
						+ ",%s" % getCqlStr(d['cur'])
						+ ",%s" % getCqlNum(d['win'])
						+ ",%s" % getCqlNum(d['bidfloor'])
						+ ",%s" % getCqlNum(d['cpt'])
						+ ",%s" % getCqlNum(d['auction_price'])
						+ ",%s" % getCqlStr(d['datetime'])
						+ ");\n"
					)
					writeToFile(query, 'insert.cql')
			elif info=="resp":
				responsesNr += 1
				if (len(data['seatbid'])>1 or len(data['seatbid'][0]['bid'])>1):
					multiresponsesNr += 1
			else:
				print "ERROR:"
				print jsonStr
				print line

	print "multirequestsNr  = " + str(multirequestsNr)
	print "multiresponsesNr = " + str(multiresponsesNr)
	print "i = " + str(i) 
	return requestsNr, responsesNr

def main():
	cursor = initDb()
	lines = loadFile()
	responses = getBids(cursor, lines)
	requestsNr, responsesNr = getImport(lines, responses)
	print "responsesNr = " + str(responsesNr)
	print "requestsNr  = " + str(requestsNr)

if __name__ == "__main__":
	main()