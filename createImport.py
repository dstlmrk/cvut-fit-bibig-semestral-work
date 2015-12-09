#!/usr/bin/python
# coding=utf-8

# od Miry na replace apostrofu na uvozovky
# replace("'", '"').replace('": u"', '": "').replace("\\x", "")

import json

def cutPrefix(str):
        i = 0
        for letter in line:
                i += 1
                if (letter=='['):
                        break
        str = str[i:]
        return str

def getCompany(str):
        company = ""
        for letter in str:
                if (letter==']'):
                        break
                company += letter
        return company

def cutHashReverse(str):
	i = 0
	for c in reversed(str):
		i += 1
		if (c=='['):
                        break
     	str = str[:-i]
     	return str

def cutInfoReverse(str):
	i = 0
	for c in reversed(str):
		i += 1
		if (c=='{'):
                	break
     	str = str[:-i]
     	return str

# with open('theSmallestDataset.txt') as f:
# with open('smallDataset.txt') as f:
with open('normalDataset.txt') as f:
        lines = f.readlines()

# s = '{"foo": 6, "bar": [1, 2, 3]}'
# d = json.loads(s)
# print isinstance(d, dict)
# print d


print "SELECT * FROM dsp_bid WHERE original_id IN ("

	# 4565510a-7fcb-4748-a152-a7c62e16ecd2") limit 10;"

for line in lines:
	# print "---------------------------------"
	# odreze datum
	if (line.find("request", 50, 70)>0):
		# print line
		# odreze datum
		line = line[34:]
		# odreze id
		line = cutPrefix(line)
		company = getCompany(line)
		# print "REQUEST["+str(company)+"]"
		# odreze company a jmeno logu
		line = line[(len(company)+2+len("request")+2):]
		# odreze konec
		# line = line[:-43]
		line = cutInfoReverse(line)
		line = cutHashReverse(line)

		
		# nahradi apostrofy za uvozovky
		# ten overkill s XXXXXXX je tam kvuli tomu, abychom puvodni uvozovku mohl prevest na apostrof
		line = line.replace('\"', 'XXXXXXX').replace("'", '"').replace('": u"', '": "').replace("\\x", "").replace('XXXXXXX', '\'')


		# print line
		# nacte do slovniku
		d = json.loads(line)

		# insert = "INSERT INTO dostam12_ks.logs(value1,...) VALUES (value1,...);"
		# query = "INSERT INTO accesslog_ks.logs(user,date,webpage,severity) VALUES ('#{user}','#{date}','#{site}','#{sev}');";
		# print insert

	elif (line.find("response", 40, 70)>0):
		# print line
		# odreze datum
		line = line[21:]
		# odreze id
		line = cutPrefix(line)
		company = getCompany(line)
		# print "RESPONSE["+str(company)+"]"
		line = line[(len(company)+2+len("response")+2):]

		# odreze time
		i = 0
		for letter in line:
			if (letter=='{'):
				break
			i += 1
		line = line[i:]
		# odreze konec (tohle by se melo jeste postelo)
		line = cutInfoReverse(line)
		line = cutHashReverse(line)

		# print line
		# nacte do slovniku
		d = json.loads(line)

		# ziska id requestu a responsu
		print str("\"") + d['id'].lower()+str("\",")


        # i = 0
        # for letter in line:
        #         i += 1
        #         if (letter=='['):
        #                 break
        # odreze dalsi znaky
        # line = line[i:]
        # nacte company
        company = ""


print "\"\") LIMIT 100;"