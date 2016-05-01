#converts Fortinet's Fortiguard Webfilterlog (which is space separated) into a CSV file.

from glob import glob
import pandas as pd
import re, os

#since the file is space separated, the spaces between the quotes need to be replaced with another character ('-' here) in order to function properly.
def replace_spaces_within_quotes():
	print "Processing the logs...."
	files = glob('*.log')
	for filename in files:
		output = open(filename[:-4]+'.plog',"w")
		input = open(filename)
		for line in input:
	    		for quoted_text in re.findall(r'\"(.+?)\"', line):
	    			line = line.replace(quoted_text, quoted_text.replace(" ", "-"))
			output.write(line)
		input.close()
		output.close()

#create CSV out of the .plog files (that have already been processed to remove spaces within the quotes)
def value(item):
    return item[item.find('=')+1:]

def create_csv():
	print "Exporting CSV file...."
	csv_list = []
	files = glob('*.plog')
	for filename in files:
	    df = pd.read_table(filename, header=None, delim_whitespace=True, quotechar='"',
		               converters={i:value for i in range(34)},
		               names='date time logid type subtype eventtype level vd policyid identidx sessionid user group srcip srcport srcintf dstip dstport dstinf2 service hostname profiletype profile status reqtype url sentbyte rcvdbyte msg method class cat catdesc'.split())
	    csv_list.append(df)
	    os.remove(filename)   
	return csv_list


#create a complete dataframe by concatenating all the CSVs and then export it.
replace_spaces_within_quotes()

complete_df = pd.DataFrame()
csv_list = create_csv()
complete_df = pd.concat(csv_list)
complete_df.drop_duplicates(inplace=True)
complete_df.to_csv('complete_log.csv')

print "Done. Check the file complete_log.csv."