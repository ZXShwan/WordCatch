import sys, re, nltk, math, hashlib, json
from operator import add
from pyspark import SparkContext, SparkConf
sys.path.append("..")
# from ngram_util import convert_to_json

EXAMPLE_NUM = 3


def reduce_func(x, y):
	ret1 = x[0] + y[0]
	a = str_to_list(x[1])
	b = str_to_list(y[1])
	temp = a + b
	ret2 = temp[:EXAMPLE_NUM]
	return ret1, ret2


def str_to_list(x):
	if isinstance(x, list):
		return x
	else:
		ret_list = [x]
		return ret_list


def non_word_filter(x):
	if x[0][0][0].isalpha() and x[0][1][0].isalpha() and x[0][2][0].isalpha():
		return True


def salt(key, modulus):
	salt_as_int = str(int(hashlib.md5(key.encode('utf-8')).hexdigest()[:8], 16) % modulus)
	# // left pading with 0
	chars_in_salt = int(digits_required(modulus))
	return salt_as_int.zfill(chars_in_salt) + ":" + key


def digits_required(modulus):
	return math.floor(math.log10(modulus-1)+1)


def convert_to_json(x, column_family):
	ret = {}
	key = x[0][0]
	ret[key] = column_family_to_dict(x[1], column_family)
	return json.dumps(ret)


def column_family_to_dict(x, column_family):
	ret = {str(column_family): column_to_dict(x)}
	return ret


def column_to_dict(x):
	ret = {}
	can = {"pos": x[0][1], "count": x[1][0], "example": x[1][1]}
	ret[x[0][0]] = can
	return ret


if __name__ == "__main__":

	if len(sys.argv) != 3:
		print("Usage: ngarm input_file output_dir")
		exit(-1)
	sc = SparkContext()

	partition_num = 15
	word_threshold = 10

	lines = sc.textFile(sys.argv[1])

	sentences = lines.glom() \
		.map(lambda x: " ".join(x)) \
		.flatMap(lambda x: re.split('[.]', x))
	words_pos_tags = sentences.map(lambda sentence: (nltk.word_tokenize(sentence.lower()), sentence))\
		.map(lambda token_sentence_tuple: (nltk.pos_tag(token_sentence_tuple[0]), token_sentence_tuple[1]))

	trigrams = words_pos_tags\
		.flatMap(lambda x: (((x[0][i], x[0][i + 1], x[0][i + 2]), x[1]) for i in range(len(x[0]) - 2))) \
		.filter(non_word_filter)\
		.map(lambda x: (x[0], (1, x[1])))\
		.reduceByKey(reduce_func)

	words10 = trigrams.filter(lambda x: x[1][0] > word_threshold).cache()

	first = words10.map(lambda x: ((x[0][1][0] + "," + x[0][2][0]), (x[0][0], x[1]))) \
		.map(lambda x: ((salt(x[0], partition_num), "1", x[1][0][0]), x[1]))\

	middle = words10.map(lambda x: ((x[0][0][0] + "," + x[0][2][0]), (x[0][1], x[1]))) \
		.map(lambda x: ((salt(x[0], partition_num), "2", x[1][0][0]), x[1]))\

	last = words10.map(lambda x: ((x[0][0][0] + "," + x[0][1][0]), (x[0][2], x[1]))) \
		.map(lambda x: ((salt(x[0], partition_num), "3", x[1][0][0]), x[1]))\

	final = first.union(middle).union(last)\
		.repartitionAndSortWithinPartitions(
		partition_num, lambda x: int(x[0][0:int(digits_required(partition_num))]), True, lambda x: (x[0], x[1], x[2])
		)
	final_json = final.map(lambda x: (x[0][0], convert_to_json(x, x[0][1])))

	keyConv = "ngram.NgramKeyConverter"
	valueConv = "ngram.NgramCellConverter"
	conf = {
		"hbase.mapreduce.hfileoutputformat.table.name": "mytable",
		"hbase.zookeeper.quorum": "babar.es.its.nyu.edu,master-1-1.local,login-1-1.local",
			}
	final_json.saveAsNewAPIHadoopFile(
		path=sys.argv[2],
		outputFormatClass="ngram.PatchedHFileOutputFormat2",
		keyClass="org.apache.hadoop.hbase.io.ImmutableBytesWritable",
		valueClass="org.apache.hadoop.hbase.Cell",
		keyConverter=keyConv,
		valueConverter=valueConv,
		conf=conf)
	sc.stop()
