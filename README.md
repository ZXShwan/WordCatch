# WordCatch
WordCatch is a webapp which helps non-native English speaker learn English. It gives users a few word candidates to fill in an incomplete phrase. For example, when a user typing "different _ them", our webapp will prompt out the following candidates: "different from them", "different than them", "different to them", "different about them" etc, each with five example sentences. It also supports filtering by part of speech. When a user typing "v. an issue", it will prompt out candidates like "submit an issue", "have an issue", etc.

### Technology
WordCatch is built by/on:

* Spark
* Hbase
* NLTK(Natural Language Toolkit)
* Django

### Schema Design

When we want to get all word candidates which fill in the phrase "different _ them", we could  query Hbase like this  `get 'wordcatch_umbc','07:different,them',[2]` in hbase shell.

In this example:

**Table Name**: `wordcatch_umbc`

**Partition key**: `07:different,them`  Prefix 07 means the key is salted to regions.

**Column Family**: `2 `  It means we want to find all word candidates filled in the middle blank.

**Column key**: `2:from` the word candidate with column family name `2`

**Cell **:

```Json
{ 
   "count":278,
   "pos":"IN",
   "example":[ 
      "We still have too many Americans who give into their fears of those who are different from them",
      " For centuries, the Japanese have considered Burakumin people as descendants of Korean prisoners of war, even though there is no evidence that they are racially different from them"
   ]
}
```

In this json file:

`count` the word candidate occurance in the umbc copus

`pos` the part of speech of this word candidate (See [Penn TreeBank Project](https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html))

`example` example sentences to further clarify the usage

[**More Example Query Result**](http://htmlpreview.github.io/?https://github.com/hialvin/WordCatch/blob/master/WordCatch-core/src/main/python/QuerySample.html)

### Check it out

[**main program by Spark**](https://github.com/hialvin/WordCatch/blob/master/WordCatch-core/src/main/python/main.py)

**[Cell Converter](https://github.com/hialvin/WordCatch/blob/master/ScalaUtils/src/main/scala/ngram/NgramValueUtil.scala)** :  help generate `Cell` for the bulk load by HFile.

### Issues

Should upgrade the Django App to get compatible with the new schema.

### Future Update




