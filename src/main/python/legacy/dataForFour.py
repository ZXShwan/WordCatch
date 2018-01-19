from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import re

dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="https://dynamodb.us-west-2.amazonaws.com")

table = dynamodb.Table('FourSecond')

with open("/Users/xuanyu/Desktop/FourSecondOut/part-00022") as file:
    line = file.readline()
    while line:
      words = re.split('[^a-zA-Z0-9\-\=\?\>]+',line)
      if len(words)<6 or int(words[-2])<3:
        line = file.readline()
        continue
      else:
        HasWrong = False 
        KeyWord = words[1]+" "+words[2]+" "+words[3]
        k = 0
        word = ()
        for i in range(4,len(words)-1):
          if i%2 == 0: 
            Tempword = words[i]
            i=i+1
          elif words[i].isdigit():
            Tempnum = words[i]
            Temp = (Tempword, Tempnum)
            word = word + Temp
            k=k+1
            i=i+1

          else:
            HasWrong = True
            break



        if HasWrong:
          line = file.readline()
          continue

      print(KeyWord)
      ItemContent={
        'Key': KeyWord
      }
      ItemContent.clear()
      index = 1
      for i in range(len(word)):
        if i%2 != 0:
          ItemContent[str(index)+'num'] = int(word[i])
          index = index + 1
          i = i + 1
        else:
          ItemContent[str(index)+'word'] = word[i]
          i = i + 1




      table.put_item(
        Item={
          'Keyword': KeyWord,
          'content': ItemContent,
        })
      line = file.readline()


        
        

        

      #   table.put_item(
      #      Item={
      #          'word': word,
      #          'content': content,
      #       }
      #   )

