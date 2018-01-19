for((i=10;i<40;i++));

do

wget https://s3-us-west-1.amazonaws.com/wordcatch/wiki10_39/wiki_$i.bz2
echo https://s3-us-west-1.amazonaws.com/wordcatch/wiki10_39/wiki_$i.bz2
sleep 3

done