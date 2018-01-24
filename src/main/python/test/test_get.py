import hashlib
import math


def salt(key, modulus):
	salt_as_int = str(int(hashlib.md5(key.encode('utf-8')).hexdigest()[:8], 16) % modulus)
	# // left pading with 0
	chars_in_salt = int(digits_required(modulus))
	return salt_as_int.zfill(chars_in_salt) + ":" + key


def digits_required(modulus):
	return math.floor(math.log10(modulus-1)+1)


key = "take,over"
key = "different,them"
modulus = 15
print salt(key,modulus)