import pickle

matches = []

with open("match_ids.txt", "rb") as f:
	matches = pickle.load(f)

for match in matches:
	print(match)
