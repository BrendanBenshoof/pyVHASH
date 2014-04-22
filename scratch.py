a = ["a", "b", "c", "z"]
b = ["d","b","a"]

for i in [for x in a if x not in b]:
    print i
