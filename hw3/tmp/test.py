import re
f = open("test1589171440.txt", 'r')
lines = f.readlines()
print(lines)
str4 = "".join(lines)
test = [ m.end(0) for m in re.finditer('--', str4)]
comment = str4[test[-1]:]
print(comment)
f.close()
