import json

def generate():
	final = {}
	jdata = open('commands.json', 'r')
	data = json.load(jdata)
	for key in data:
		groupkey = data[key]['group']
		if groupkey in final.keys():
			final[groupkey].append(key)
		else:
			final[groupkey] = [key]

	c=open('Batbeetle/Commands.generated.cs', 'w')
	c.write('namespace Batbeetle\n')
	c.write('{\n')
	c.write('    public static class Commands\n')
	c.write('    {\n')
	for key in final:
		c.write('        //' + key+'\n')
		for cmd in sorted(final[key]):
			if ' ' in cmd:
				cmds = cmd.split(' ')
				for cmd2 in cmds:
					c.write('        public static readonly byte[] ' + cmd2.title() + ' = new byte[] { ' + ", ".join("0x{0:x}".format(ord(c)) for c in cmd2) + ' };\n')
			else:
				c.write('        public static readonly byte[] ' + cmd.title() + ' = new byte[] { ' + ", ".join("0x{0:x}".format(ord(c)) for c in cmd) + ' };\n')
	c.write('    }\n')
	c.write('}\n')
if __name__ == '__main__':
  generate()
