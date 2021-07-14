class Transaction:
	def __init__(self, hash='', inputs=[], outputs=[], fee=0, type='unclassified'):
		self.hash = hash
		self.inputs = inputs
		self.outputs = outputs
		self.fee = fee
		self.type = type
		
	def is_coinbase(self):
		return self.inputs == [('coinbase',)]

	def from_csv_string(self, csv_string):
		fields = csv_string.split(',')

		self.hash = fields[0]

		input_addresses = fields[1].split(':')
		
		if fields[2]:
			input_values = [int(x) for x in fields[2].split(':')]
		else:
			input_values = []

		self.inputs = list(zip(input_addresses, input_values))

		output_addresses = fields[3].split(':')
		output_values = [int(x) for x in fields[4].split(':')]
		self.outputs = list(zip(output_addresses, output_values))

		self.fee = int(fields[5])

	def to_csv_string(self):
		input_addresses = ':'.join([input[0] for input in self.inputs])
		input_values = ':'.join([str(input[1]) for input in self.inputs])

		output_addresses = ':'.join([output[0] for output in self.outputs])
		output_values = ':'.join([str(output[1]) for output in self.outputs])

		csv_string = f'{self.hash},{input_addresses},{input_values},{output_addresses},{output_values},{self.fee},{self.type}\n'
		return csv_string