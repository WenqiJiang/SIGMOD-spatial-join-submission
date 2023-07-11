import numpy as np


class Resource:

	def __init__(self, LUT, FF, BRAM_36K, DSP):
		self.LUT = LUT
		self.FF = FF
		self.BRAM_36K = BRAM_36K
		self.DSP = DSP

	def print(self):
		print(f"LUT: {self.LUT}\tFF: {self.FF}\tBRAM_36K: {self.BRAM_36K}\tDSP: {self.DSP}")

def add_resource(resource_A, resource_B):

	return Resource(resource_A.LUT + resource_B.LUT,
		 resource_A.FF + resource_B.FF,
		 resource_A.BRAM_36K + resource_B.BRAM_36K,
		 resource_A.DSP + resource_B.DSP)

def get_consumption_percentage(resource, U250_resources):
	print("LUT: {:.2f}%\tFF: {:.2f}%\tBRAM_36K: {:.2f}%\tDSP: {:.2f}%".format(
		resource.LUT / U250_resources.LUT * 100,
		resource.FF / U250_resources.FF * 100,
		resource.BRAM_36K / U250_resources.BRAM_36K * 100,
		resource.DSP / U250_resources.DSP * 100,
	))

U250_resources = Resource(1728000,	3456000,	2688,	12288)

kernel_1_PE = Resource(11587, 	15295, 	66, 	20 )
print("Kernel 1 PE: ")
kernel_1_PE.print()
get_consumption_percentage(kernel_1_PE, U250_resources)

kernel_2_PE = Resource(15011, 	18960, 	98, 	26 )
print("Kernel 2 PE: ")
kernel_2_PE.print()
get_consumption_percentage(kernel_2_PE, U250_resources)

kernel_4_PE = Resource(21505, 	25771, 	162, 	42 )
print("Kernel 4 PE: ")
kernel_4_PE.print()
get_consumption_percentage(kernel_4_PE, U250_resources)

kernel_8_PE = Resource(33860, 	38900, 	290, 	74 )
print("Kernel 8 PE: ")
kernel_8_PE.print()
get_consumption_percentage(kernel_8_PE, U250_resources)

kernel_16_PE = Resource(57842, 	55256, 	754, 	138 )
print("Kernel 16 PE: ")
kernel_16_PE.print()
get_consumption_percentage(kernel_16_PE, U250_resources)

shell = Resource(188204, 	318385, 	402, 	13 )
print("Shell: ")
shell.print()
get_consumption_percentage(shell, U250_resources)

shell_and_16_PE = add_resource(kernel_16_PE, shell)
print("Kernel 16 PE + shell: ")
shell_and_16_PE.print()
get_consumption_percentage(shell_and_16_PE, U250_resources)