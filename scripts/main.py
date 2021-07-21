import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input',
                    dest='input_directory',
                    type=str,
                    help='a directory containing the input files'
                    )

parser.add_argument('-o', '-output',
                    dest='output_directory',
                    type=str,
                    help='the directoy where output files are written'
                    )

args = parser.parse_args()

DEFAULT_INPUT_DIRECTORY = '../block_data'
DEFUALT_OUTPUT_DIRECTORY = '../csv_files'

if args.input_directory:
    input_directory = args.input_directory
    print(f'using user-specified input directory {input_directory}')
else:
    input_directory = DEFAULT_INPUT_DIRECTORY
    print(f'using default input directory {input_directory}')

if args.output_directory:
    output_directory = args.output_directory
    print(f'using user-specified output directory {output_directory}')
else:
    output_directory = DEFUALT_OUTPUT_DIRECTORY
    print(f'using default output directory {output_directory}')