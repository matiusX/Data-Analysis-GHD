import re
file = open("/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/reposColeta.csv", "r")
file_out = open("/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/resources/repositories.txt", "w")
lines = file.readlines()

for line in lines:
    match = re.match(r'([^;]+);([^;]+)',line)
    if match:
        name = match.group(1)
        repo = match.group(2).rstrip('\n')
        file_out.write(f"{name} {repo}\n")

file.close()
file_out.close()