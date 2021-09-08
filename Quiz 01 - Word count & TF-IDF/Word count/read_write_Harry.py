import time

with open('Harry Potter.txt','r',encoding="utf-8") as f:
    read = f.readlines()

file_source = "C:\my_config\/file-source.txt"

for i in range(7,100):
    if read[i] != "\n":
        with open(file_source,'w') as file:
            file.write(read[i])
        time.sleep(1)