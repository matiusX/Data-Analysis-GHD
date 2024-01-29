import json
import os
import re

path_repos = os.listdir("/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/data/raw")
for repo in path_repos:
    path_files = "/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/data/raw/" + repo
    files_names = os.listdir(path_files)

    file_out = open("/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/data/transformed/" + repo + ".txt", "w")

    for file_name in files_names:
        file_name_json= path_files + "/" + file_name
        file_json = open(file_name_json, "r")

        data_json = json.load(file_json)

        padrao_regex = r"discussions/(\d+)"
        id = re.search(padrao_regex, data_json["url"]).group(1)
        titulo = data_json["title"]
        categoria = data_json["category"]["name"]
        quantidade_comentario = len(data_json["comments"])
        data_postagem = data_json["date"]
        if(data_json["answer"] != None):
            bool_answer = 1
            data_resposta = data_json["answer"]["publishedAt"]
        else:
            bool_answer = 0
            data_resposta = None

        data = f"{id},__,{titulo},__,{categoria},__,{quantidade_comentario},__,{data_postagem},__,{bool_answer},__,{data_resposta}\n"
        file_out.write(data)
        
        '''
        print("ID:", id)
        print("Título:", data_json["title"])
        print("Quantidade Comentarios:", len(data_json["comments"]))
        print("Categoria:", data_json["category"]["name"])
        print("Data:", data_json["date"])

        #print("Answer:", data_json["answer"])
        if(data_json["answer"] != None):
            print("Answer:", 1)#1 foi respondido, 0 nao foi
            print("Answered At:", data_json["answer"]["publishedAt"])
        '''
        file_json.close()

    file_out.close()

#id,titulo,categoria,quantidade_comentarios,data_postagem,bool_respondida,data_resposta(pode None)