import os
import json

def processar_pasta(diretorio):
    arquivos = os.listdir(diretorio)

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)

        if os.path.isfile(caminho_arquivo):
            try:
                with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo_atual:
                    dados = json.load(arquivo_atual)

                    if 'answer' in dados and 'comments' in dados and dados['answer'] is None and dados['comments'] == []:
                        os.remove(caminho_arquivo)
                        print(f"Arquivo removido: {caminho_arquivo}")
                
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        elif os.path.isdir(caminho_arquivo):
            processar_pasta(caminho_arquivo)

diretorio_principal = "/home/matius/Documentos/marcinha/discussions-labeled-dataset-main/data/raw/"

processar_pasta(diretorio_principal)
