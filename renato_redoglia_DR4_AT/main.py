import pandas as pd
import json
import urllib.request
from urllib.error import HTTPError, URLError
import unicodedata
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from sqlalchemy import text


# Q.1
def carregar_dados():
    """
    Carrega os dados de um arquivo JSON e os transforma em um DataFrame Pandas.

    O arquivo carregado deve conter as colunas obrigatórias. Caso alguma esteja ausente ou com valores vazios,
    ela será preenchida com "Não Informado".

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados normalizados e preenchidos com as colunas obrigatórias.
    """
    arquivo = "INFwebNet_Data.json"

    try:
        with open(arquivo, encoding="utf-8") as f:
            dados = json.load(f)
            df = pd.DataFrame(dados)

        colunas_obrigatorias = [
            "id",
            "nome",
            "sobrenome",
            "email",
            "idade",
            "data de nascimento",
            "cidade",
            "estado",
            "hobbies",
            "linguagens de programacao",
            "jogos",
            "ano nascimento",
        ]

        for coluna in colunas_obrigatorias:
            if coluna not in df.columns:
                df[coluna] = "Não Informado"

        df = df.fillna("Não Informado")

        for coluna in df.columns:
            if df[coluna].dtype == object:
                df[coluna] = df[coluna].replace("", "Não Informado")

        return df

    except FileNotFoundError:
        print(f"O arquivo {arquivo} não foi encontrado.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados: {e}")
        return pd.DataFrame()


# Q.2
def extrair_plataformas(df):
    """
    Extrai as plataformas de jogos presentes no DataFrame e salva em um arquivo de texto.

    A função percorre a coluna 'jogos' do DataFrame, identifica as plataformas mencionadas e as salva no
    arquivo "plataformas.txt".

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna 'jogos', onde os jogos e suas plataformas estão listados.

    Returns:
        set: Um conjunto com as plataformas extraídas.
    """
    try:
        plataformas = set()

        for jogos_usuario in df["jogos"]:
            if jogos_usuario != "Não Informado":
                for jogo in jogos_usuario:
                    if len(jogo) > 1:
                        plataformas.add(jogo[1])

        with open("plataformas.txt", "w", encoding="utf-8") as f:
            for plataforma in plataformas:
                f.write(f"{plataforma}\n")

        return plataformas

    except Exception as e:
        print(f"Ocorreu um erro ao extrair as plataformas: {e}")
        return set()


# Q.3
def carregar_plataformas():
    """
    Carrega a lista de plataformas a partir do arquivo 'plataformas.txt'.

    Solicita um novo caminho caso o arquivo não seja encontrado.

    Returns:
        list: Lista com os nomes das plataformas.
    """
    arquivo = "plataformas.txt"

    while True:
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                plataformas = [linha.strip() for linha in f if linha.strip()]
            return plataformas

        except FileNotFoundError:
            print(f"O arquivo '{arquivo}' não foi encontrado.")
            novo_caminho = input(
                "\nInsira o caminho correto do arquivo ou digite 'sair' para encerrar: "
            ).strip()
            if novo_caminho.lower() == "sair":
                print("Encerrando o programa.")
                exit()
            else:
                arquivo = novo_caminho

        except Exception as e:
            print(f"Ocorreu um erro ao carregar o arquivo: {e}")
            exit()


# Q.4 / Q.5
def baixar_paginas_wikipedia(plataformas):
    """
    Baixa páginas da Wikipedia referentes às plataformas informadas.

    Salva o HTML de cada página em arquivos individuais.

    Args:
        plataformas (list): Lista com os nomes das plataformas.

    Returns:
        list: Lista com os caminhos dos arquivos HTML salvos.
    """
    caminhos_arquivos = []
    arquivo_log_erros = "erros_download.txt"

    for plataforma in plataformas:
        nome_plataforma = plataforma.replace(" ", "_")
        url = f"https://pt.wikipedia.org/wiki/Lista_de_jogos_para_{nome_plataforma}"

        try:
            resposta = urllib.request.urlopen(url)
            html = resposta.read()

            nome_arquivo = f"plataforma_{nome_plataforma}.html"
            with open(nome_arquivo, "wb") as f:
                f.write(html)

            caminhos_arquivos.append(nome_arquivo)

        except HTTPError as e:
            with open(arquivo_log_erros, "a", encoding="utf-8") as log_f:
                log_f.write(
                    f"Erro HTTP ao acessar página da plataforma {plataforma}: {e.code} - {e.reason}\n"
                )
            print(
                f"Erro HTTP ao acessar página da plataforma {plataforma}: {e.code} - {e.reason}"
            )

        except URLError as e:
            with open(arquivo_log_erros, "a", encoding="utf-8") as log_f:
                log_f.write(
                    f"Erro de URL ao acessar página da plataforma {plataforma}: {e.reason}\n"
                )
            print(
                f"Erro de URL ao acessar página da plataforma {plataforma}: {e.reason}"
            )

        except Exception as e:
            with open(arquivo_log_erros, "a", encoding="utf-8") as log_f:
                log_f.write(
                    f"Erro desconhecido ao acessar página da plataforma {plataforma}: {e}\n"
                )
            print(
                f"Erro desconhecido ao acessar página da plataforma {plataforma}: {e}"
            )

    return caminhos_arquivos


# Q.6 / Q.7
def normalizar_string(string):
    """
    Normaliza uma string removendo acentos e convertendo para minúsculas.

    Args:
        string (str): A string a ser normalizada.

    Returns:
        str: A string normalizada.
    """
    return unicodedata.normalize("NFC", string).lower()


class ParseException(Exception):
    """
    Exceção personalizada para erros de parse durante a validação dos dados extraídos.
    """

    def __init__(self, mensagem):
        self.mensagem = mensagem
        super().__init__(self.mensagem)


def parsear_paginas(caminhos_arquivos_html):
    """
    Faz o parsing de páginas HTML baixadas, extrai dados sobre jogos e valida o título da página.

    A função verifica se o título da página corresponde ao nome da plataforma e, se válido, extrai
    as tabelas de jogos disponíveis no HTML.

    Args:
        caminhos_arquivos_html (list): Lista com os caminhos para os arquivos HTML das plataformas.

    Returns:
        list: Uma lista de dicionários contendo dados dos jogos extraídos por plataforma.
    """
    resultados = []

    for caminho_arquivo_html in caminhos_arquivos_html:
        try:
            with open(caminho_arquivo_html, "r", encoding="utf-8") as f:
                conteudo_html = f.read()

            soup = BeautifulSoup(conteudo_html, "html.parser")
            titulo_pagina = soup.title.string if soup.title else ""
            nome_plataforma = (
                caminho_arquivo_html.split("plataforma_")[1]
                .split(".html")[0]
                .replace("_", " ")
            )

            if not normalizar_string(nome_plataforma) in normalizar_string(
                titulo_pagina
            ):
                raise ParseException(
                    f"O título da página '{titulo_pagina}' não corresponde ao nome da plataforma '{nome_plataforma}'."
                )

            print(
                f"Título da página validado com sucesso para a plataforma '{nome_plataforma}'."
            )

            tabelas = soup.find_all("table", class_="wikitable")
            jogos = []

            for tabela in tabelas:
                cabecalho = tabela.find("tr")
                if not cabecalho:
                    continue

                colunas = [th.get_text(strip=True) for th in cabecalho.find_all("th")]

                for linha in tabela.find_all("tr")[1:]:
                    dados_jogo = {}
                    colunas_valores = linha.find_all("td")

                    if len(colunas_valores) == len(colunas):
                        for idx, coluna_valor in enumerate(colunas_valores):
                            campo = colunas[idx]
                            valor = coluna_valor.get_text(strip=True)
                            dados_jogo[campo] = valor

                        nome_jogo = dados_jogo.get("Título") or dados_jogo.get(
                            "Jogo", "Desconhecido"
                        )
                        jogos.append({"nome_jogo": nome_jogo, "dados_jogo": dados_jogo})

            resultado = {"plataforma": nome_plataforma, "jogos": jogos}

            print(f"Dados extraídos para a plataforma '{nome_plataforma}'.")
            resultados.append(resultado)

        except ParseException as e:
            with open("erros_parse.txt", "a", encoding="utf-8") as log_f:
                log_f.write(
                    f"Erro ao validar o título da página '{caminho_arquivo_html}': {e.mensagem}\n"
                )
            print(
                f"Erro ao validar o título da página '{caminho_arquivo_html}': {e.mensagem}"
            )

        except Exception as e:
            print(
                f"Ocorreu um erro ao tentar parsear a página '{caminho_arquivo_html}': {e}"
            )
            with open("erros_parse.txt", "a", encoding="utf-8") as log_f:
                log_f.write(
                    f"Erro desconhecido ao tentar parsear a página '{caminho_arquivo_html}': {e}\n"
                )

    return resultados


# Q.8
def extrair_urls_emails(caminhos_arquivos_html):
    conexoes = {"urls": [], "emails": []}

    regex_urls = r"https?://[a-zA-Z0-9-._~:/?#[\]@!$&\'()*+,;=]+"
    regex_emails = r"^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$"

    for caminho_arquivo_html in caminhos_arquivos_html:
        try:
            with open(caminho_arquivo_html, "r", encoding="utf-8") as f:
                conteudo_html = f.read()

            soup = BeautifulSoup(conteudo_html, "html.parser")

            urls_encontradas = re.findall(regex_urls, conteudo_html)
            conexoes["urls"].extend(urls_encontradas)

            emails_encontrados = re.findall(regex_emails, conteudo_html)
            conexoes["emails"].extend(emails_encontrados)

            print(
                f"URLs e e-mails extraídos com sucesso para a plataforma '{caminho_arquivo_html}'."
            )

        except Exception as e:
            print(
                f"Ocorreu um erro ao processar o arquivo '{caminho_arquivo_html}': {e}"
            )

    with open("conexoes_plataformas.json", "w", encoding="utf-8") as f:
        json.dump(conexoes, f, ensure_ascii=False, indent=4)

    print("Conexões salvas em 'conexoes_plataformas.json'.")
    return conexoes


# Q.9
def exportar_dados_jogos(dados_extraidos):
    try:
        arquivo = "dados_jogos_plataformas.json"

        with open(arquivo, "w", encoding="utf-8") as f:
            json.dump(dados_extraidos, f, ensure_ascii=False, indent=4)

        print(f"Dados dos jogos exportados com sucesso para o arquivo '{arquivo}'.")

    except Exception as e:
        print(f"Ocorreu um erro ao exportar os dados dos jogos: {e}")


# Q.10
def associar_jogos_usuarios(df_usuarios, dados_jogos_extraidos):
    try:
        df_usuarios["jogos_associados"] = None

        for idx, usuario in df_usuarios.iterrows():
            jogos_associados = []

            for jogo_usuario, plataforma_usuario in usuario["jogos"]:
                for plataforma in dados_jogos_extraidos:
                    if plataforma["plataforma"].lower() == plataforma_usuario.lower():
                        for jogo in plataforma["jogos"]:
                            if jogo["nome_jogo"].lower() == jogo_usuario.lower():
                                jogos_associados.append(
                                    {
                                        "nome_jogo": jogo_usuario,
                                        "plataforma": plataforma_usuario,
                                    }
                                )

            df_usuarios.at[idx, "jogos_associados"] = jogos_associados

        print("Jogos associados com sucesso!")
        return df_usuarios

    except Exception as e:
        print(f"Ocorreu um erro ao associar os jogos aos usuários: {e}")
        return df_usuarios


# Q.11
def atualizar_banco_dados(dados_jogos_extraidos):
    try:
        engine = create_engine("sqlite:///INFwebNET_DB.db")
        jogos_plataformas = []

        for plataforma in dados_jogos_extraidos:
            nome_plataforma = plataforma["plataforma"]
            for jogo in plataforma["jogos"]:
                nome_jogo = jogo["nome_jogo"]
                dados_jogo = jogo["dados_jogo"]
                dados_jogo_json = json.dumps(dados_jogo)

                jogos_plataformas.append(
                    {
                        "nome_plataforma": nome_plataforma,
                        "nome_jogo": nome_jogo,
                        "dados_jogo": dados_jogo_json,
                    }
                )

        df_jogos_plataformas = pd.DataFrame(jogos_plataformas)

        inspector = inspect(engine)
        if inspector.has_table("Jogos_Plataformas"):
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS Jogos_Plataformas"))

        df_jogos_plataformas.to_sql(
            "Jogos_Plataformas", engine, index=False, if_exists="replace"
        )

        print("Tabela 'Jogos_Plataformas' atualizada com sucesso.")

    except SQLAlchemyError as e:
        print(f"Erro ao atualizar o banco de dados: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")


# Q.12
def consultar_usuarios_por_jogo():
    nome_jogo = input("Digite o nome do jogo que deseja consultar: ").strip()

    if not nome_jogo:
        print("O nome do jogo não pode ser vazio.")
        return []

    try:
        engine = create_engine("sqlite:///INFwebNET_DB.db")
        with engine.connect() as conn:
            query = text(
                """
                SELECT nome, sobrenome 
                FROM Usuarios 
                WHERE jogos LIKE :jogo
                """
            )

            resultado = conn.execute(query, {"jogo": f"%{nome_jogo}%"})
            usuarios = resultado.fetchall()

            if usuarios:
                print(f"Usuários que jogam '{nome_jogo}':")
                for usuario in usuarios:
                    print(f"- {usuario[0]} {usuario[1]}")
            else:
                print(f"Nenhum usuário encontrado para o jogo '{nome_jogo}'.")

            return usuarios

    except SQLAlchemyError as e:
        print(f"Erro ao consultar o banco de dados: {e}")
        return []
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        return []


if __name__ == "__main__":
    df_usuarios = carregar_dados()
    print(df_usuarios)
    print()

    plataformas = extrair_plataformas(df_usuarios)
    print("Plataformas extraídas:", plataformas)
    print()

    plataformas_carregadas = carregar_plataformas()
    print("Plataformas carregadas:", plataformas_carregadas)
    print()

    caminhos_paginas_baixadas = baixar_paginas_wikipedia(plataformas)
    print("Caminhos para os arquivos gerados:", caminhos_paginas_baixadas)
    print()

    dados_jogos_plataformas = parsear_paginas(caminhos_paginas_baixadas)
    print()

    conexoes_plataformas = extrair_urls_emails(caminhos_paginas_baixadas)
    print("Urls:", conexoes_plataformas["urls"][:5])
    print("E-mails:", conexoes_plataformas["emails"][:5])
    print()

    exportar_dados_jogos(dados_jogos_plataformas)
    print()

    df_usuarios_atualizado = associar_jogos_usuarios(
        df_usuarios, dados_jogos_plataformas
    )
    print(df_usuarios_atualizado.iloc[50].to_dict())
    print()

    atualizar_banco_dados(dados_jogos_plataformas)
    print()

    consultar_usuarios_por_jogo()
    print()
