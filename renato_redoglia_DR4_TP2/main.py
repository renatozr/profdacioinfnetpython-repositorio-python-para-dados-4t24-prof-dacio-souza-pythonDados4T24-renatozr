from datetime import datetime
import csv
import json
import secrets
import ast
from collections import Counter

import pandas as pd

# 1. Abrindo as Portas
infnetianos = []

with open("./brutos/rede_INFNET_atualizado.txt", "r", encoding="utf-8") as arquivo_txt:
    for linha in arquivo_txt:
        [nome, idade, cidade, estado, *resto] = linha.strip().split("?")
        infnetianos.append([nome, idade, cidade, estado])

with open("./gerados/INFwebNet.csv", "w", newline="", encoding="utf-8") as arquivo_csv:
    csv.writer(arquivo_csv).writerows(infnetianos)

# 2. Estruturando os Dados
infnetianos = []

with open("./gerados/INFwebNet.csv", "r", encoding="utf-8") as arquivo_csv:
    leitor_csv = csv.DictReader(arquivo_csv)

    for linha in leitor_csv:
        infnetianos.append(
            {
                "nome": linha["nome"],
                "idade": int(linha["idade"]),
                "cidade": linha["cidade"],
                "estado": linha["estado"],
            }
        )

with open("./gerados/INFwebNET.json", "w", encoding="utf-8") as arquivo_json:
    json.dump(infnetianos, arquivo_json, ensure_ascii=False, indent=4)

# 3. Cadastro Simplificado
print("** Cadastro Simplificado **")
with open("./gerados/INFwebNET.json", "r", encoding="utf-8") as arquivo_json:
    infnetianos = json.load(arquivo_json)

while True:
    cadastrar_infnetiano = input('\nDigite "sim" para inserir um novo infnetiano: ')

    if cadastrar_infnetiano != "sim":
        print("\n** Fim do Cadastro Simplificado **\n")
        break

    infnetiano = {
        "nome": input("Nome: "),
        "idade": int(input("Idade: ")),
        "cidade": input("Cidade: "),
        "estado": input("Estado: "),
    }

    infnetianos.append(infnetiano)

with open("./gerados/INFwebNET.json", "w", encoding="utf-8") as arquivo_json:
    json.dump(infnetianos, arquivo_json, ensure_ascii=False, indent=4)

# 4. Análise com Pandas
with open("./gerados/INFwebNET.json", "r", encoding="utf-8") as arquivo_json:
    infnetianos = json.load(arquivo_json)

infnetianos_df = pd.DataFrame(infnetianos)
infnetianos_idade_media = infnetianos_df["idade"].mean()

print("**")
print(f"\nA média de idade dos infnetianos é {infnetianos_idade_media:.2f}")
print("\n**\n")


# 5. Ampliando as Informações
def inserir_infnetianos():
    print("** Cadastro Ampliado **")
    with open("./gerados/INFwebNET.json", "r", encoding="utf-8") as arquivo_json:
        infnetianos = json.load(arquivo_json)

    infnetianos = [
        {
            "nome": infnetiano["nome"],
            "idade": infnetiano["idade"],
            "cidade": infnetiano["cidade"],
            "estado": infnetiano["estado"],
            "hobbies": [],
            "linguagens de programação": [],
            "jogos": [],
        }
        for infnetiano in infnetianos
    ]

    while True:
        cadastrar_infnetiano = input('\nDigite "sim" para inserir um novo infnetiano: ')

        if cadastrar_infnetiano != "sim":
            print("\n** Fim do Cadastro Ampliado **\n")
            break

        nome = input("Nome: ")
        idade = int(input("Idade: "))
        cidade = input("Cidade: ")
        estado = input("Estado: ")

        hobbies_input = input("Hobbies (separados por vírgula): ")
        hobbies = [] if hobbies_input == "" else hobbies_input.split(",")

        linguagens_input = input("Linguagens de programação (separadas por vírgula): ")
        linguagens = [] if linguagens_input == "" else linguagens_input.split(",")

        jogos = []
        print("Jogos Favoritos: ")
        while True:
            nome_do_jogo = input('Nome do jogo (ou "sair" para parar): ')
            if nome_do_jogo == "sair":
                break
            plataforma = input("Plataforma: ")
            jogos.append({"nome": nome_do_jogo, "plataforma": plataforma})

        infnetiano = {
            "nome": nome,
            "idade": idade,
            "cidade": cidade,
            "estado": estado,
            "hobbies": [hobby.strip() for hobby in hobbies],
            "linguagens de programação": [
                linguagem.strip() for linguagem in linguagens
            ],
            "jogos": jogos,
        }

        infnetianos.append(infnetiano)

    with open("./gerados/INFwebNET.json", "w", encoding="utf-8") as arquivo_json:
        json.dump(infnetianos, arquivo_json, ensure_ascii=False, indent=4)


inserir_infnetianos()

# 6. Dados Delimitados
with open("./brutos/dados_usuarios_novos.txt", "r", encoding="utf-8") as arquivo_txt:
    leitor_csv = csv.DictReader(arquivo_txt, delimiter=";")

# 7. Organizando a Bagunça
with open("./gerados/INFwebNET.json", "r", encoding="utf-8") as arquivo_json:
    infnetianos = json.load(arquivo_json)

infnetianos = [
    {
        "id": secrets.token_hex(4),
        "nome": infnetiano["nome"],
        "sobrenome": "",
        "email": "",
        "idade": infnetiano["idade"],
        "data de nascimento": datetime(datetime.now().year - infnetiano["idade"], 1, 1),
        "cidade": infnetiano["cidade"],
        "estado": infnetiano["estado"],
        "hobbies": infnetiano["hobbies"],
        "linguagens de programação": infnetiano["linguagens de programação"],
        "jogos": [(jogo["nome"], jogo["plataforma"]) for jogo in infnetiano["jogos"]],
    }
    for infnetiano in infnetianos
]

with open("./brutos/dados_usuarios_novos.txt", "r", encoding="utf-8") as arquivo_txt:
    leitor_csv = csv.DictReader(arquivo_txt, delimiter=";")

    for linha in leitor_csv:
        idade = None if linha["idade"] == "" else int(float(linha["idade"]))

        data_de_nascimento = None
        formatos_de_data = [
            "%Y-%m-%d",
            "%b %d, %Y",
            "%d/%m/%y",
            "%Y-%m-%d",
        ]
        for formato in formatos_de_data:
            try:
                data = datetime.strptime(linha["data de nascimento"], formato)
                data_de_nascimento = data
            except ValueError:
                continue

        infnetianos.append(
            {
                "id": linha["id"],
                "nome": linha["nome"],
                "sobrenome": linha["sobrenome"],
                "email": linha["email"],
                "idade": idade,
                "data de nascimento": data_de_nascimento,
                "cidade": linha["cidade"],
                "estado": linha["estado"],
                "hobbies": ast.literal_eval(linha["hobbies"]),
                "linguagens de programação": ast.literal_eval(
                    linha["linguagens de programação"]
                ),
                "jogos": ast.literal_eval(linha["jogos"]),
            }
        )

infnetianos_df = pd.DataFrame(infnetianos)

# 8. Criando Informações
infnetianos_df["ano nascimento"] = (
    datetime.now().year - infnetianos_df["idade"].fillna(0)
).astype(int)


# 9. Completando os Dados
# Utilizamos o campo "data de nascimento", pois esta é a fonte mais precisa para determinar a idade. Evita-se, assim, possíveis inconsistências.
def completando_idade(x):
    if pd.isna(x["idade"]) and pd.notna(x["data de nascimento"]):
        return int(datetime.now().year - x["data de nascimento"].year)
    else:
        return x["idade"]


infnetianos_df["idade"] = infnetianos_df.apply(completando_idade, axis=1)


# Quando só a idade está disponível, estimamos a data de nascimento assumindo o dia 1º de janeiro. Embora seja uma simplificação, é uma prática comum quando se trabalha com dados incompletos.
def completando_data_de_nascimento(x):
    if pd.isna(x["data de nascimento"]) and pd.notna(x["idade"]):
        return datetime(x["ano nascimento"], 1, 1)
    else:
        return x["data de nascimento"]


infnetianos_df["data de nascimento"] = infnetianos_df.apply(
    completando_data_de_nascimento, axis=1
)

# 10. Guardando as Informações
infnetianos_df["data de nascimento"] = infnetianos_df["data de nascimento"].dt.strftime(
    "%Y-%m-%d"
)
infnetianos_df.to_json(
    "./gerados/INFwebNet_Data.json", orient="records", force_ascii=False, indent=4
)

# 11. Selecionando Grupos
siglas_estados = infnetianos_df["estado"].unique()

for sigla in siglas_estados:
    df_estado = infnetianos_df[infnetianos_df["estado"] == sigla]

    df_estado.to_csv(
        f"./gerados/grupo_{sigla}.csv", index=False, encoding="utf-8", sep=";"
    )


# 12. Agrupando INFNETianos
def filtrar_por_ano_nascimento(ano_inicial, ano_final):
    infnetianos_df["data de nascimento"] = pd.to_datetime(
        infnetianos_df["data de nascimento"]
    )

    filtro = (infnetianos_df["data de nascimento"].dt.year >= ano_inicial) & (
        infnetianos_df["data de nascimento"].dt.year <= ano_final
    )
    infnetianos_filtrados = infnetianos_df[filtro]

    print(infnetianos_filtrados)


filtrar_por_ano_nascimento(2000, 2010)


# 13. Selecionando INFNETiano
def buscar_infnetiano(nome):
    resultados = infnetianos_df[
        infnetianos_df["nome"].str.contains(nome, case=False, na=False)
    ]

    if resultados.empty:
        print(f"Nenhum INFNETiano encontrado com o nome '{nome}'.")
        return None

    print("\nUsuários encontrados:")
    for index, row in resultados.iterrows():
        print(
            f"[{index}] Nome: {row['nome']}, Idade: {row['idade']}, Cidade: {row['cidade']}"
        )

    try:
        indice = int(input("\nDigite o índice do INFNETiano que deseja selecionar: "))
        if indice in resultados.index:
            return resultados.loc[indice].to_dict()
        else:
            print("Índice inválido.")
            return None
    except ValueError:
        print("Entrada inválida.")
        return None


infnetiano_selecionado = buscar_infnetiano("João")


# 14. Atualizando Dados
def atualizar_infnetiano(infnetiano):
    index = infnetianos_df[infnetianos_df["nome"] == infnetiano["nome"]].index[0]

    infnetiano["idade"] = int(
        input(f"Nova idade ({infnetiano['idade']}): ") or infnetiano["idade"]
    )
    infnetiano["cidade"] = (
        input(f"Nova cidade ({infnetiano['cidade']}): ") or infnetiano["cidade"]
    )
    infnetiano["estado"] = (
        input(f"Novo estado ({infnetiano['estado']}): ") or infnetiano["estado"]
    )

    hobbies = input(
        f"Novos hobbies (máx 5, separados por vírgula) [{', '.join(infnetiano['hobbies'])}]: "
    )
    infnetiano["hobbies"] = (
        [h.strip() for h in hobbies.split(",")[:5]]
        if hobbies
        else infnetiano["hobbies"]
    )

    jogos = []
    print("Atualize os jogos (máx 5):")
    for _ in range(5):
        nome_jogo = input("Nome do jogo (ou pressione Enter para parar): ")
        if not nome_jogo:
            break
        plataforma = input("Plataforma: ")
        jogos.append((nome_jogo.strip(), plataforma.strip()))
    infnetiano["jogos"] = jogos if jogos else infnetiano["jogos"]

    infnetianos_df.loc[index] = infnetiano

    print()
    print(infnetiano)
    print("\nDados atualizados com sucesso!")
    return infnetianos_df


atualizar_infnetiano(infnetiano_selecionado)


# 15. Trending
def linguagens_trending():
    linguagens = [
        linguagem
        for lista in infnetianos_df["linguagens de programação"]
        for linguagem in lista
    ]
    contagem = Counter(linguagens)
    top_5 = contagem.most_common(5)

    print("\nTop 5 Linguagens de Programação:")
    for linguagem, qtd in top_5:
        print(f"{linguagem}: {qtd} vezes")


linguagens_trending()
