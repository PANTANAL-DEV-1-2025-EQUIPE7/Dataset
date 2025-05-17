import basedosdados as bd
import pandas as pd
import os
import json
from time import sleep

GOOGLE_APPLICATION_CREDENTIALS= "./chave.json"


BILLING_ID = "uplifted-smile-412914"

OUTPUT_DIR = "./dados_estban"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")

anos = list(range(1988, 2025))
meses = list(range(1, 13))

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        checkpoint = json.load(f)
else:
    checkpoint = {}

def save_checkpoint(ano, mes):
    checkpoint["ultimo_ano"] = ano
    checkpoint["ultimo_mes"] = mes
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)

def ja_baixado(ano, mes):
    filename = f"dados_ano_{ano}_mes_{mes:02d}.csv"
    return os.path.exists(os.path.join(OUTPUT_DIR, filename))

for ano in anos:
    for mes in meses:
        if checkpoint.get("ultimo_ano", 0) > ano or \
           (checkpoint.get("ultimo_ano", 0) == ano and checkpoint.get("ultimo_mes", 0) >= mes):
            print(f"PULANDO {ano}-{mes:02d} (já feito)")
            continue

        if ja_baixado(ano, mes):
            print(f"Arquivo já existe: {ano}-{mes:02d}")
            save_checkpoint(ano, mes)
            continue

        print(f"Baixando dados para: {ano}-{mes:02d}")
        query = f"""
        SELECT *
        FROM `basedosdados.br_bcb_estban.municipio`
        WHERE ano = {ano} AND mes = {mes}
        """

        try:
            df = bd.read_sql(query, billing_project_id=BILLING_ID)
            if df.empty:
                print(f"Nenhum dado encontrado para {ano}-{mes:02d}")
            else:
                output_file = os.path.join(OUTPUT_DIR, f"dados_ano_{ano}_mes_{mes:02d}.csv")
                df.to_csv(output_file, index=False)
                print(f"Salvo: {output_file}")
            
            save_checkpoint(ano, mes)
            
            sleep(2)

        except Exception as e:
            print(f"Erro em {ano}-{mes:02d}: {e}")
            print("Tentando novamente após pausa de 10s...")
            sleep(10)
            continue

print("Finalizado!")
