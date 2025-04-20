import pandas as pd
import re
import pdfplumber
from datetime import datetime
from pathlib import Path


def parse_csv_nubank(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={
        df.columns[0]: 'Data',
        df.columns[1]: 'Descricao',
        df.columns[2]: 'Valor'
    })
    df['Data'] = pd.to_datetime(df['Data'], dayfirst=True)
    df['Cartao'] = 'Nubank'
    df['Mes_Ano'] = df['Data'].dt.strftime('%m/%Y')
    return df[['Data', 'Descricao', 'Valor', 'Cartao', 'Mes_Ano']]


def parse_pdf_nubank(file_path):
    dados = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            linhas = text.split('\n')
            for linha in linhas:
                match = re.match(r'(\d{2} [A-Z]{3}) .*? (R\$ [\d,.]+)', linha)
                if match:
                    partes = linha.split('R$')
                    data_str = partes[0][:6].strip()
                    descricao = partes[0][6:].strip()
                    valor_str = partes[1].strip().replace('.', '').replace(',', '.')
                    try:
                        data = datetime.strptime(data_str + ' 2025', '%d %b %Y')
                        valor = float(valor_str)
                        dados.append({
                            'Data': data,
                            'Descricao': descricao,
                            'Valor': valor,
                            'Cartao': 'Nubank',
                            'Mes_Ano': data.strftime('%m/%Y')
                        })
                    except:
                        continue
    return pd.DataFrame(dados)


def parse_pdf_picpay(file_path):
    dados = []
    mes_ano_fatura = None
    padrao_linha = re.compile(r'^(\d{2}/\d{2})\s+(.+?)\s+(\d{1,3}(?:\.\d{3})*,\d{2})$')

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            linhas = text.split('\n')
            for linha in linhas:
                if not mes_ano_fatura:
                    match_mes = re.search(r'Resumo - Mês de ([a-zç]+)', linha, re.IGNORECASE)
                    if match_mes:
                        nome_mes = match_mes.group(1).strip().lower()
                        meses = {
                            'janeiro': '01', 'fevereiro': '02', 'março': '03', 'marco': '03',
                            'abril': '04', 'maio': '05', 'junho': '06', 'julho': '07',
                            'agosto': '08', 'setembro': '09', 'outubro': '10',
                            'novembro': '11', 'dezembro': '12'
                        }
                        if nome_mes in meses:
                            mes_ano_fatura = f"{meses[nome_mes]}/2025"

                match = padrao_linha.match(linha.strip())
                if match:
                    data_txt, descricao, valor_txt = match.groups()
                    if 'pagamento de fatura' in descricao.lower():
                        continue  # Ignora pagamentos de fatura
                    try:
                        data = datetime.strptime(data_txt + "/2025", '%d/%m/%Y')
                        valor = float(valor_txt.replace('.', '').replace(',', '.'))
                        dados.append({
                            'Data': data,
                            'Descricao': descricao.strip(),
                            'Valor': valor,
                            'Cartao': 'PicPay',
                            'Mes_Ano': data.strftime('%m/%Y')
                        })
                    except Exception as e:
                        print(f"Erro ao processar linha: {linha} -> {e}")
                        continue

    return pd.DataFrame(dados)


def consolidar_faturas(df_list):
    df_total = pd.concat(df_list, ignore_index=True)
    df_total = df_total.sort_values(by='Data')
    return df_total


def gerar_relatorio_mensal(df):
    print("\n=== Gastos Mensais por Cartão ===")
    print(df.groupby(['Mes_Ano', 'Cartao'])['Valor'].sum().round(2))

    print("\n=== Gastos Totais por Mês ===")
    print(df.groupby('Mes_Ano')['Valor'].sum().round(2))


# Exemplo de uso
if __name__ == '__main__':
    pasta_dados = Path('dados')
    arquivos = list(pasta_dados.glob('*'))
    df_todos = []

    for arquivo in arquivos:
        nome = arquivo.stem.lower()
        if arquivo.suffix.lower() == '.csv' and 'nubank' in nome:
            print(f"Lendo CSV: {arquivo.name}")
            df_todos.append(parse_csv_nubank(arquivo))
        elif arquivo.suffix.lower() == '.pdf':
            if 'nubank' in nome:
                print(f"Lendo PDF Nubank: {arquivo.name}")
                df_todos.append(parse_pdf_nubank(arquivo))
            elif 'picpay' in nome:
                print(f"Lendo PDF PicPay: {arquivo.name}")
                df_todos.append(parse_pdf_picpay(arquivo))

    if df_todos:
        df_consolidado = consolidar_faturas(df_todos)
        gerar_relatorio_mensal(df_consolidado)
        df_consolidado.to_csv('gastos_consolidados.csv', index=False)
        print("\nArquivo 'gastos_consolidados.csv' salvo com sucesso.")
    else:
        print("Nenhum arquivo processado.")
