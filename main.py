import csv
import glob
import psycopg2
#import psycopg2.extras

def bd_con():

    #db informations

    connection = psycopg2.connect(user="",
                                    password="",
                                    host="localhost",
                                    port="5432",
                                    database="database_csv")

    connection.autocommit = True


    return connection

def insert(records):

    try:
        conn = bd_con()
        
        cur = conn.cursor()

        #public.csv_data this table must has all csv columns

        cur.execute('INSERT INTO public.csv_data ("Compet_Requerimento", "CPF_Requerente", "PIS_PASEP_NIT", "Numero_Serie_CTPS", "Numero_CTPS", "Faixa_Reincidencia", "Qtd_Reincidencias", "Habilitacao", "Cod_Habilitacao", "Qtd_Meses_Emprego_Requerente", "Ano_Status_Requerimento", "Classe_CNAE_20", "Cód_Classe_CNAE_20", "Cód_Grande_Setor_IBGE", "Cód_Ocupação_CBO", "Indicador_Mesmo_Empregador", "Inscrição_Empregador_CEICNPJ", "Faixa_Etária", "Faixa_Salarial", "Faixa_Tempo_Trabalhado", "Gênero", "Grau_Instrução", "Município_Residência", "Cod_Município_Residência", "Qtd_Parcelas_Pagas", "Qtd_Público_Prioritário", "Número_Protocolo", "Indicador_Pronatec", "Indicador_Público_Prioritário", "UF_Residência", "Vlr_Parcelas_Pagas", "Motivo_Bloqueio", "Motivo_Cancelamento", "Faixa_Tempo_Trab_MP665", "Indicador_Seguro_Completo", "Situação_Requerimento", "Data_Beneficiário", "Data_Demissão_Requerente", "Data_Segurado", "Intervalo_Segurado_Beneficiário", "Vlr_Arredond_Parc_Pagas", "Vlr_Último_Salário_CNIS", "Vlr_Último_Salário", "Vlr_Média_Salários") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', records)
        
        conn.commit()
        
        cur.close()
    except (Exception) as e:
        print (e)

    finally:
        if conn is not None:
            conn.close() 

def executa():

    #put all csv files in the same dir (basededados/)
    
    files = sorted([str(file) for file in glob.glob("basededados/*.csv")])

    for file in files:

        with open(file, encoding="ISO8859") as csvfile:
            
            reader = csv.DictReader(csvfile)
            
            records = []

            for dct in map(dict, reader):

                records.append(list(dct.values()))


            for x in records:

                one_by_one = [str(i) for i in x]
                insert(one_by_one)

executa()