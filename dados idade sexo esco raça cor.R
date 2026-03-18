# ==============================================================================
# SCRIPT COMPLETO E FINAL: PNADC - Dados Sociodemográficos (2018-2026)
# ATUALIZAÇÃO: Inclusão da variável Idade (V2009) e criação de Faixa Etária
# CORREÇÃO: Download direto via BigQuery (Evita erro de ficheiro temporário no Windows)
# ==============================================================================

# 1. Instalação e carregamento dos pacotes necessários
if (!require("dplyr")) install.packages("dplyr") 
if (!require("basedosdados")) install.packages("basedosdados")
if (!require("writexl")) install.packages("writexl")
if (!require("bigrquery")) install.packages("bigrquery")

library(basedosdados)
library(writexl)
library(dplyr)
library(bigrquery)

# ==============================================================================
# 2. AUTENTICAÇÃO E CONFIGURAÇÃO
# ==============================================================================
options(timeout = 600) # Aumenta o tempo limite de rede para evitar quebras
bq_auth(email = "livramkurios@gmail.com")
set_billing_id("teak-hearth-439318-i1")

# ==============================================================================
# 3. EXTRAÇÃO DE DADOS (QUERY SQL DIRETA NO BIGQUERY)
# ==============================================================================
# Adicionada a variável V2009 (Idade do morador na data de referência)
query <- "
SELECT
    ano,
    sigla_uf,
    V2009 as idade,
    V2010 as raca_cor_codigo,
    V2007 as sexo_codigo,
    VD3004 as instrucao_codigo,
    SUM(V1028) as populacao_estimada
FROM `basedosdados.br_ibge_pnadc.microdados`
WHERE ano BETWEEN 2018 AND 2026
  AND trimestre = 4
GROUP BY ano, sigla_uf, V2009, V2010, V2007, VD3004
ORDER BY ano, sigla_uf, V2009, V2010, V2007, VD3004
"

cat("A processar a consulta nos servidores da Google...\n")
# Em vez de read_sql, usamos o bq_project_query diretamente para contornar o bug
tabela_temp <- bq_project_query(x = get_billing_id(), query = query)

cat("A descarregar os dados diretamente para a memória (Isto evita o erro da pasta Temp)...\n")
# O page_size grande força o download de uma só vez, sem criar dezenas de ficheiros .json
df_pnadc <- bq_table_download(tabela_temp, page_size = 100000)

# ==============================================================================
# 4. TRATAMENTO, TRADUÇÃO E CÁLCULOS
# ==============================================================================
cat("A processar e a traduzir as variáveis...\n")
df_completo <- df_pnadc %>%
  mutate(
    # Garantir que a idade é tratada como número para podermos criar as faixas
    idade = as.numeric(idade),
    
    # Criar faixas etárias padrão (opcional, mas muito útil para análise)
    faixa_etaria = case_when(
      idade <= 14 ~ "0 a 14 anos",
      idade >= 15 & idade <= 29 ~ "15 a 29 anos",
      idade >= 30 & idade <= 49 ~ "30 a 49 anos",
      idade >= 50 & idade <= 64 ~ "50 a 64 anos",
      idade >= 65 ~ "65 anos ou mais",
      TRUE ~ "Idade Ignorada"
    ),
    raca_cor = case_when(
      as.character(raca_cor_codigo) == "1" ~ "Branca",
      as.character(raca_cor_codigo) == "2" ~ "Preta",
      as.character(raca_cor_codigo) == "3" ~ "Amarela",
      as.character(raca_cor_codigo) == "4" ~ "Parda",
      as.character(raca_cor_codigo) == "5" ~ "Indígena",
      as.character(raca_cor_codigo) == "9" ~ "Ignorado",
      TRUE ~ "Não Informado"
    ),
    sexo = case_when(
      as.character(sexo_codigo) == "1" ~ "Homem",
      as.character(sexo_codigo) == "2" ~ "Mulher",
      TRUE ~ "Não Informado"
    ),
    nivel_instrucao = case_when(
      as.character(instrucao_codigo) == "1" ~ "Sem instrução e menos de 1 ano de estudo",
      as.character(instrucao_codigo) == "2" ~ "Fundamental incompleto ou equivalente",
      as.character(instrucao_codigo) == "3" ~ "Fundamental completo ou equivalente",
      as.character(instrucao_codigo) == "4" ~ "Médio incompleto ou equivalente",
      as.character(instrucao_codigo) == "5" ~ "Médio completo ou equivalente",
      as.character(instrucao_codigo) == "6" ~ "Superior incompleto ou equivalente",
      as.character(instrucao_codigo) == "7" ~ "Superior completo",
      TRUE ~ "Não Aplicável / Menores de 5 anos"
    ),
    regiao = case_when(
      sigla_uf %in% c("AM", "RR", "AP", "PA", "TO", "RO", "AC") ~ "Norte",
      sigla_uf %in% c("MA", "PI", "CE", "RN", "PE", "PB", "SE", "AL", "BA") ~ "Nordeste",
      sigla_uf %in% c("MT", "MS", "GO", "DF") ~ "Centro-Oeste",
      sigla_uf %in% c("SP", "RJ", "ES", "MG") ~ "Sudeste",
      sigla_uf %in% c("PR", "RS", "SC") ~ "Sul",
      TRUE ~ "Outra"
    )
  ) %>%
  group_by(ano, sigla_uf) %>%
  mutate(
    populacao_total_estado = sum(populacao_estimada, na.rm = TRUE),
    percentagem = round((populacao_estimada / populacao_total_estado) * 100, 4)
  ) %>%
  ungroup() %>% 
  # Selecionar e ordenar as novas variáveis
  select(ano, regiao, sigla_uf, idade, faixa_etaria, sexo, raca_cor, nivel_instrucao, populacao_estimada, populacao_total_estado, percentagem) %>%
  arrange(ano, regiao, sigla_uf, idade, sexo, raca_cor, nivel_instrucao)

# ==============================================================================
# 5. EXPORTAÇÃO PARA EXCEL E VISUALIZAÇÃO
# ==============================================================================
caminho_ficheiro <- "~/pnadc_dados_sociodemograficos_com_idade_2018_2026.xlsx"
write_xlsx(df_completo, path = caminho_ficheiro)

cat("\n========================================================\n")
cat("Processo concluído com sucesso!\n")
cat("O ficheiro Excel foi guardado no seguinte caminho:\n")
cat(caminho_ficheiro, "\n")
cat("========================================================\n")

View(df_completo)

