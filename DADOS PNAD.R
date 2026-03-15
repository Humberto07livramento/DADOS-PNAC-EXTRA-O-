# ==============================================================================
# SCRIPT COMPLETO: PNADC - População por Ano, Região, Estado e Raça/Cor
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

# 2. Autenticação no Google Cloud
# ATENÇÃO: Fique atento à aba "Console" (Consola) do RStudio. 
# Se surgir uma pergunta sobre "cache OAuth", clique na consola, digite 1 e prima Enter.
bq_auth(email = "livramkurios@gmail.com")

# 3. Definir o ID do seu projeto no Google Cloud
set_billing_id("teak-hearth-439318-i1")

# 4. Preparar a consulta (Query) - Extraindo dados brutos resumidos do servidor
# Utilizamos o trimestre = 4 e a variável V1028 (peso amostral) para estimar a população.
query <- "
SELECT
    ano,
    sigla_uf,
    V2010 as raca_cor_codigo,
    SUM(V1028) as populacao_estimada
FROM `basedosdados.br_ibge_pnadc.microdados`
WHERE ano BETWEEN 2018 AND 2025
  AND trimestre = 4
GROUP BY ano, sigla_uf, V2010
ORDER BY ano, sigla_uf, V2010
"

# 5. Descarregar os dados já resumidos
cat("A descarregar os dados com raça/cor do servidor. Isto pode demorar alguns segundos...\n")
df_raca_cor <- read_sql(query, billing_project_id = get_billing_id())

# 6. Tratamento dos dados: Traduzir, criar Regiões e Calcular Percentagens
df_completo <- df_raca_cor %>%
  mutate(
    # Traduzir os códigos do IBGE para o texto correspondente
    raca_cor = case_when(
      as.character(raca_cor_codigo) == "1" ~ "Branca",
      as.character(raca_cor_codigo) == "2" ~ "Preta",
      as.character(raca_cor_codigo) == "3" ~ "Amarela",
      as.character(raca_cor_codigo) == "4" ~ "Parda",
      as.character(raca_cor_codigo) == "5" ~ "Indígena",
      as.character(raca_cor_codigo) == "9" ~ "Ignorado",
      TRUE ~ "Não Informado"
    ),
    # Criar a coluna de Região baseada na sigla da Unidade da Federação
    regiao = case_when(
      sigla_uf %in% c("AM", "RR", "AP", "PA", "TO", "RO", "AC") ~ "Norte",
      sigla_uf %in% c("MA", "PI", "CE", "RN", "PE", "PB", "SE", "AL", "BA") ~ "Nordeste",
      sigla_uf %in% c("MT", "MS", "GO", "DF") ~ "Centro-Oeste",
      sigla_uf %in% c("SP", "RJ", "ES", "MG") ~ "Sudeste",
      sigla_uf %in% c("PR", "RS", "SC") ~ "Sul",
      TRUE ~ "Outra"
    )
  ) %>%
  # Agrupar por ano e estado para calcular os totais e as proporções
  group_by(ano, sigla_uf) %>%
  mutate(
    populacao_total_estado = sum(populacao_estimada, na.rm = TRUE),
    percentagem = round((populacao_estimada / populacao_total_estado) * 100, 2)
  ) %>%
  ungroup() %>% # Desagrupar para evitar erros futuros em novas análises
  # Reorganizar as colunas para o Excel ficar estruturado e limpo
  select(ano, regiao, sigla_uf, raca_cor, populacao_estimada, populacao_total_estado, percentagem) %>%
  arrange(ano, regiao, sigla_uf, raca_cor)

# 7. Guardar o ficheiro Excel nos seus Documentos
caminho_ficheiro <- "~/pnadc_raca_cor_completo_2018_2025.xlsx"
write_xlsx(df_completo, path = caminho_ficheiro)

cat("Processo concluído! O ficheiro Excel foi guardado em:", caminho_ficheiro, "\n")

# 8. Visualizar a tabela final no RStudio
View(df_completo)