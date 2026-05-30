"""Column definitions for all Receita Federal CNPJ dump file types.

Dump files have no header rows. Column order follows the official
layout document (Dados Abertos CNPJ – Receita Federal do Brasil).
"""

# ── Empresas ───────────────────────────────────────────────────────────────
EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte",
    "ente_federativo",
]

# ── Estabelecimentos ───────────────────────────────────────────────────────
ESTAB_COLUMNS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

# ── Lookup tables (all share the same two-column layout) ───────────────────
CNAE_COLUMNS      = ["codigo", "descricao"]
NATUREZA_COLUMNS  = ["codigo", "descricao"]
MUNICIPIO_COLUMNS = ["codigo", "descricao"]
PAIS_COLUMNS      = ["codigo", "descricao"]
MOTIVO_COLUMNS    = ["codigo", "descricao"]
QUALIFICACAO_COLUMNS = ["codigo", "descricao"]

# ── Final POI output schema (after join + enrichment) ─────────────────────
OUTPUT_COLUMNS = [
    "cnpj",
    "cnpj_basico",
    "razao_social",
    "nome_fantasia",
    "natureza_juridica",
    "natureza_juridica_descricao",
    "porte",
    "capital_social",
    "cnae_fiscal_principal",
    "cnae_fiscal_principal_descricao",
    "cnae_fiscal_secundaria",
    "identificador_matriz_filial",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "data_inicio_atividade",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "municipio",
    "municipio_descricao",
    "uf",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "correio_eletronico",
]

PORTE_MAP = {
    "00": "Não informado",
    "01": "Micro Empresa",
    "03": "Empresa de Pequeno Porte",
    "05": "MEI",
    "99": "Demais",
}
