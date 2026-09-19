voebem — Análise de Atrasos de Voos ANAC

Pipeline de dados e dashboard analítico construído a partir da base VRA (Voo Regular Ativo) da ANAC, cobrindo o período de agosto/2025 a julho/2026. O projeto segue a arquitetura Medallion (Bronze → Silver → Gold), implementada em Databricks, PySpark e Delta Lake, com o resultado final visualizado em um AI/BI Dashboard (Lakeview).

Desenvolvido durante a Imersão Engenharia de Dados com IA 2026, da Alura.

📊 Dashboard

🔗 Acesse o dashboard publicado

Mostrar Imagem

Perguntas de negócio respondidas
#	Pergunta	Painel
P1	Quais aeroportos concentram os maiores atrasos de partida no Brasil?	Piores aeroportos por % de voos atrasados
P2	Como o atraso evolui ao longo do dia (efeito cascata)?	Atraso médio de partida ao longo do dia
P3	Quais companhias têm melhor pontualidade x cancelamento?	Pontualidade por companhia aérea
P5	Quanto atraso as companhias recuperam em voo?	Minutos de atraso recuperados em voo, por companhia
Principais achados
Guarulhos (GRU) lidera os atrasos, com 27,1% dos voos atrasados na partida — quase 8 pontos percentuais à frente do segundo colocado (Congonhas, 19,4%), reflexo do volume e da complexidade operacional do maior aeroporto do país.
Efeito cascata ao longo do dia: o atraso médio começa alto por volta da meia-noite (resíduo do dia anterior), cai para o menor ponto na madrugada (~4h-6h, quando a malha está mais limpa) e volta a subir progressivamente até a noite, conforme atrasos se acumulam ao longo da operação.
GOL e Azul lideram em pontualidade entre as companhias com volume relevante (>86% e >85% de voos pontuais, respectivamente).
TAP recupera, em média, quase o dobro de atraso em voo (9,8 min) comparada às companhias domésticas (2,6 a 4,5 min) — explicado pelo fato de operar voos majoritariamente internacionais e de longo curso, que costumam ter mais margem programada na malha de horários.
Stack técnica
Ingestão e transformação: PySpark, Delta Lake, arquitetura Medallion
Orquestração: Databricks Workflows
Visualização: Databricks AI/BI Dashboard (Lakeview)
Fonte de dados: VRA — Voo Regular Ativo, ANAC
