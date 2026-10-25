# Sem REQUIRES: o componente main depende de todos os componentes,
# o que evita diferenças de nomes entre versões do ESP-IDF.
idf_component_register(SRCS "main.c"
                       INCLUDE_DIRS ".")
