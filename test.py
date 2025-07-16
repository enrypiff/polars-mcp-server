from server import describe_csv, execute_queries

print("describe_csv function imported successfully.")
print(describe_csv("/home/ubuntu/NEA/nea-enrico/excel-mcp-server/Sheet1_A5.csv"))
print(describe_csv("/home/ubuntu/NEA/nea-enrico/data/SL -> POP OPM_A2_W14875.csv"))

print("execute_queries function imported successfully.")
queries = [
    "SELECT * FROM self",
    """SELECT CLLI, DISTRETTO_TELEFONO, 
       SEDE_LATITUDINE_UNICARA, SEDE_LONGITUDINE_UNICARA,
       ARMADIO_LATITUDINE_UNICARA, ARMADIO_LONGITUDINE_UNICARA,
       DISTRIBUTORE_LATITUDINE_UNICARA, DISTRIBUTORE_LONGITUDINE_UNICARA,
       LATITUDINE_NETMAP, LONGITUDINE_NETMAP
FROM self 
WHERE CLLI = 'PALEITAY' AND DISTRETTO_TELEFONO = '0000005173802'""",
    "SELECT \"Latitudine SL\", \"Longitudine SL\" FROM self WHERE CLLI = 'PALEITAY' AND DISTRETTO = '0000005173802'",
]
print(
    execute_queries(
        "/home/ubuntu/NEA/nea-enrico/excel-mcp-server/Sheet1_A5.csv", queries
    )
)
