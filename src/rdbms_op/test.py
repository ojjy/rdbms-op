

if __name__ == "__main__":

    dbt = [('DW', 'W_CC_MT_FACT_001 '), ('DW', 'W_CC_MT_FACT_003'), ('DW', 'W_CC_MCI_FACT_001'), ('DM', 'M_CC_MT_CD_003'), ('DW', 'W_CC_META_CD_001B')]

    for dbn, tbn in dbt:
        print(dbn, tbn)