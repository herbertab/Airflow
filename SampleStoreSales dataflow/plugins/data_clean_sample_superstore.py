import pandas as pd

def CleanSampleSuperstore(df: pd.DataFrame) -> pd.DataFrame:
    # variaveis
#    file_path = "D:\Portfolio\Projeto 1\data\Sample - Superstore.csv"
#    file_path_out = "D:\Portfolio\Projeto 1\data\Sample - Superstore_clean.csv"
#    df = pd.read_csv(file_path,encoding='latin1')

    print(df.head())
    print(df.info())

    # conversões de tipos
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])

    print(df.info())

    # remover colunas desnecessárias
    df = df.drop(['Row ID', 'Postal Code'], axis=1)
    print(df.head())

    # colunas novas
    df['City / State'] = df['City'] + " / " + df['State']

    print(df.head())

    # verificando consistência dos valores tipo texto
    def count_unique(dataframe: pd.DataFrame):
        print("Função count_unique()")
        for column in df.columns: 
            print(f"Valores únicos na coluna '{column}': ", df[column].nunique())

    count_unique(df)

    # função para verificar inconsistència entre campos de nome e id
    def show_inconsistence(dataframe: pd.DataFrame, columnA: str, columnB: str):
        incosistence = dataframe.groupby(columnA)[columnB].unique()
        incosistence = incosistence[incosistence.apply(lambda x: len(x) > 1)]
        print('\n',len(incosistence), f" '{columnA}' com mais de um '{columnB}':")
        print("Inconsistências:")
        print(incosistence, '\n')

    # verificando se há inconsistência entre 
    # Product Name -> Product ID / Product ID -> Product Name
    # Product ID -> Category --- caso exista, indica mesmo produto cadastrado em mais de uma categoria
    # Product ID -> Sub-Category --- caso exista, indica mesmo produto cadastrado em mais de uma sub-categoria
    # Sub-Category -> Category --- caso exista, indica sub-categoria pertencente a mais de uma categoria
    show_inconsistence(df, 'Product Name', 'Product ID')
    show_inconsistence(df, 'Product ID', 'Product Name')
    show_inconsistence(df, 'Product ID', 'Category')
    show_inconsistence(df, 'Product ID', 'Sub-Category')
    show_inconsistence(df, 'Sub-Category', 'Category')

    # verificando se há incosistência entre Customer ID -> Customer Name 
    # --- No caso do nome de cliente não vamos verificar se há nomes iguais com ids diferentes, pois isso pode acontecer no caso de homônimos
    # Customer ID -> Segment --- caso exista, indica um mesmo cliente tendo mais de um segmento
    show_inconsistence(df, 'Customer ID', 'Customer Name')
    show_inconsistence(df, 'Customer ID', 'Segment')

    # função para resolver a inconsistência 
    def solve_inconsistence(dataframe: pd.DataFrame, column_name_A: str, column_name_B: str) -> pd.DataFrame:
        dup = dataframe[dataframe.duplicated(subset=column_name_A, keep=False)]
        columnA_to_columnB = {}
        for idx, row in dup.iterrows():
            itemA = row[column_name_A]
            itemB = row[column_name_B]
            if itemA not in columnA_to_columnB:
                columnA_to_columnB[itemA] = itemB
            else:
                dataframe.loc[idx, column_name_B] = columnA_to_columnB[itemA]
        return dataframe

    df = solve_inconsistence(df, 'Product Name', 'Product ID')
    show_inconsistence(df, 'Product Name', 'Product ID')

    # No caso da inconsistência produtos diferentes com o mesmo Product ID, foi adotada a estratégia de igualar os nomes dos produtos a fim de manter a consistência
    # dos dados. Entretanto, dependendo da característica do projeto, pode-se ter que criar novos IDs para os produtos em vez de renomeá-los.
    df = solve_inconsistence(df, 'Product ID', 'Product Name')
    show_inconsistence(df, 'Product ID', 'Product Name')
    show_inconsistence(df, 'Product ID', 'Category')
    show_inconsistence(df, 'Product ID', 'Sub-Category')
    show_inconsistence(df, 'Sub-Category', 'Category')

    return df