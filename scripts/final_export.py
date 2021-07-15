import pandas as pd


if __name__ == '__main__':
    df = pd.read_excel('/home/cytoragis/gis-preps/scripts/rel.xlsx')
    df.to_csv('UK_labeled_uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2.csv', index=False)

    print(df.info)
