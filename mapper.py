import os
import pandas as pd

def wczytaj_pliki_csv(sciezki_folderow):
    # Ta funkcja wczytuje wszystkie pliki CSV z określonych folderów i łączy je w jeden DataFrame
    wszystkie_dane = pd.DataFrame()
    for folder in sciezki_folderow:
        for plik in os.listdir(folder):
            if plik.endswith('.csv'):
                sciezka_pliku = os.path.join(folder, plik)
                dane = pd.read_csv(sciezka_pliku)
                wszystkie_dane = pd.concat([wszystkie_dane, dane], ignore_index=True)
    return wszystkie_dane

def przetworz_dane(dane, ps_carmake, ps_carmodel, ps_caryear):
    # Mapowanie makeid do modeli na podstawie unikalnych par Make-Model
    make_model_pair = dane[['Make', 'Model']].drop_duplicates()
    make_model_pair['makeid'] = make_model_pair['Make'].map(ps_carmake.set_index('makename')['makeid'])
    ps_carmodel = ps_carmodel.merge(make_model_pair, left_on='modelname', right_on='Model', how='left')
    ps_carmodel.drop(['Make', 'Model'], axis=1, inplace=True)

    # Tworzenie ps_caryear - musimy tutaj przypisać yearid dla każdej kombinacji makeid, modelid i year
    car_years = dane[['Make', 'Model', 'Year']].drop_duplicates()
    car_years['makeid'] = car_years['Make'].map(ps_carmake.set_index('makename')['makeid'])
    car_years['modelid'] = car_years['Model'].map(ps_carmodel.set_index('modelname')['modelid'])
    car_years.dropna(subset=['makeid', 'modelid'], inplace=True)  # Usuwanie rekordów bez makeid/modelid

    # Przypisujemy unikalne yearid dla każdego rekordu
    car_years['yearid'] = range(1, len(car_years) + 1)
    ps_caryear = car_years[['yearid', 'makeid', 'modelid', 'Year']].rename(columns={'Year': 'year'})

    # Tworzenie ps_car_filter_data z mapowaniem do identyfikatorów
    ps_car_filter_data = dane.copy()
    ps_car_filter_data['make'] = ps_car_filter_data['Make'].map(ps_carmake.set_index('makename')['makeid'])
    ps_car_filter_data['model'] = ps_car_filter_data['Model'].map(ps_carmodel.set_index('modelname')['modelid'])
    # Tworzymy tymczasowy klucz do mapowania yearid
    ps_car_filter_data['temp_key'] = ps_car_filter_data.apply(lambda x: f"{x['make']}_{x['model']}_{x['Year']}", axis=1)
    year_map = ps_caryear.set_index(['makeid', 'modelid', 'year'])['yearid'].to_dict()
    # Mapowanie yearid za pomocą tymczasowego klucza
    ps_car_filter_data['year'] = ps_car_filter_data['temp_key'].map(
        lambda x: year_map.get(tuple(map(int, x.split('_'))), np.nan))
    ps_car_filter_data.drop(['temp_key'], axis=1, inplace=True)

    # Usuwanie wierszy z brakującymi danymi
    ps_car_filter_data.dropna(subset=['make', 'model', 'year'], inplace=True)

    return ps_carmake, ps_carmodel, ps_caryear, ps_car_filter_data

def zapisz_dane(ps_carmake, ps_carmodel, ps_caryear, ps_car_filter_data, sciezka_zapisu):
    # Upewnij się, że ścieżka istnieje
    if not os.path.exists(sciezka_zapisu):
        os.makedirs(sciezka_zapisu)

    # Zapisywanie danych do plików CSV
    ps_carmake.to_csv(os.path.join(sciezka_zapisu, 'ps_carmake.csv'), index=False)
    ps_carmodel.to_csv(os.path.join(sciezka_zapisu, 'ps_carmodel.csv'), index=False)
    ps_caryear.to_csv(os.path.join(sciezka_zapisu, 'ps_caryear.csv'), index=False)
    ps_car_filter_data.to_csv(os.path.join(sciezka_zapisu, 'ps_car_filter_data.csv'), index=False)

#Definiowanie ścieżek folderów i miejsca zapisu
sciezki_folderow = ['/mk2']
sciezka_zapisu = '/mk1'

# Wczytywanie danych
dane_zrodlowe = wczytaj_pliki_csv(sciezki_folderow)

# Przetwarzanie danych
ps_carmake, ps_carmodel, ps_caryear, ps_car_filter_data = przetworz_dane(dane_zrodlowe)

# Zapisywanie danych do plików
zapisz_dane(ps_carmake, ps_carmodel, ps_caryear, ps_car_filter_data, sciezka_zapisu)
