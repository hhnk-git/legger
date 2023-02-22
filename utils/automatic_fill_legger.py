import csv
import os.path
from copy import deepcopy

from legger.utils.spatialite import load_spatialite


class AutomaticFillLegger(object):

    def __init__(self, spatialite_path, legger_table=None):
        self.spatialite_path = spatialite_path
        self.legger_table = (os.path.join(
            os.path.dirname(__file__),
            'standaard_profielen.csv'
        ) if legger_table is None else legger_table)
        self._cursor = None
        self._begroeiingsvariant_mapping = None

    @property
    def _db_cursor(self):
        if not self._cursor:
            con_legger = load_spatialite(self.spatialite_path)
            self._cursor = con_legger.cursor()
        return self._cursor

    def get_table(self):
        settings = csv.DictReader(open(self.legger_table, 'r'))

        tmp = {}

        for setting in settings:
            if setting['grondsoort'] not in tmp:
                tmp[setting['grondsoort']] = {}

            debiet = setting['debiet']

            if debiet not in tmp[setting['grondsoort']]:
                tmp[setting['grondsoort']][debiet] = {
                    'debiet': float(debiet),
                    'varianten': []
                }
            tmp[setting['grondsoort']][debiet]['varianten'].append(
                dict(setting)
            )

        out = tmp
        # for key, value in tmp.items():
        #     out[key] = list(value.values())

        return out

    def get_hydrovakken(self):
        self._db_cursor.execute("""
            SELECT 
                ho.id, 
                ho.code, 
                ho.debiet,
                k.diepte,
                k.breedte,
                k.lengte,
                k.grondsoort
            FROM 
                hydroobject ho 
            INNER JOIN kenmerken k ON k.hydro_id = ho.id
        """)
        hydroobjects = [
            dict(
                id=r[0],
                code=r[1],
                debiet=r[2],
                diepte=r[3],
                breedte=r[4],
                lengte=r[5],
                grondsoort=r[6]
            )
            for nr, r in enumerate(self._db_cursor.fetchall())]

        return hydroobjects

    @property
    def begroeiingsvariant_mapping(self):
        (3, 'vol', 1, 34, 65, 0.9),
        (2, 'half', 0, 34, 30, 0.5),
        (1, 'kwart', 0, 34, 30, 0.25)
        if self._begroeiingsvariant_mapping is None:

            self._db_cursor.execute("""
                SELECT naam, id
                FROM begroeiingsvariant
            """)
            self._begroeiingsvariant_mapping = dict([(a[0], a[1]) for a in self._db_cursor.fetchall()])
            if 'basis' in self._begroeiingsvariant_mapping:
                self._begroeiingsvariant_mapping['kwart'] = self._begroeiingsvariant_mapping['basis']
                self._begroeiingsvariant_mapping['half'] = self._begroeiingsvariant_mapping['half vol']
                self._begroeiingsvariant_mapping['vol'] = self._begroeiingsvariant_mapping['volledig begroeid']

        return self._begroeiingsvariant_mapping

    def add_default_variants(self, hydro_id, hydro_code, profile_options):

        self._db_cursor.executemany("""
                INSERT INTO varianten (
                id,
                begroeiingsvariant_id, 
                diepte, waterbreedte, bodembreedte, talud,
                hydraulische_diepte, hydraulische_waterbreedte, hydraulische_bodembreedte, hydraulische_talud,
                hydro_id, standaard_profiel_code) 
                VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO 
                UPDATE SET
                    begroeiingsvariant_id = excluded.begroeiingsvariant_id,
                    diepte = excluded.diepte,
                    waterbreedte = excluded.waterbreedte,
                    bodembreedte = excluded.bodembreedte,
                    talud = excluded.talud,
                    hydraulische_diepte = excluded.hydraulische_diepte,
                    hydraulische_waterbreedte = excluded.hydraulische_waterbreedte,
                    hydraulische_bodembreedte = excluded.hydraulische_bodembreedte
                            """, [(
            f'{hydro_code}_stand_{option.get("profiel_code")}',
            self.begroeiingsvariant_mapping.get(option.get('begroeiingsgraad')),
            option.get('ldiepte'),
            option.get('lwbreedte'),
            option.get('lbbreedte'),
            option.get('ltalud'),
            option.get('hdiepte'),
            option.get('hwbreedte'),
            option.get('hbbreedte'),
            option.get('htalud'),
            hydro_id,
            option.get('profiel_code'),

        ) for option in profile_options.get('varianten')])

        self._db_cursor.connection.commit()
        self._db_cursor.execute("""
            SELECT id, standaard_profiel_code
            FROM varianten
            WHERE standaard_profiel_code IS NOT NULL AND hydro_id = ?
        """, [hydro_id])

        available = dict([(a[1], a[0]) for a in self._db_cursor.fetchall()])

        for profiel_option in profile_options.get('varianten'):
            profiel_option['variant_id'] = available.get(profiel_option.get('profiel_code'))

        a = 1

    def save_to_database(self, selected_variants):

        items = [(selected['hydrovak'].get('id'), selected['option'].get('variant_id'))
                for code, selected in selected_variants.items() if selected is not None and selected['option'].get('variant_id') is not None]

        self._db_cursor.executemany("""
        INSERT INTO geselecteerd(hydro_id, variant_id, selected_on)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(hydro_id) DO 
          UPDATE SET
            variant_id=excluded.variant_id,
            selected_on=datetime('now')
          --WHERE hydro_id = excluded.hydro_id;     
        """, items)

        self._db_cursor.connection.commit()

        # hydrovak = begroeiingsvariant_id

    def get_network(self):
        pass

    def run(self):

        hydrovak_selected = {}

        hydrovakken = self.get_hydrovakken()

        prof_table = self.get_table()

        def get_profile_options(grondsoort, debiet):
            tbl = prof_table

            if grondsoort in prof_table:
                gsr = prof_table[grondsoort]
            else:
                gsr = prof_table['overig']

            debieten = list(gsr.keys())
            debieten.sort(key=lambda op: float(op), reverse=True)

            selected_debiet = None

            for cat_debiet in debieten:
                if abs(debiet) <= float(cat_debiet):
                    selected_debiet = cat_debiet

            if not selected_debiet:
                return None

            options = deepcopy(gsr[selected_debiet])

            return options

        for hydrovak in hydrovakken:

            selected = {
                'hydrovak': hydrovak,
                'selected': None
            }

            hydro_id = hydrovak.get('id')
            code = hydrovak.get('code')
            debiet = hydrovak.get('debiet')
            grondsoort = hydrovak.get('grondsoort')
            profiel_breedte = hydrovak.get('breedte')
            profiel_diepte = hydrovak.get('diepte')

            if code == 'OAF-C-4217':
                a = 1

            profile_options = get_profile_options(grondsoort, debiet)
            if profile_options is None:
                continue

            self.add_default_variants(hydro_id, code, profile_options)

            hydrovak_selected[code] = []

            found = False
            varianten = profile_options.get('varianten')
            varianten.reverse()
            if profiel_breedte is not None and profiel_diepte is not None:
                for option in varianten:
                    if (float(option.get("lwbreedte")) <= profiel_breedte and
                            float(option.get("ldiepte")) <= profiel_diepte):
                        hydrovak_selected[code].append({
                            'option': option,
                            'hydrovak': hydrovak
                        })

        # netwerk analyses om meerdere opties te toetsen op diepte.
        # voor nu de eerste optie
        for key, items in hydrovak_selected.items():

            if key == 'OAF-C-4217':
                a = 1
            if len(items):
                selected = items[0]
            else:
                selected = None

            hydrovak_selected[key] = selected

        # wegschrijven
        self.save_to_database(hydrovak_selected)

        # extra kolommen

        # a. get Varianten

        # b. get or update GeselecteerdeProfielen

    def get_profile_options(self, debiet, grondsoort):
        prof_table = self.get_table()

        for debiet_settings in prof_table:
            if debiet >= debiet_settings.get('debiet'):
                return debiet_settings[grondsoort]

        return None


def automatic_fill_legger(polder_sqlite_path):

    af = AutomaticFillLegger(polder_sqlite_path, None)
    table = af.run()


if __name__ == '__main__':
    # path = r"D:\tmp\legger\test_aanwijzen\Westzaan_v2_verhang_gecombineerd.sqlite"
    # path = r"D:\tmp\legger\test_aanwijzen\legger_assendelft_rev32_leeg.sqlite"
    path = r'd:\tmp\legger\legger_marken_met_3di_3.sqlite'

    automatic_fill_legger(path)
