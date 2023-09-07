#   profile_matcha.py
#   onderdeel van leggertool
#   plaatst theoretische profielen zo goed mogelijk binnen gemeten profielen

#   variant a) database structuur 20180129

import logging
import matplotlib
import os

matplotlib.use('AGG')

import sqlite3
import matplotlib.pyplot as plt
import shapely
import shapely.geometry
from shapely.errors import TopologicalError
# from descartes import PolygonPatch
from matplotlib.pyplot import savefig

try:
    from legger.sql_models.legger_database import load_spatialite
except ImportError:
    from sql_models.legger_database import load_spatialite

logger = logging.getLogger(__name__)


def mk_pro_x_hy_kompas(cur, straal=0.5, aantalstappen=90, srid=28992):
    """ Voor alle gemeten profielen wordt het snijpunt met het bijbehorende hydroobject gemaakt en het azimut
    (kompasrichting) van het hydroobject ter plekke.
    Invoer: cur =  een cursor naar de database met gemetenprofielen, hydroobjecten en theoretische profielen
            straal = de straal van een semi-cirkel met als middelpunt het snijpunt van gemetenprofiel en hydroobject
            aantalstappen = het aantal lijnstukken van de semi-cirkel
            srid = het nummer van het geografische referentiestelsel; 28992 =  Rijksdriehoekmeting
    Uitvoer: gevulde tabel pro_x_hy_kompas in de database

    Op de loodlijn op het lijnstuk van het hydroobject ter plekke van het snijpunt, worden later het gemeten
    profiel en de theoretische profielen geprojecteerd.
    Het snijpunt met de kompasrichting wordt opgeslagen in tabel pro_x_hy_kompas in de database.

    Het snijpunt wordt bepaald met de spatialite functie Intersection(profiellijn,hydroobject)
    De kommpasrichting wordt bepaald met de spatialite functie Azimuth(punt1,punt2)
    punt1 en punt2 worden bepaald door Intersection(profiellijn, cirkeltje_om_snijpunt)
    cirkeltje_om_snijpunt wordt gemaakt door Buffer(snijpunt, straal, aantalstappen)
    straal is een variabele evenals aantal stappen"""

    cur.execute('drop table if exists pro_x_hy_kompas')
    cur.execute('create table pro_x_hy_kompas (pro_id bigint primary key, ovk_ovk_id bigint, kompas float,'
                'CONSTRAINT fk_pro  FOREIGN KEY (pro_id) REFERENCES pro(pro_id), '
                'CONSTRAINT fk_hy  FOREIGN KEY (ovk_ovk_id) REFERENCES hydroobject(id)) ')
    cur.execute('select DiscardGeometryColumn("pro_x_hy_kompas","geometry")')
    cur.execute('select AddGeometryColumn("pro_x_hy_kompas", "geometry", %d, "POINT")' % srid)
    cur.execute('insert into pro_x_hy_kompas (pro_id, ovk_ovk_id, geometry, kompas)'
                ' select pro_id, ovk_ovk_id, Intersection(pro.GEOMETRY, hydroobject.GEOMETRY),'
                'Azimuth('
                'PointN(Intersection(pro.GEOMETRY,Buffer(Intersection(pro.GEOMETRY, hydroobject.GEOMETRY),%f,%d)),1), '
                'PointN(Intersection(pro.GEOMETRY,Buffer(Intersection(pro.GEOMETRY, hydroobject.GEOMETRY),%f,%d)),2)) '
                'from pro inner join hydroobject on '
                '(pro.ovk_ovk_id=hydroobject.id)' % (straal, aantalstappen, straal, aantalstappen))
    return


def peilperprofiel(cur, peilcriterium="min", debug=0):
    """ Haal per gemeten profiel het heersende peil op.
    Invoer: cur =  een cursor naar de database met gemetenprofielen, hydroobjecten en theoretische profielen
            peilcriterium = vlag met waarden min of max om resp.de minimale of maximale waterhoogte te kiezen
    Uitvoer: een dictionary met als sleutel het id van het profiel en als waarden het id van het hydroobject en peil

    In versie 0 wordt domweg op grond van de administratieve joins de waterhoogte uit de tabel streefpeilen gebruikt
    Dit kan later zo nodig verfijnt worden met een spatial join
    In het testgebied Geestmerambacht levert de gebruikte query 43 null waarden op voor waterhoogte
       steekproeven geven aan dat dit vooral komt doordat profielen geselecteerd zijn met een buffer
       rond het gebied

     IS NIET MEER NODIG
        """
    if peilcriterium != 'min' and peilcriterium != 'max':
        peilcriterium = 'min'
        # logger.debug("PEILCRITERIUM AANGEPAST NAAR %s", peilcriterium)

    q = '''select pro.pro_id, pro.ovk_ovk_id, %s(streefpeil.waterhoogte) from 
            pro left outer join hydroobject on (pro.ovk_ovk_id = hydroobject.id) 
            left outer join peilgebiedpraktijk on (hydroobject.ws_in_peilgebied = peilgebiedpraktijk.code) 
            left outer join streefpeil on (peilgebiedpraktijk.id=streefpeil.peilgebiedpraktijkid)
        group by pro.pro_id, pro.ovk_ovk_id
        ''' % peilcriterium
    prof = {}
    for r in cur.execute(q):
        prof[r[0]] = (r[1], r[2])
    q = '''insert into hyob_voorkeurpeil select hydroobject.id, %s(streefpeil.waterhoogte) from 
            hydroobject left outer join peilgebiedpraktijk on (hydroobject.ws_in_peilgebied = peilgebiedpraktijk.code) 
            left outer join streefpeil on (peilgebiedpraktijk.id=streefpeil.peilgebiedpraktijkid)
        group by hydroobject.id''' % peilcriterium
    cur.execute(q)
    # logger.debug("aantal gemeten profielen in een hydro_object met een peil: %d ", len(prof))

    return prof


def haal_meetprofielen1(cur, profielsoort="Z1", filter_profiel_id=None):
    """ Haal de gemeten profieelpunten op uit de database voor de profielsoort vastebodem (Z1)
     Invoer:    cur = een cursor naar de database met gemetenprofielen, hydroobjecten en theoretische profielen
                profielsoort = de code voor de harde bodem
                peilcriterium = vlag met waarden min of max om resp.de minimale of maximale waterhoogte te kiezen
     Uitvoer:   een dictionary met als sleutel de profielids van gemeten profielen
                met per profielid:
                het hydroid (hydro object id)
                het peil
                de punten van het gemeten profiel + extra begin- en eindpunt 100m hoger """
    prof = {}

    q = 'select profielen.pro_id, profielen.proident, hydroobject.id, hydroobject.streefpeil from profielen inner join hydroobject ' \
        'on (profielen.hydro_id=hydroobject.id) WHERE hydroobject.streefpeil IS NOT NULL '
    if filter_profiel_id is not None:
        q += ' WHERE profielen.id = %d' % filter_profiel_id

    q_punten = '''select pl.pro_id, pp.iws_volgnr, X(pp.GEOMETRY), Y(pp.GEOMETRY), pp.iws_hoogte, pl.hydro_id
           from profielen as pl inner join profielpunten as pp on (pl.pro_id = pp.pro_pro_id)
           where pl.pro_id = %d and pp.osmomsch = "%s" 
           order by pp.iws_volgnr'''
    cur.execute(q)

    for proid, proident, hydro_id, streefpeil in list(cur.fetchall()):
        prof[proid] = {}  # Er kunnen meerdere gemeten profielen per hydrovak zijn.
        prof[proid]['hydroid'] = hydro_id
        prof[proid]['peil'] = streefpeil
        prof[proid]['proident'] = proident
        prof[proid]["orig"] = []

        cur.execute(q_punten % (proid, profielsoort))
        for r in cur.fetchall():
            prof[proid]["orig"].append([r[2], r[3], r[4], r[5]])

        if len(prof[proid]["orig"]) > 0:
            # eerste en laatste punt 1000 meter hoger
            prof[proid]["orig"][0][2] = max(prof[proid]["orig"][0][2], streefpeil) + 1000.0
            prof[proid]["orig"][-1][2] = max(prof[proid]["orig"][-1][2], streefpeil) + 1000.0

            # Verrijk profielen met de projectie op een rechte lijn
            # prof verrijkt met de key "proj" met daarin een list van lists van afstand-geprojecteerd,
            # x-geprojecteerd, y-geprojecteerd en diepte
            lijn = shapely.geometry.LineString([(prof[proid]['orig'][0][0], prof[proid]['orig'][0][1]),
                                                (prof[proid]['orig'][-1][0], prof[proid]['orig'][-1][1])])
            prof[proid]['proj'] = []
            for p in prof[proid]['orig']:
                afstand = lijn.project(shapely.geometry.Point((p[0], p[1])))
                pr = lijn.interpolate(afstand)
                prof[proid]['proj'].append([afstand, pr.x, pr.y, p[2], p[3]])

    return prof


def interpoleerafstand(l, r, p):
    """interpoleer de afstand op grond van de hoogtes
    Invoer: l = linker list met afstand, x, y en z
            r = rechter list met [a, x, y, z]
            p = z waarde tussen l[3] en r[3] in

    Uitvoer:    tuple van afstand a en  hoogte z
    """
    factor = (p - l[3]) / (r[3] - l[3])
    return l[0] + factor * (r[0] - l[0]), p


def verrijkgemprof(cur, prof):
    """Verrijk de tabel profielpunten met de afstanden zoals die in projecteerprofielen berekend zijn"""
    q = 'update profielpunten set afstand = %f where OGC_FID= %d'
    for proid in prof:
        for p in prof[proid]['proj']:
            if p[4] != 0:
                cur.execute(q % (p[0], p[4]))
    return


def mkmogelijktheoprofiel(talud, waterdiepte, bodembreedte, peil):
    """Maak een shapely polygoon van het theoretisch profiel """
    return shapely.geometry.Polygon([(0, peil), (talud * waterdiepte, peil - waterdiepte),
                                     (talud * waterdiepte + bodembreedte, peil - waterdiepte),
                                     (talud * waterdiepte + bodembreedte + talud * waterdiepte, peil)])


def mkrechthoekondertheoprofiel(rhlb, rhrb):
    """Maak een shapely polygoon van een rechthoek onder het theoretisch profiel"""
    return shapely.geometry.Polygon([rhlb, (rhlb[0], rhlb[1] - 1000.0), (rhrb[0], rhrb[1] - 1000.0), rhrb])


def grootste(xy):
    index = 0
    m = 0
    for i in xy:
        if len(xy[i]) > 2:
            if (xy[i][-1][0] - xy[i][0][0]) > m:
                index = i
                m = xy[i][-1][0] - xy[i][0][0]
    return index


def mkgemprof(axyzlist, peil):
    """ Maak een shapely polygoon van het gemeten profiel (afstanden en diepte), doorsnijden met het peil
    helaas is split pas vanaf shapely versie 1.6 aanwezig (qgis 2.18 heeft shapely 1.2) daarom

    Invoer: list van lists met afstand, x, y en z
            peil
    Uitvoer: shapely polygon
    """
    xy = {}
    tel = 0
    xy[tel] = []
    links = axyzlist[0]
    positie = 'b'
    for c in axyzlist[1:]:
        if positie == 'b':
            if c[3] < peil:
                xy[tel].append(interpoleerafstand(links, c, peil))
                xy[tel].append((c[0], c[3]))
                positie = "o"
            elif c[3] == peil:
                xy[tel].append((c[0], c[3]))
                positie = "o"
        else:
            if c[3] > peil:
                xy[tel].append(interpoleerafstand(links, c, peil))
                positie = "b"
                tel += 1
                xy[tel] = []
            elif c[3] == peil:
                xy[tel].append((c[0], c[3]))
                positie = "b"
                tel += 1
                xy[tel] = []
            else:
                xy[tel].append((c[0], c[3]))
        links = c
    if tel > 0:
        tel = grootste(xy)
    return shapely.geometry.Polygon(xy[tel])


def prof_in_prof(profgem, proftheo, aantstap=100, delta=0.001, obdiepte=0.001, debug=0):
    """Het theoretisch profiel wordt in stapjes verschoven over het gemeten profiel (te beginnen links van het
    gemeten profiel zonder overlap, tot en met rechts van het gemeten profiel zonder overlap). Indien het
    theoretisch profiel nergens past binnen het gemeten profiel is de plek met het maximale oppervlak van de
    intersectie van het theoretisch met het gemeten profiel de optimale plek voor het theoretisch profiel,
    tenzij er een traject is met een gelijk maximaal oppervlak; in dat geval wordt het midden van dit traject de
    optimale plek voor het theoretisch profiel.
    Wanneer het oppervlak van deze intersectie gelijk is aan het oppervlak van het theoretisch profiel, dan
    past het theoretisch profiel volkomen binnen het gemeten profiel. In dat geval wordt het midden van het
    traject waarvoor het intersectie oppervlak gelijk is aan het oppervlak van het theoretisch profiel, de
    optimale plek voor het theoretisch profiel
    Invoer: profgem:    het shapely polygoon van het gemeten profiel
            proftheo:   het shapely polygoon van het theoretisch profiel
            aantstap:   het aantal stappen dat gebruikt wordt voor de verschuiving van het theoretisch profiel
            delta:      het acceptabele verschil om vast te stellen of twee oppervlakken gelijk zijn
            obdiepte:   de diepte waarop de beschikbare overbreedte bepaald wordt
    Uitvoer: fit:       de fractie van het  oppervlak van het theoretisch profiel dat past binnen het gemeten
            optimaal:   de optimale verschuiving (afstand) van het theoretisch profiel gemeten vanaf
                        de start van het gemeten profiel
            fractie:    1 - de fractie van het gemeten profiel dat bedekt wordt door het theoretisch profiel
            overdiepte: de gemiddelde afstand onder rechte stuk van het theoretisch profiel tot het gemeten profiel
            linksover:  de afstand tussen het gemeten profiel en het theoretisch profiel op obdiepte links
            rechtsover: de afstand tussen het gemeten profiel en het theoretisch profiel op obdiepte rechts"""

    waterbreedte_gemeten = profgem.bounds[2] - profgem.bounds[0]
    waterbreedte_theo = proftheo.bounds[2] - proftheo.bounds[0]
    waterdiepte_theo = proftheo.bounds[3] - proftheo.bounds[1]
    rhlb = proftheo.exterior.coords[1]  # de linkerbovenhoek resp rechterbovenhoek van een rechthoek onder
    rhrb = proftheo.exterior.coords[2]  # het rechte stuk van het theoretisch profiel (tbv overdiepte)
    oblijn = shapely.geometry.LineString([(0, profgem.bounds[3] - obdiepte),
                                          (waterbreedte_gemeten + 2 * waterbreedte_theo, profgem.bounds[3] - obdiepte)])
    if not oblijn.is_valid:
        logger.error('oblijn not valid. geom is: %s', oblijn.wkt)

    if not profgem.is_valid:
        logger.error('profgem not valid. geom is: %s', profgem.wkt)

    try:
        clijn = profgem.intersection(oblijn)  # oblijn tbv overbreedte, clijn controle lijnstuk tbv overbreedte
    except TopologicalError as e:
        logger.error(e)
        return -99, -99, -99, -99, -99, -99

    gemprof = shapely.affinity.translate(profgem, -profgem.bounds[0], 0.0, 0.0)  # op nul meter laten beginnen
    profzoek = shapely.affinity.translate(proftheo, -waterbreedte_theo, 0.0, 0.0)  # verschuif naar nul overlap
    stap = (waterbreedte_gemeten + waterbreedte_theo + waterbreedte_theo) / aantstap
    zoekopp = profzoek.area  # het oppervlak van het theoretisch profiel
    maxopp = -9.9  # het maximale oppervlak van de intersectie
    traject = ''  # in traject komt per stap een 0 of 1 (1 indien maxopp == zoekopp)
    optimaal = -waterbreedte_theo  # dit is de start van het theoretisch profiel, overlap is nul!
    fit = 0  # de verhouding maxopp / zoekopp (een goodness of fit)
    # logger.debug("wb_gem: %.2f, wb_theo: %.2f; stap: %.2f, zoekopp: %.3f",
    #          waterbreedte_gemeten, waterbreedte_theo, stap, zoekopp)
    for i in range(aantstap):
        profzoek = shapely.affinity.translate(profzoek, stap, 0.0, 0.0)
        inter = gemprof.intersection(profzoek)
        if abs(zoekopp - inter.area) < delta:  # opp intersectie == zoekopp dus past profzoek volledig in gemprof
            optimaal = profzoek.bounds[0]
            maxopp = inter.area
            traject += '1'
            # logger.debug('Volledig: optimaal: %f; maxopp: %f', optimaal, maxopp)
        else:
            if inter.area == maxopp:
                traject += '2'
            elif inter.area > maxopp:  # opp intersectie groter dan voorgaand oppervlak
                optimaal = profzoek.bounds[0]
                maxopp = inter.area
                traject = traject.replace('2', '0')  # evt oud traject met kleiner oppervlak weghalen
                traject += '2'
                # logger.debug('Niet vol: optimaal: %f; maxopp: %f', optimaal, maxopp)
            else:
                traject += '0'
    fit = maxopp / zoekopp  # de best fit
    fractie = (gemprof.area - maxopp) / gemprof.area  # de overblijvende fractie oppervlak van het gemetenprofiel
    # logger.debug('Na loop: Fit: %.3f; optimaal: %f', fit, optimaal)
    # logger.debug(traject)

    if traject.find('1') > 0:  # er zijn 1 tekens in traject
        traject = traject.replace('2', '0')  # evt oud traject met kleiner oppervlak weghalen
        zoek = max(traject.split('0'))  # in zoek komt de eerste langste reeks 1 tekens in traject
        astap = traject.find(zoek) + len(zoek) / 2.0 + 1
        optimaal = astap * stap
        optimaal -= waterbreedte_theo  # corrigeer voor de verschuiving van het theoretisch profiel
    elif traject.find('2') > 0:  # er zijn trajecten met een kleiner oppervlak
        zoek = max(traject.split('0'))  # in zoek komt de eerste langste reeks 1 tekens in traject
        astap = traject.find(zoek) + len(zoek) / 2.0 + 1
        optimaal = astap * stap
        optimaal -= waterbreedte_theo  # corrigeer voor de verschuiving van het theoretisch profiel
    optimaal += profgem.bounds[0]  # corrigeer voor de verschuiving van het gemeten profiel
    # logger.debug("optimaal: %f", optimaal)
    roth = shapely.affinity.translate(mkrechthoekondertheoprofiel(rhlb, rhrb), optimaal, 0.0, 0.0)
    poloth = profgem.intersection(roth)
    overdiepte = poloth.area / (rhrb[0] - rhlb[0])  # oppervlak gedeeld door breedte geeft diepte

    linksover = 0.0
    rechtsover = 0.0
    restbak = profgem.difference(shapely.affinity.translate(proftheo, optimaal, 0.0, 0.0))
    hlijn = restbak.intersection(oblijn)  # restbak is restant gemeten profiel min theor. profiel
    if obdiepte < (profgem.bounds[3] - profgem.bounds[1]):  # de obdiepte is hoger dan de bodem van het gemetenprofiel
        try:
            xlinks = clijn.coords[0][0]
            xrechts = clijn.coords[1][0]

            try:
                if len(hlijn) == 2:  # is hlijn een MultiLineString van twee LineStrings
                    if (hlijn[0].coords[0][0] == xlinks) and (hlijn[1].coords[1][0] == xrechts):
                        linksover = hlijn[0].length
                        rechtsover = hlijn[1].length
                elif len(hlijn) == 3:  # is hlijn een MultiLineString van drie LineStrings
                    # logger.debug("hlijn 3 stuks, xl, xr, hl", xlinks, xrechts, hlijn)
                    if (hlijn[0].coords[1][0] == xlinks) and (hlijn[2].coords[1][0] == xrechts):
                        linksover = hlijn[1].length
                        rechtsover = hlijn[2].length
            except:
                # logger.info("hlijn is geen MultiLineString")
                pass
        except:
            # logger.info("clijn is geen LineString")
            pass
    return fit, optimaal, fractie, overdiepte, linksover, rechtsover


def altertable(cur, tabelnaam, veldnaam, veldtype):
    """"Primitieve alter table voor float, integer, double met controle of het veld al bestaat"""
    gevonden = 0
    for r in cur.execute('pragma table_info("%s")' % tabelnaam):
        if r[1] == veldnaam:
            gevonden = 1
    if not gevonden:
        cur.execute('alter table "%s" add column "%s" "%s"' % (tabelnaam, veldnaam, veldtype))
    return


def maaktabellen(cur):
    """"Maak de tabellen met verrijkte platgeslagen profielen tbv presentatie
        presentatie: klik op de kaart nabij een hydroobject en een profiel (selecteer dichtstbijzijnde,
                    een hydroobject kan meer gemeten profielen hebben!!
        tabel hyob_voorkeurpeil geeft op grond van het id van het hydroobject het gekozen peil (kan natuurlijk
            ook als view, maar dit zal sneller zijn, geen  idee of het van belang is)
        tabel profielfiguren is een platte tabel met alle info voor figuren met gemeten en theoretische profielen
            met infor over fit, overdiepte enz enz.

        Aanpassing van bestaande tabellen:
         hydroobject met voorkeurpeil
         profielpunten met afstand
     """
    # altertable(cur, "hydroobject", "voorkeurpeil", "float")
    # altertable(cur, "profielpunten", "afstand", "float")

    # cur.execute('drop table if exists hyob_voorkeurpeil')
    # cur.execute('create table hyob_voorkeurpeil (id integer primary key, voorkeurpeil float)')
    cur.execute('drop table if exists profielfiguren')
    cur.execute('drop index if exists profielfiguren0')
    cur.execute('drop index if exists profielfiguren1')
    cur.execute('create table profielfiguren(id_hydro integer, profid varchar(16), type_prof char(1), coord text, '
                'peil float, t_talud float, t_waterdiepte float, t_bodembreedte float, t_fit float, t_afst float, '
                'g_rest float, t_overdiepte float, t_overbreedte_l float, t_overbreedte_r float)')
    cur.execute('vacuum')
    return


def controlefig(gemprof, theoprof, afstand, fit, fractie, overdiepte, overlinks, overrechts, hydroid, profid,
                talud, diepte, breedte, peil):
    fnm = 'cf/%d_%d_%d_%.2f_%.2f' % (profid, hydroid, talud, diepte, breedte)
    fnm = fnm.replace('.', ',')
    txt1 = 'Hydroid: %d, profid: %d, peil: %.2f; Talud: %d; Waterdiepte: %.2f; Bodembreedte: %2f.' % \
           (hydroid, profid, peil, talud, diepte, breedte)
    txt2 = 'Fit: %.2f; Fractie %.2f, Overdiepte %.3f; Overbreedte links: %.1f; Overbreedte rechts: %.1f.' % \
           (fit, fractie, overdiepte, overlinks, overrechts)
    blue = '#6699cc'
    orange = '#cc9933'
    fig, ax = plt.subplots()
    fig.text(0.95, 0.15, txt1,
             fontsize=7, color='black',
             ha='right', va='bottom', alpha=0.9)
    fig.text(0.95, 0.10, txt2,
             fontsize=7, color='black',
             ha='right', va='bottom', alpha=0.9)
    p2 = PolygonPatch(gemprof, fc=blue, ec=blue, alpha=0.5)
    ax.add_patch(p2)
    p1 = PolygonPatch(shapely.affinity.translate(theoprof, afstand, 0.0, 0.0), fc=orange, ec=orange, alpha=0.5)
    ax.add_patch(p1)
    ax.axis('scaled')
    savefig(fnm, dpi=200)
    plt.close()
    return


def doe_profinprof(cur0, cur1, aantalstappen=200, precisie=0.0001, codevastebodem="Z1",
                   obdiepte=0.001, debug=0, profiel_id=None):
    """

    :param cur0: cursor naar de legger database
    :param cur1: cursor naar de legger database
    :param aantalstappen:
    :param precisie:
    :param codevastebodem:
    :param obdiepte:
    :param debug:
    :return:
    """

    gemetenprofielen = haal_meetprofielen1(cur0, codevastebodem, profiel_id)

    # alleen theoretische profielen die liggen in hydro-objecten waar ook gemeten profielen zijn ophalen
    q = """select id, id, talud, diepte, bodembreedte from varianten where hydro_id='%s'"""
    qm = """insert into profielfiguren (id_hydro, profid, type_prof, coord, peil) values (%d, "%s", "m", "%s", %f)"""
    qt = """insert into profielfiguren (id_hydro, profid, type_prof, coord, peil, t_talud, t_waterdiepte, t_bodembreedte, 
            t_fit, t_afst, g_rest, t_overdiepte, t_overbreedte_l, t_overbreedte_r) values 
            (%d, "%s", "t", "%s", %f, %f, %f, %f, %f, %f, %f, %f, %f, %f)"""
    for profielid, profiel in gemetenprofielen.items():
        try:
            # mkgemprof aanroepen met list van lists met afstand, x, y, z (geprojecteerd); en het peil;
            # levert een shapely polygoon
            gemprofshapely = mkgemprof(profiel['proj'], profiel['peil'])

            if gemprofshapely.is_empty:
                logger.warning('Profiel %s (%i) geeft een lege profiel geometry terug. skip profiel',
                               profiel['proident'], profielid)
                continue

            h = qm % (profiel['hydroid'], profiel['proident'],
                      gemprofshapely.wkt, profiel['peil'])
            # logger.debug(h)
            cur1.execute(h)
            cur0.execute(q % profiel['hydroid'])
            for theo_data in cur0.fetchall():
                # mkmogelijkprofiel aanroepen met talud, waterdiepte, bodembreedte en peil, levert een shapely polygon
                theoprofshapely = mkmogelijktheoprofiel(theo_data[2], theo_data[3], theo_data[4],
                                                        profiel['peil'])
                # prof_in_prof aanroepen met gemetenprofiel, theoretisch profiel aantal stappen en aanvaardbaar verschil
                #  (profielen bestaan uit shapely polygons)
                fit, afstand, fractie, overdiepte, overlinks, overrechts = \
                    prof_in_prof(gemprofshapely, theoprofshapely, aantalstappen, precisie, obdiepte, debug)

                cur1.execute(qt % (profiel['hydroid'], theo_data[1],
                                   shapely.affinity.translate(theoprofshapely, afstand, 0.0, 0.0).wkt,
                                   profiel['peil'], theo_data[2], theo_data[3], theo_data[4],
                                   fit, afstand, fractie, overdiepte, overlinks, overrechts))
        except Exception as e:
            logger.error('Fout in verwerken profiel %s (%i), hydrovak_id %s', profiel['proident'], profielid,
                         profiel['hydroid'])
            logger.exception(e)

    cur0.execute('CREATE INDEX IF NOT EXISTS profielfiguren0 on profielfiguren(id_hydro)')
    cur0.execute('CREATE INDEX IF NOT EXISTS profielfiguren1 on profielfiguren(profid)')
    # cur0.execute('vacuum')
    resultaat = "klaar"

    return resultaat


if __name__ == '__main__':
    import sys

    os.environ["PROJ_LIB"] = "/Applications/QGIS-LTR.app/Contents/Resources/proj"
    os.environ["GDAL_DATA"] = "/Applications/QGIS-LTR.app/Contents/Resources/gdal"
    sys.path.append('/Users/bastiaanroos/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins')

    conn = load_spatialite('//Users/bastiaanroos/Documents/testdata/leggertool/legger_hub_update_bastiaan2.sqlite')
    cur0 = conn.cursor()
    cur1 = conn.cursor()
    #
    resultaat = doe_profinprof(cur0, cur1, debug=True)
    print(resultaat)
    a = 1
