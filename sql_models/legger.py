import datetime
import logging

from geoalchemy2.types import Geometry
from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import ClauseElement

logger = logging.getLogger('legger.sql_models.legger')

Base = declarative_base()


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True


class BegroeiingsVariant(Base):
    __tablename__ = 'begroeiingsvariant'

    id = Column(Integer(), primary_key=True, autoincrement=True, index=True)
    naam = Column(String(20))
    friction_manning = Column(Float())
    friction_begroeiing = Column(Float())
    begroeiingsdeel = Column(Float())

    is_default = Column(Boolean, default=False)

    profielvariant = relationship('Varianten',
                                  # primaryjoin="Varianten.begroeiingsvariant_id == BegroeiingsVariant.id",
                                  uselist=True,
                                  lazy='dynamic',
                                  # foreign_keys='ws_in_peilgebied',
                                  back_populates="begroeiingsvariant")

    def __str__(self):
        return u'begroeiingsvariant {0}'.format(
            self.naam)


class Waterdeel(Base):
    __tablename__ = 'waterdeel'
    extend_existing = True

    id = Column(Integer, primary_key=True, index=True)
    shape_length = Column(Float)
    shape_area = Column(Float)
    geometry = Column(
        "GEOMETRY",
        Geometry(geometry_type='MULTIPOLYGON', srid=28992, spatial_index=True, management=True)
    )

    def __str__(self):
        return u'Waterdeel {0}'.format(
            self.id)


class Categorie(Base):
    __tablename__ = 'categorie'
    extend_existing = True

    categorie = Column(Integer(), primary_key=True, index=True)
    naam = Column(String(20))
    variant_diepte_min = Column(Float())
    variant_diepte_max = Column(Float())
    default_talud = Column(Float())

    def __str__(self):
        return u'categorie {0} - {1}'.format(
            self.categorie, self.naam)


class HydroObject(Base):
    __tablename__ = 'hydroobject'
    extend_existing = True

    id = Column(Integer, primary_key=True, index=True)
    geometry = Column(
        "GEOMETRY",
        Geometry(geometry_type='MULTILINESTRING', srid=28992, spatial_index=True, management=True)
    )
    code = Column(String(50), index=True)
    categorieoppwaterlichaam = Column(Integer)
    streefpeil = Column(Float)
    zomerpeil = Column(Float)
    debiet_3di = Column(Float)
    debiet_fme = Column(Float)
    richting_fme = Column(Integer)
    debiet_inlaat = Column(Float)
    debiet_aangepast = Column(Float)
    debiet_opgelegd = Column(Float)
    debiet = Column(Float)
    channel_id = Column(Integer)  # link to 3di id
    flowline_id = Column(Integer)  # link to 3di id
    score = Column(Float)
    begroeiingsvariant_id = Column(Integer,
                                   ForeignKey(BegroeiingsVariant.__tablename__ + ".id"),
                                   index=True)
    opmerkingen = Column(String())

    duiker_count = Column(Integer)  # aantal duikers op basis van match duikersifonhevel

    eindpunt_potentieel = Column(Boolean)
    eindpunt_geselecteerd = Column(Boolean)
    geforceerd_omgedraaid = Column(Boolean)
    routing_gewicht = Column(Float)

    kijkp_breedte = Column(Float)
    kijkp_diepte = Column(Float)
    kijkp_talud = Column(Float)
    kijkp_reden = Column(String(30))
    # shape_length = Column(Float)

    # profielen = relationship("Profielen",
    #                          uselist=True,
    #                          back_populates="hydro")

    kenmerken = relationship("Kenmerken",
                             back_populates="hydro")

    geselecteerd = relationship("GeselecteerdeProfielen",
                                uselist=False,
                                back_populates="hydro")

    figuren = relationship("ProfielFiguren",
                           primaryjoin="HydroObject.id == ProfielFiguren.hydro_id",
                           lazy='dynamic',
                           # foreign_keys=[id],
                           back_populates="hydro")

    varianten = relationship("Varianten",
                             primaryjoin="HydroObject.id == Varianten.hydro_id",
                             # uselist=True,
                             lazy='dynamic',
                             back_populates="hydro")

    begroeiingsvariant = relationship(BegroeiingsVariant,
                                      # foreign_keys='begroeiingsvariant_id',
                                      # primaryjoin="Varianten.begroeiingsvariant_id == BegroeiingsVariant.id",
                                      uselist=False)

    def __str__(self):
        return u'Hydro object {0}'.format(
            self.code)


class Profielen(Base):
    __tablename__ = 'profielen'

    id = Column(Integer, primary_key=True, index=True)
    proident = Column(String(24))
    bron_profiel = Column(String(50))
    pro_id = Column(Integer, index=True)
    hydro_id = Column(Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      index=True)

    # shape_lengte = Column(Float)
    # hydro = relationship(HydroObject,
    #                      back_populates="profielen")

    # profielpunten = relationship(
    #     "Profielpunten",
    #     uselist=True,
    #     back_populates="profiel")

    def __str__(self):
        return u'profiel {0} - {1}'.format(
            self.id, self.proident)


HydroObject.profielen = relationship(Profielen, uselist=True, back_populates="hydro")
Profielen.hydro = relationship(HydroObject, back_populates="profielen")


class Profielpunten(Base):
    __tablename__ = 'profielpunten'

    objectid = Column(Integer, primary_key=True, autoincrement=True, index=True)
    pbp_id = Column(Integer)
    prw_id = Column(Integer)
    pbpident = Column(String(24))
    osmomsch = Column(String(60))
    iws_volgnr = Column(Integer)
    iws_hoogte = Column(Float)
    afstand = Column(Float)
    pro_pro_id = Column(Integer,
                        ForeignKey(Profielen.__tablename__ + '.pro_id'),
                        index=True)
    geometry = Column(
        "GEOMETRY",
        Geometry(geometry_type='POINT', srid=28992, spatial_index=True, management=True)
    )

    profiel = relationship(
        "Profielen",
        uselist=False,
        back_populates="profielpunten")

    def __str__(self):
        return u'profielpunt {0}'.format(
            self.pbpident)


Profielen.profielpunten = relationship(
    Profielpunten,
    uselist=True,
    back_populates="profiel")


# Profielpunten.profiel = relationship(
#     Profielen,
#     uselist=False,
#     back_populates="profielpunten")


class Kenmerken(Base):
    __tablename__ = 'kenmerken'

    id = Column(Integer, primary_key=True, index=True)
    diepte = Column(Float)
    bron_diepte = Column(String(50))
    bodemhoogte = Column(Float)
    breedte = Column(Float)
    bron_breedte = Column(String(50))
    lengte = Column(Float)
    taludvoorkeur = Column(Float)
    steilste_talud = Column(Float)
    grondsoort = Column(String(50))
    soort_vak = Column(Integer)  # 3 = kunstwerkvak, 4 = pseudovak

    bron_grondsoort = Column(String(50))
    hydro_id = Column(Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      index=True)
    na_lengte = Column(Float)
    voor_lengte = Column(Float)
    new_category = Column(Integer)

    hydro = relationship(HydroObject,
                         # foreign_keys='ws_in_peilgebied',
                         back_populates="kenmerken")

    def __str__(self):
        return u'kenmerken {0}'.format(
            self.id)


class Varianten(Base):
    __tablename__ = 'varianten'

    id = Column(String(), primary_key=True, index=True)
    begroeiingsvariant_id = Column(Integer,
                                   ForeignKey(BegroeiingsVariant.__tablename__ + ".id"),
                                   index=True)
    diepte = Column(Float,
                    index=True)
    waterbreedte = Column(Float)
    bodembreedte = Column(Float)
    talud = Column(Float)
    hydraulische_diepte = Column(Float)
    hydraulische_waterbreedte = Column(Float)
    hydraulische_bodembreedte = Column(Float)
    hydraulische_talud = Column(Float)
    # maatgevend_debiet = Column(Float)
    verhang = Column(Float)
    verhang_inlaat = Column(Float)
    afvoer_leidend = Column(Integer)
    opmerkingen = Column(String())
    hydro_id = Column(Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      index=True)

    standaard_profiel_code = Column(String())

    begroeiingsvariant = relationship(BegroeiingsVariant,
                                      # foreign_keys='begroeiingsvariant_id',
                                      primaryjoin="Varianten.begroeiingsvariant_id == BegroeiingsVariant.id",
                                      uselist=False,
                                      back_populates="profielvariant")

    hydro = relationship(HydroObject,
                         # foreign_keys='hydro_id',
                         uselist=False,
                         back_populates="varianten")

    figuren = relationship('ProfielFiguren',
                           primaryjoin="Varianten.id == ProfielFiguren.profid",
                           # foreign_keys=[hydro_id],
                           back_populates="prof")

    # geselecteerd = relationship("GeselecteerdeProfielen",
    #                        back_populates="variant")

    def __str__(self):
        return u'profiel_variant {0}'.format(
            self.id)


class GeselecteerdeProfielen(Base):
    __tablename__ = 'geselecteerd'

    hydro_id = Column(Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      primary_key=True,
                      index=True)
    variant_id = Column(String(),
                        ForeignKey(Varianten.__tablename__ + ".id"),
                        index=True)
    selected_on = Column(DateTime, default=datetime.datetime.utcnow)
    tot_verhang = Column(Float)

    hydro_verhang = Column(Float)

    hydro = relationship(HydroObject,
                         back_populates="geselecteerd")

    variant = relationship(Varianten)
    UniqueConstraint('hydro_id', 'variant_id', name='hydro_variant_unique')
    # back_populates="geselecteerd")


class ProfielFiguren(Base):
    __tablename__ = 'profielfiguren'

    # object_id = Column(Integer, primary_key=True)
    hydro_id = Column('id_hydro', Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      index=True)
    profid = Column(String(16),
                    ForeignKey(Varianten.__tablename__ + ".id"),
                    primary_key=True,
                    index=True)
    type_prof = Column(String(1))
    coord = Column(String())
    peil = Column(Float)
    t_talud = Column(Float)
    t_waterdiepte = Column(Float)
    t_bodembreedte = Column(Float)
    t_fit = Column(Float)
    t_afst = Column(Float)
    g_rest = Column(Float)
    t_overdiepte = Column(Float)
    t_overbreedte_l = Column(Float)
    t_overbreedte_r = Column(Float)

    hydro = relationship(HydroObject,
                         primaryjoin="HydroObject.id == ProfielFiguren.hydro_id",
                         # foreign_keys=[hydro_id],
                         back_populates="figuren")

    prof = relationship(Varianten,
                        primaryjoin="Varianten.id == ProfielFiguren.profid",
                        # foreign_keys=[hydro_id],
                        back_populates="figuren")

    def __str__(self):
        return u'profiel_figuren {0} - {1}'.format(
            self.hydro_id, self.profid)


class DuikerSifonHevel(Base):
    __tablename__ = 'duikersifonhevel'
    extend_existing = True

    id = Column(Integer, primary_key=True, index=True)
    geometry = Column(
        "GEOMETRY",
        Geometry(geometry_type='MULTILINESTRING', srid=28992, spatial_index=True, management=True)
    )
    code = Column(String(50), index=True)
    categorie = Column(Integer)
    lengte = Column(Float)
    hoogteopening = Column(Float)
    breedteopening = Column(Float)
    hoogtebinnenonderkantbene = Column(Float)
    hoogtebinnenonderkantbov = Column(Float)
    vormkoker = Column(Float)
    # shape_lengte = Column(Float)

    debiet = Column(Float)  # extra
    channel_id = Column(Integer)  # extra
    flowline_id = Column(Integer)  # extra

    hydro_id = Column(Integer,
                      ForeignKey(HydroObject.__tablename__ + ".id"),
                      index=True)

    def __str__(self):
        return u'DuikerSifonHevel {0}'.format(
            self.code)
