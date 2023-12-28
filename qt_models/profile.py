# -*- coding: utf-8 -*-
from collections import OrderedDict
from random import randint

import numpy as np

from .base import ModifiedBaseModel, ValueField, ColorField, CheckboxField

COLOR_LIST = [
    (34, 34, 34),
    (243, 195, 0),
    (135, 86, 146),
    (243, 132, 0),
    (161, 202, 241),
    (190, 0, 50),
    (194, 178, 128),
    (132, 132, 130),
    (0, 136, 86),
    (230, 143, 172),
    (0, 103, 165),
    (249, 147, 121),
    (96, 78, 151),
    (246, 166, 0),
    (179, 68, 108),
    (220, 211, 0),
    (136, 45, 23),
    (141, 182, 0),
    (101, 69, 34),
    (226, 88, 34),
    (43, 61, 38)
]

EMPTY_TIMESERIES = np.array([], dtype=float)


class ValueWithChangeSignal(object):

    def __init__(self, signal_name, signal_setting_name, init_value=None):
        self.signal_name = signal_name
        self.signal_setting_name = signal_setting_name
        self.value = init_value

    def __get__(self, instance, type):
        return self.value

    def __set__(self, instance, value):
        self.value = value
        getattr(instance, self.signal_name).emit(
            self.signal_setting_name, value)


def select_default_color(item_field):
    """
    return color for lines. First Colors are used as defined in COLOR_LIST
    item_field (ItemField): ...
    return (tuple): tuple with the 3 RGB color bands (values between 0-256)
    """

    model = item_field.item.model
    colors = OrderedDict([(str(color), color) for color in COLOR_LIST])

    for item in model.rows:
        if str(item.color.value) in colors:
            del colors[str(item.color.value)]

    if len(colors) >= 1:
        return colors.values()[0]

    # predefined colors are all used, return random color
    return (randint(0, 256), randint(0, 256), randint(0, 256), 180)


class ProfileModel(ModifiedBaseModel):
    """Model implementation for possible legger profiles"""

    class Fields:
        """Fields and functions of ModelItem"""

        is_standaard = CheckboxField(show=False,
                                     default_value=False,
                                     column_width=40)

        active = CheckboxField(show=True,
                               default_value=False,
                               column_width=40,
                               column_name='',
                               color_from='color')
        color = ColorField(show=False,
                           column_width=20,
                           column_name='',
                           default_value=(230, 143, 172, 160))
        name = ValueField(show=False,
                          column_width=130,
                          column_name='name')
        depth = ValueField(show=True,
                           column_width=45,
                           round=2,
                           column_name='dpt',
                           column_tooltip='profieldiepte [m]')
        width = ValueField(show=True,
                           column_width=45,
                           round=2,
                           column_name='br',
                           column_tooltip='profielbreedte [m]')
        begroeiingsvariant = ValueField(show=True,
                                        column_width=70,
                                        column_name='begr',
                                        column_tooltip='begroeiingsvariant naam',
                                        color_from='begroeiingsvariant_color')
        begroeiingsvariant_color = ColorField(show=False,
                                              default_value=(0, 0, 0, 0))

        score = ValueField(show=True,
                           column_width=45,
                           round=2,
                           column_name='sc',
                           column_tooltip='score')
        score_color = ColorField(show=False,
                                 default_value=(0, 0, 0, 0))

        over_depth = ValueField(show=True,
                                column_width=45,
                                round=2,
                                column_name='od',
                                column_tooltip='overdiepte [m]')
        over_depth_color = ColorField(show=False,
                                      default_value=(0, 0, 0, 0))

        over_width = ValueField(show=True,
                                column_width=45,
                                round=2,
                                column_name='ob',
                                color_from='over_width_color',
                                column_tooltip='overbreedte [m]')
        over_width_color = ColorField(show=False,
                                      default_value=(0, 0, 0, 0))

        verhang = ValueField(show=True,
                             column_width=45,
                             round=1,
                             column_name='verh',
                             column_tooltip='verhang [cm/km]',
                             color_from='verhang_color')
        verhang_color = ColorField(show=False,
                                   default_value=(0, 0, 0, 0))

        verhang_inlaat = ValueField(show=True,
                                    column_width=45,
                                    round=1,
                                    column_name='inl.vh.',
                                    column_tooltip='verhang inlaat [cm/km]',
                                    color_from='verhang_inlaat_color')
        verhang_inlaat_color = ColorField(show=False,
                                          default_value=(0, 0, 0, 0))

        hover = ValueField(show=False,
                           default_value=False)

        prof_series = ValueField(show=False)
        points = ValueField(show=False)
        _plot = None
