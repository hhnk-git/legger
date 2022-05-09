from qgis.core import NULL


def try_round(value_or_function, decimals=0, default_value=None):

    if callable(value_or_function):
        try:
            value = value_or_function()
        except (ValueError, TypeError):
            return default_value
    else:
        value = value_or_function
    if value is None:
        return default_value
    try:
        return round(value, decimals)
    except TypeError:
        # if string
        try:
            return round(float(value), decimals)
        except (ValueError, TypeError):
            return default_value

    except ValueError:
        return default_value


def python_value(value, default_value=None, func=None):
    """
    help function for translating QVariant Null values into None
    value: QVariant value or python value
    default_value: value in case provided value is None
    func (function): function for transforming value
    :return: python value
    """

    # check on QVariantNull... type
    if hasattr(value, 'isNull') and value.isNull():
        return default_value
    else:
        if default_value is not None and value is None:
            return default_value
        else:
            if func is not None:
                return func(value)
            else:
                return value


def make_type(value, typ, default_value=None, round_digits=None, factor=1):
    """transform value (also Qt NULL values) to specified type or default value if None.
    Can also round value or multiply value

    value (any type): input value to transform to other type
    typ (python type): python type object like int, str or float
    default_value (any): default value returned when value is None or NULL
    round_digits (int): Number of digits to round value on.
    factor (float or int): Multiplication factor

    return (any): transformed value
    """
    if value is None or value == NULL:
        return default_value
    try:
        output = typ(value)
        if typ in (float, int,):
            if round is not None:
                return round(factor * output, round_digits)
            else:
                return factor * output
        else:
            return output

    except (TypeError, ValueError):
        return default_value


def transform_none(value):
    """ Transform Qt NULL value to python None

    value (any): input value
    return (any): value or None when value is NULL
    """
    if value == NULL:
        return None
    else:
        return value
