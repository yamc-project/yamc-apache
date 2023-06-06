# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc_apache.providers.mod_status import ModStatusProvider

from importlib.metadata import version, PackageNotFoundError


def __getattr__(name):
    """
    Return the version number of the package as a lazy attribute.
    """
    if name == "__version__":
        try:
            return version("yamc-pushover")
        except PackageNotFoundError as e:
            return "unknown"
    raise AttributeError(f"module {__name__} has no attribute {name}")
