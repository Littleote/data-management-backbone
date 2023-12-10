# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os

class Path():
    """
    Allows to change working directory with contexts

    Parameters
    ----------
    directory : str, optional
        Directory to set after initialization. The default is None.

    Returns
    -------
    None.

    """
    def __init__(self, directory=None):
        self.base = os.getcwd()
        self.directories = []
        self.move(directory)

    def __enter__(self):
        self._acces_last()

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def move(self, path):
        """
        Move working directory to another path

        Parameters
        ----------
        path : str
            Next path.

        Returns
        -------
        None.

        """
        if path is not None:
            self.directories.append(path)
        self._acces_last()

    def undo(self):
        """
        Return working directory to previous path

        Returns
        -------
        None.

        """
        if len(self.directories) > 0:
            self.directories.pop()
        self._acces_last()


    def reset(self):
        """
        Return working directory to original path

        Returns
        -------
        None.

        """
        self.directories = []
        os.chdir(self.base)

    def _acces_last(self):
        if len(self.directories) > 0:
            os.chdir(self.directories[-1])
        else:
            os.chdir(self.base)


def get_zones():
    """
    Get an instance of the list of zones

    Returns
    -------
    list
        Zones of the program.

    """
    return [
        "landing",
        "formatted",
        "trusted",
        "exploitation",
        "sandbox",
        "feature_generation",
        "models",
        "dataset_info"
    ]


def select(name, options):
    """
    Asks user to select an option among a set

    Parameters
    ----------
    name : str
        Name of the options presented to the user.
    options : list[T]
        List of options to present the user.

    Returns
    -------
    T or None
        Selected option or None if failed.

    """
    if len(options) > 1:
        # Choose if there are multiple
        print(f"Available {name}s:")
        for i, option in enumerate(options):
            print(f"   {i + 1}.- {option}")
        print()
        option = input(f"Select a {name}'s index: ")
        try:
            option = int(option)
            assert 0 < option <= len(options)
        except (AssertionError, ValueError):
            print("Invalid index")
            return None
        return options[option - 1]

    if len(options) == 1:
        # Confirm if there's only one
        print(f"Only {name} available is {options[0]}, procced?")
        option = input("[Y]/N\n")
        if len(option) <= 0 or option.lower()[0] != 'n':
            return options[0]
    # Cancel if not
    return None
    